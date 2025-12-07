import express from "express";
import fetch from "node-fetch";
import sharp from "sharp";
import dotenv from "dotenv";
import FormData from "form-data";

dotenv.config();

const app = express();
app.use(express.json({ limit: "10mb" }));

const PORT = process.env.PORT || 3000;

const SHOPIFY_STORE_DOMAIN = process.env.SHOPIFY_STORE_DOMAIN;
const SHOPIFY_ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ----------------- Helper: Bild in Shopify Files hochladen -----------------

async function uploadToShopifyFiles(buffer, filename) {
  if (!SHOPIFY_STORE_DOMAIN || !SHOPIFY_ACCESS_TOKEN) {
    console.error("[JOB] Missing Shopify env vars");
    throw new Error("Shopify credentials missing");
  }

  console.log("[JOB] Creating staged upload for", filename);

  // 1) stagedUploadsCreate → S3-Upload-Ziel holen
  const stagedMutation = `
    mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
      stagedUploadsCreate(input: $input) {
        stagedTargets {
          url
          resourceUrl
          parameters {
            name
            value
          }
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const stagedVariables = {
    input: [
      {
        resource: "FILE",
        filename,
        mimeType: "image/png",
        httpMethod: "POST"
      }
    ]
  };

  const stagedResp = await fetch(
    `https://${SHOPIFY_STORE_DOMAIN}/admin/api/2024-07/graphql.json`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
      },
      body: JSON.stringify({
        query: stagedMutation,
        variables: stagedVariables
      })
    }
  );

  const stagedData = await stagedResp.json();
  console.log(
    "[JOB] stagedUploadsCreate response:",
    JSON.stringify(stagedData, null, 2)
  );

  const stagedErrors = stagedData?.data?.stagedUploadsCreate?.userErrors || [];
  if (stagedErrors.length > 0) {
    throw new Error(
      "stagedUploadsCreate userErrors: " + JSON.stringify(stagedErrors)
    );
  }

  const target =
    stagedData?.data?.stagedUploadsCreate?.stagedTargets?.[0] || null;
  if (!target) {
    throw new Error("No staged upload target returned");
  }

  // 2) Datei zu S3 hochladen (multipart/form-data)
  const form = new FormData();
  for (const param of target.parameters) {
    form.append(param.name, param.value);
  }
  // Das Feld muss "file" heißen
  form.append("file", buffer, {
    filename,
    contentType: "image/png"
  });

  console.log("[JOB] Uploading file buffer to staged URL:", target.url);
  const uploadResp = await fetch(target.url, {
    method: "POST",
    body: form
  });

  if (!uploadResp.ok) {
    const text = await uploadResp.text().catch(() => "");
    throw new Error(
      `Staged upload failed with status ${uploadResp.status}: ${text}`
    );
  }

  // 3) filesCreate → aus staged Upload eine File-Ressource machen
  const filesCreateMutation = `
    mutation filesCreate($files: [FileCreateInput!]!) {
      filesCreate(files: $files) {
        files {
          id
          url
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const filesVariables = {
    files: [
      {
        contentType: "IMAGE",
        originalSource: target.resourceUrl,
        altText: filename
      }
    ]
  };

  const filesResp = await fetch(
    `https://${SHOPIFY_STORE_DOMAIN}/admin/api/2024-07/graphql.json`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
      },
      body: JSON.stringify({
        query: filesCreateMutation,
        variables: filesVariables
      })
    }
  );

  const filesData = await filesResp.json();
  console.log(
    "[JOB] filesCreate response:",
    JSON.stringify(filesData, null, 2)
  );

  const filesErrors = filesData?.data?.filesCreate?.userErrors || [];
  if (filesErrors.length > 0) {
    throw new Error(
      "filesCreate userErrors: " + JSON.stringify(filesErrors)
    );
  }

  const file = filesData?.data?.filesCreate?.files?.[0] || null;
  if (!file || !file.url) {
    throw new Error("No file with URL returned from filesCreate");
  }

  console.log("[JOB] Uploaded file URL:", file.url);
  return file.url; // neue URL auf cdn.shopify.com
}

// --------------- Hintergrund-Job: Bild drehen + Metafeld updaten ----------

async function processRotateAndUpdateJob(payload) {
  console.log("=== [JOB] Start rotate-and-update job ===");
  console.log("Job payload:", JSON.stringify(payload, null, 2));

  const {
    image_url,
    product_id,
    metafield_namespace,
    metafield_key,
    rotation
  } = payload;

  if (!image_url || !product_id || !metafield_namespace || !metafield_key) {
    console.log(
      "[JOB] Missing required fields, aborting (image_url, product_id, metafield_namespace, metafield_key)."
    );
    return;
  }

  if (!SHOPIFY_STORE_DOMAIN || !SHOPIFY_ACCESS_TOKEN) {
    console.error("[JOB] Missing Shopify env vars");
    return;
  }

  try {
    console.log("[JOB] Waiting 3 minutes before first attempt...");
    await sleep(3 * 60 * 1000);

    const maxAttempts = 4;
    let lastError = null;
    let rotatedImageBuffer = null;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        console.log(`[JOB] Attempt ${attempt} to download image: ${image_url}`);
        const resp = await fetch(image_url);
        if (!resp.ok) {
          throw new Error(`Image fetch failed with status ${resp.status}`);
        }
        const buffer = Buffer.from(await resp.arrayBuffer());

        console.log("[JOB] Rotating image by", rotation || 90, "degrees");
        rotatedImageBuffer = await sharp(buffer)
          .rotate(rotation || 90)
          .png()
          .toBuffer();

        console.log("[JOB] Image rotated successfully");
        break;
      } catch (err) {
        console.error(`[JOB] Error in attempt ${attempt}:`, err);
        lastError = err;
        if (attempt < maxAttempts) {
          console.log("[JOB] Waiting 1 minute before next attempt...");
          await sleep(60 * 1000);
        }
      }
    }

    if (!rotatedImageBuffer) {
      console.error("[JOB] All attempts failed:", lastError);
      return;
    }

    // 2) Rotiertes Bild in Shopify Files hochladen
    const filename = `rotated-${product_id}-${Date.now()}.png`;
    const finalImageUrl = await uploadToShopifyFiles(
      rotatedImageBuffer,
      filename
    );

    // 3) Metafeld in Shopify updaten
    const ownerId = `gid://shopify/Product/${product_id}`;
    console.log("[JOB] Updating metafield for ownerId:", ownerId);

    const metafieldsSetMutation = `
      mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $metafields) {
          metafields {
            id
            namespace
            key
            value
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    const metafieldInput = [
      {
        ownerId,
        namespace: metafield_namespace,
        key: metafield_key,
        type: "url",
        value: finalImageUrl
      }
    ];

    const gqlResp = await fetch(
      `https://${SHOPIFY_STORE_DOMAIN}/admin/api/2024-07/graphql.json`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
        },
        body: JSON.stringify({
          query: metafieldsSetMutation,
          variables: { metafields: metafieldInput }
        })
      }
    );

    const gqlData = await gqlResp.json();
    console.log(
      "[JOB] Shopify metafieldsSet response:",
      JSON.stringify(gqlData, null, 2)
    );

    const userErrors =
      gqlData?.data?.metafieldsSet?.userErrors ||
      gqlData?.errors ||
      [];

    if (userErrors.length > 0) {
      console.error("[JOB] Shopify Metafield Errors:", userErrors);
      return;
    }

    console.log("[JOB] Metafield updated successfully to:", finalImageUrl);
  } catch (err) {
    console.error("[JOB] Unexpected error in processRotateAndUpdateJob:", err);
  } finally {
    console.log("=== [JOB] End rotate-and-update job ===");
  }
}

// -------------------------- Routen -----------------------------------------

app.get("/", (req, res) => {
  console.log("GET / called");
  res.json({ status: "ok", message: "rotate service running" });
});

app.post("/rotate-and-update", (req, res) => {
  console.log("---- Incoming /rotate-and-update ----");
  console.log("Body:", JSON.stringify(req.body, null, 2));

  // Job async starten, Cloudhooks sofort Antwort geben
  processRotateAndUpdateJob(req.body).catch((err) =>
    console.error("Background job error:", err)
  );

  res.status(200).json({ status: "accepted" });
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
