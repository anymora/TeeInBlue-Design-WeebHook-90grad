import express from "express";
import fetch from "node-fetch";
import sharp from "sharp";
import dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(express.json({ limit: "10mb" }));

const PORT = process.env.PORT || 3000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Hintergrund-Job: Bild holen, drehen, Metafeld updaten
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
      "[JOB] Missing required fields, aborting job (image_url, product_id, metafield_namespace, metafield_key)."
    );
    return;
  }

  const SHOPIFY_STORE_DOMAIN = process.env.SHOPIFY_STORE_DOMAIN;
  const SHOPIFY_ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;

  if (!SHOPIFY_STORE_DOMAIN || !SHOPIFY_ACCESS_TOKEN) {
    console.error("[JOB] Missing SHOPIFY_STORE_DOMAIN or SHOPIFY_ACCESS_TOKEN");
    return;
  }

  try {
    console.log("[JOB] Waiting 3 minutes before first attempt...");
    await sleep(3 * 60 * 1000);

    const maxAttempts = 4;
    let lastError = null;
    let rotatedImageBuffer = null;
    let finalImageUrl = null;

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
          .toBuffer();

        // TODO: Hier müsstest du das gedrehte Bild irgendwo hochladen (S3, Cloudinary, eigenes CDN)
        // und die neue URL in finalImageUrl speichern.
        // Solange du das nicht hast, verwenden wir die Original-URL weiter:
        finalImageUrl = image_url;

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

    if (!rotatedImageBuffer || !finalImageUrl) {
      console.error("[JOB] All attempts failed:", lastError);
      return;
    }

    // Metafeld in Shopify updaten
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
        // wenn dein Feld in Shopify als "url" definiert ist, hier auf "url" ändern:
        type: "single_line_text_field",
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
    console.log("[JOB] Shopify metafieldsSet response:", JSON.stringify(gqlData, null, 2));

    const userErrors =
      gqlData?.data?.metafieldsSet?.userErrors ||
      gqlData?.errors ||
      [];

    if (userErrors.length > 0) {
      console.error("[JOB] Shopify Metafield Errors:", userErrors);
      return;
    }

    console.log("[JOB] Metafield updated successfully.");
  } catch (err) {
    console.error("[JOB] Unexpected error in processRotateAndUpdateJob:", err);
  } finally {
    console.log("=== [JOB] End rotate-and-update job ===");
  }
}

// Healthcheck
app.get("/", (req, res) => {
  console.log("GET / called");
  res.json({ status: "ok", message: "rotate service running" });
});

// Webhook-Endpoint
app.post("/rotate-and-update", (req, res) => {
  console.log("---- Incoming /rotate-and-update ----");
  console.log("Body:", JSON.stringify(req.body, null, 2));

  // Job im Hintergrund starten (nicht awaiten)
  processRotateAndUpdateJob(req.body).catch((err) =>
    console.error("Background job error:", err)
  );

  // Sofort antworten, damit Cloudhooks kein Timeout bekommt
  res.status(200).json({ status: "accepted" });
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
