import express from "express";
import fetch from "node-fetch";
import sharp from "sharp";
import dotenv from "dotenv";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

dotenv.config();

const app = express();
app.use(express.json({ limit: "10mb" }));

const PORT = process.env.PORT || 3000;

// Shopify-Env
const SHOPIFY_STORE_DOMAIN = process.env.SHOPIFY_STORE_DOMAIN;
const SHOPIFY_ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;

// R2-Env
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME; // z.B. anymora-rotated-designs
const R2_ENDPOINT = process.env.R2_ENDPOINT; // https://<ACCOUNT_ID>.r2.cloudflarestorage.com (OHNE Bucket)
const R2_PUBLIC_BASE_URL = process.env.R2_PUBLIC_BASE_URL; // z.B. https://pub-xxx.r2.dev ODER mit Bucket

// S3-kompatibler Client für Cloudflare R2
const r2Client = new S3Client({
  region: "auto",
  endpoint: R2_ENDPOINT,
  forcePathStyle: true,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY
  }
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ----------------- Helper: Bild in R2 hochladen -----------------

async function uploadToR2(buffer, filename) {
  if (
    !R2_ACCESS_KEY_ID ||
    !R2_SECRET_ACCESS_KEY ||
    !R2_BUCKET_NAME ||
    !R2_ENDPOINT ||
    !R2_PUBLIC_BASE_URL
  ) {
    console.error("[JOB] Missing R2 env vars");
    throw new Error("R2 credentials / config missing");
  }

  const key = `rotated/${filename}`;

  console.log("[JOB] Uploading rotated image to R2:", {
    bucket: R2_BUCKET_NAME,
    key
  });

  try {
    const cmd = new PutObjectCommand({
      Bucket: R2_BUCKET_NAME,
      Key: key,
      Body: buffer,
      ContentType: "image/png"
    });

    const result = await r2Client.send(cmd);
    console.log("[JOB] R2 PutObject result:", result);

    // Public-URL korrekt bauen (Bucket ggf. anhängen)
    const base = R2_PUBLIC_BASE_URL.replace(/\/$/, "");
    const bucketSegment = `/${R2_BUCKET_NAME}`;
    const baseWithBucket = base.endsWith(bucketSegment)
      ? base
      : `${base}${bucketSegment}`;

    const finalUrl = `${baseWithBucket}/${key}`;
    console.log("[JOB] Uploaded to R2 URL:", finalUrl);
    return finalUrl;
  } catch (err) {
    console.error("[JOB] Error uploading to R2:", err);
    throw err;
  }
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

    // 1) Bild holen + drehen mit max. 4 Versuchen
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

    // 2) Rotiertes Bild in R2 hochladen
    const filename = `rotated-${product_id}-${Date.now()}.png`;
    const finalImageUrl = await uploadToR2(rotatedImageBuffer, filename);

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
            type
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    // WICHTIG: Typ setzen (Annahme: Teeinblue nutzt single_line_text_field)
    const metafieldInput = [
      {
        ownerId,
        namespace: metafield_namespace,
        key: metafield_key,
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
    console.log(
      "[JOB] Shopify metafieldsSet response:",
      JSON.stringify(gqlData, null, 2)
    );

    const userErrors =
      gqlData?.data?.metafieldsSet?.userErrors || gqlData?.errors || [];

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

  processRotateAndUpdateJob(req.body).catch((err) =>
    console.error("Background job error:", err)
  );

  res.status(200).json({ status: "accepted" });
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
