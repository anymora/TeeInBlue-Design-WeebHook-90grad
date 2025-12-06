require("dotenv").config();
const express = require("express");
const axios = require("axios");
const sharp = require("sharp");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");

const app = express();
app.use(express.json({ limit: "10mb" }));

// ==== ENV VARS ====
// In Railway als Variables setzen:
//
// SHOPIFY_STORE_DOMAIN   -> z.B. my-shop.myshopify.com
// SHOPIFY_ACCESS_TOKEN   -> Admin API Access Token (Custom App in Shopify)
//
// S3_REGION              -> z.B. eu-central-1
// S3_BUCKET              -> Name des S3-Buckets, z.B. my-rotated-designs
// AWS_ACCESS_KEY_ID      -> IAM User / Role
// AWS_SECRET_ACCESS_KEY  -> IAM User / Role
//
// PORT                   -> kommt von Railway, fallback 3000

const SHOPIFY_STORE_DOMAIN = process.env.SHOPIFY_STORE_DOMAIN;
const SHOPIFY_ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;

const S3_REGION = process.env.S3_REGION;
const S3_BUCKET = process.env.S3_BUCKET;

const PORT = process.env.PORT || 3000;

if (!SHOPIFY_STORE_DOMAIN || !SHOPIFY_ACCESS_TOKEN) {
  console.error(
    "Fehlende Env Vars: SHOPIFY_STORE_DOMAIN und/oder SHOPIFY_ACCESS_TOKEN"
  );
}

if (!S3_REGION || !S3_BUCKET) {
  console.error("Fehlende Env Vars: S3_REGION und/oder S3_BUCKET");
}

// S3 Client
const s3Client = new S3Client({
  region: S3_REGION,
});

// ==== HEALTHCHECK ====
app.get("/", (req, res) => {
  res.json({ status: "ok", message: "Shopify image rotate service running" });
});

/**
 * POST /rotate-and-update
 *
 * Erwartet JSON body aus Shopify Flow:
 * {
 *   "image_url": "https://....",
 *   "product_id": 1234567890,
 *   "metafield_namespace": "custom",
 *   "metafield_key": "_tib_design_link_1",
 *   "rotation": 90
 * }
 *
 * rotation optional, default: 90 (Grad im Uhrzeigersinn)
 */
app.post("/rotate-and-update", async (req, res) => {
  const {
    image_url,
    product_id,
    metafield_namespace,
    metafield_key,
    rotation,
  } = req.body;

  if (!image_url || !product_id || !metafield_namespace || !metafield_key) {
    return res.status(400).json({
      error:
        "image_url, product_id, metafield_namespace und metafield_key sind erforderlich",
    });
  }

  const rotationAngle = rotation || 90;

  try {
    // 1. Bild herunterladen
    const imageResponse = await axios.get(image_url, {
      responseType: "arraybuffer",
    });
    const originalBuffer = Buffer.from(imageResponse.data);

    // 2. Bild drehen (90° im Uhrzeigersinn, Ausgabe PNG)
    const rotatedBuffer = await sharp(originalBuffer)
      .rotate(rotationAngle)
      .toFormat("png")
      .toBuffer();

    // 3. Bild in S3 hochladen, URL zurückbekommen
    const rotatedImageUrl = await uploadToS3AndGetUrl(
      rotatedBuffer,
      image_url
    );

    // 4. Produkt-Metafeld aktualisieren
    const metafieldUpdateResponse = await setProductMetafieldString(
      product_id,
      metafield_namespace,
      metafield_key,
      rotatedImageUrl
    );

    return res.json({
      success: true,
      rotated_image_url: rotatedImageUrl,
      metafield_update_result: metafieldUpdateResponse.data,
    });
  } catch (err) {
    console.error("Fehler im /rotate-and-update:", err.message);
    if (err.response) {
      console.error("Response data:", err.response.data);
    }
    return res.status(500).json({
      error: "Interner Fehler beim Drehen oder Aktualisieren",
      details: err.message,
    });
  }
});

/**
 * Bild in S3 hochladen und öffentlich zugängliche URL zurückgeben
 */
async function uploadToS3AndGetUrl(fileBuffer, originalImageUrl) {
  if (!S3_BUCKET || !S3_REGION) {
    throw new Error("S3_BUCKET oder S3_REGION ist nicht gesetzt");
  }

  // Dateinamen aus originaler URL ableiten
  let filename = "rotated-" + Date.now() + ".png";
  try {
    const urlObj = new URL(originalImageUrl);
    const originalName = urlObj.pathname.split("/").pop();
    if (originalName) {
      // denselben Namen verwenden, aber in einen "rotated" Ordner packen
      filename = originalName.replace(/\.(png|jpg|jpeg|webp)$/i, "") + "-rotated.png";
    }
  } catch (e) {
    console.warn("Konnte originalen Dateinamen nicht parsen, fallback genutzt.");
  }

  const key = `rotated/${filename}`;

  const putCommand = new PutObjectCommand({
    Bucket: S3_BUCKET,
    Key: key,
    Body: fileBuffer,
    ContentType: "image/png",
    ACL: "public-read", // Achtung: Bucket-Policy muss Public Access erlauben
  });

  await s3Client.send(putCommand);

  // Standard S3 URL im Virtual-Hosted-Style
  const publicUrl = `https://${S3_BUCKET}.s3.${S3_REGION}.amazonaws.com/${encodeURIComponent(
    key
  ).replace(/%2F/g, "/")}`;

  return publicUrl;
}

/**
 * Produkt-Metafeld (type: single_line_text_field oder url) als STRING setzen
 */
async function setProductMetafieldString(
  productId,
  namespace,
  key,
  valueString
) {
  const url = `https://${SHOPIFY_STORE_DOMAIN}/admin/api/2024-07/graphql.json`;

  const query = `
    mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields {
          id
          namespace
          key
          value
          type
          ownerType
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const variables = {
    metafields: [
      {
        ownerId: `gid://shopify/Product/${productId}`,
        namespace: namespace,
        key: key,
        type: "single_line_text_field", // oder "url", je nach deiner Definition im Metafeld
        value: valueString,
      },
    ],
  };

  const response = await axios.post(
    url,
    {
      query,
      variables,
    },
    {
      headers: {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
      },
    }
  );

  const errors = response.data?.data?.metafieldsSet?.userErrors || [];
  if (errors.length > 0) {
    console.error("Shopify Metafield Errors:", errors);
    throw new Error("Fehler beim Setzen des Metafelds");
  }

  return response;
}

app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
