import express from "express";
import fetch from "node-fetch";
import sharp from "sharp";
import dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(express.json({ limit: "10mb" }));

const PORT = process.env.PORT || 3000;

// Helper: Sleep
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Root zum Testen
app.get("/", (req, res) => {
  console.log("GET / called");
  res.json({ status: "ok", message: "teeinblue rotate service running" });
});

// POST /rotate-and-update
app.post("/rotate-and-update", async (req, res) => {
  console.log("---- Incoming /rotate-and-update ----");
  console.log("Body:", JSON.stringify(req.body, null, 2));

  const {
    image_url,
    product_id,
    metafield_namespace,
    metafield_key,
    rotation
  } = req.body;

  // Wenn das ein Test-Call ist oder Felder fehlen: nur loggen, kein 400 zurückgeben
  if (!image_url || !product_id || !metafield_namespace || !metafield_key) {
    console.log(
      "Missing required fields (image_url, product_id, metafield_namespace, metafield_key). Skipping work, but returning 200."
    );
    return res.status(200).json({
      status: "skipped",
      reason: "Missing required fields. No rotation/metafield update executed."
    });
  }

  const SHOPIFY_STORE_DOMAIN = process.env.SHOPIFY_STORE_DOMAIN;
  const SHOPIFY_ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;

  if (!SHOPIFY_STORE_DOMAIN || !SHOPIFY_ACCESS_TOKEN) {
    console.error("Missing SHOPIFY_STORE_DOMAIN or SHOPIFY_ACCESS_TOKEN env vars");
    return res.status(500).json({ error: "Shopify credentials not configured" });
  }

  try {
    // 1) Warten 3 Minuten
    console.log("Waiting 3 minutes before first attempt...");
    await sleep(3 * 60 * 1000);

    const maxAttempts = 4;
    let lastError = null;
    let rotatedImageBuffer = null;
    let finalImageUrl = null;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        console.log(`Attempt ${attempt} to download image: ${image_url}`);
        const resp = await fetch(image_url);
        if (!resp.ok) {
          throw new Error(`Image fetch failed with status ${resp.status}`);
        }
        const buffer = Buffer.from(await resp.arrayBuffer());

        console.log("Rotating image by", rotation || 90, "degrees");
        rotatedImageBuffer = await sharp(buffer).rotate(rotation || 90).toBuffer();

        // Hier könntest du dein eigenes Hosting nehmen.
        // Zum Test: wir tun so, als hätten wir eine neue URL (eigentlich gleich).
        finalImageUrl = image_url; // TODO: echte Upload-URL nutzen (S3 etc.)

        console.log("Image rotated successfully");
        break;
      } catch (err) {
        console.error(`Error in attempt ${attempt}:`, err);
        lastError = err;
        if (attempt < maxAttempts) {
          console.log("Waiting 1 minute before next attempt...");
          await sleep(60 * 1000);
        }
      }
    }

    if (!rotatedImageBuffer || !finalImageUrl) {
      console.error("All attempts to process image failed:", lastError);
      return res.status(500).json({ error: "Failed to process image" });
    }

    // 3) Metafeld in Shopify updaten (GraphQL metafieldsSet)
    const ownerId = `gid://shopify/Product/${product_id}`;
    console.log("Updating metafield via Shopify Admin API for ownerId:", ownerId);

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
        type: "single_line_text_field", // ggf. auf "url" ändern, wenn dein Feld URL-Typ ist
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
    console.log("Shopify metafieldsSet response:", JSON.stringify(gqlData, null, 2));

    const userErrors =
      gqlData?.data?.metafieldsSet?.userErrors ||
      gqlData?.errors ||
      [];

    if (userErrors.length > 0) {
      console.error("Shopify Metafield Errors:", userErrors);
      return res.status(500).json({ error: "Failed to set metafield", userErrors });
    }

    console.log("Metafield updated successfully.");
    return res.status(200).json({
      status: "ok",
      product_id,
      metafield_namespace,
      metafield_key,
      value: finalImageUrl
    });
  } catch (err) {
    console.error("Unexpected error in /rotate-and-update:", err);
    return res.status(500).json({ error: "Internal error", details: String(err) });
  }
});

app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
