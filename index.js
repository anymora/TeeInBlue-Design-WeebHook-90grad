import express from "express";
import fetch from "node-fetch";
import sharp from "sharp";
import dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(express.json({ limit: "10mb" }));

const PORT = process.env.PORT || 3000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

app.get("/", (req, res) => {
  console.log("GET / called");
  res.json({ status: "ok", message: "rotate service running" });
});

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

  if (!image_url || !product_id || !metafield_namespace || !metafield_key) {
    console.log("Missing fields → skip");
    return res.status(200).json({ status: "skip", missing: true });
  }

  const SHOPIFY_STORE_DOMAIN = process.env.SHOPIFY_STORE_DOMAIN;
  const SHOPIFY_ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;

  if (!SHOPIFY_STORE_DOMAIN || !SHOPIFY_ACCESS_TOKEN) {
    console.error("Missing Shopify ENV");
    return res.status(500).json({ error: "Missing Shopify ENV" });
  }

  try {
    console.log("Waiting 3 minutes...");
    await sleep(3 * 60 * 1000);

    let finalImageUrl = image_url;
    let rotatedBuffer = null;

    const resp = await fetch(image_url);
    if (!resp.ok) throw new Error(`Image fetch failed: ${resp.status}`);

    const buf = Buffer.from(await resp.arrayBuffer());

    rotatedBuffer = await sharp(buf).rotate(rotation || 90).toBuffer();

    const ownerId = `gid://shopify/Product/${product_id}`;

    console.log("Updating metafield…");

    const mutation = `
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

    const mfInput = [
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
          query: mutation,
          variables: { metafields: mfInput }
        })
      }
    );

    const gqlData = await gqlResp.json();
    console.log("MetafieldsSet response:", JSON.stringify(gqlData, null, 2));

    const errors = gqlData?.data?.metafieldsSet?.userErrors ?? [];
    if (errors.length > 0) {
      console.error("Metafield errors:", errors);
      return res.status(500).json({ error: "Shopify metafield failed", errors });
    }

    return res.status(200).json({ status: "ok", updated: true });
  } catch (e) {
    console.error("ERROR:", e);
    return res.status(500).json({ error: String(e) });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on ${PORT}`);
});
