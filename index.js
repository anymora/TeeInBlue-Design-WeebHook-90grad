// index.js
import express from "express";
import fetch from "node-fetch";
import sharp from "sharp";
import dotenv from "dotenv";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

dotenv.config();

const app = express();
app.use(express.json({ limit: "10mb" }));

const PORT = process.env.PORT || 3000;

// Shopify ENV
const SHOPIFY_STORE_DOMAIN = process.env.SHOPIFY_STORE_DOMAIN; // z.B. anymora.myshopify.com
const SHOPIFY_ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;

// R2 ENV
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME; // z.B. anymora-rotated-designs
const R2_ENDPOINT = process.env.R2_ENDPOINT; // z.B. https://<ACCOUNT_ID>.r2.cloudflarestorage.com
const R2_PUBLIC_BASE_URL = process.env.R2_PUBLIC_BASE_URL; // z.B. https://pub-xxxx.r2.dev ODER schon mit /anymora-rotated-designs

// R2 S3 Client
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

// ---------------------------------------------------
// Helper: Shopify GraphQL Request
// ---------------------------------------------------
async function shopifyGraphQL(query, variables) {
  if (!SHOPIFY_STORE_DOMAIN || !SHOPIFY_ACCESS_TOKEN) {
    throw new Error("Missing SHOPIFY_STORE_DOMAIN or SHOPIFY_ACCESS_TOKEN");
  }

  const resp = await fetch(
    `https://${SHOPIFY_STORE_DOMAIN}/admin/api/2024-07/graphql.json`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
      },
      body: JSON.stringify({ query, variables })
    }
  );

  const data = await resp.json();
  if (!resp.ok) {
    console.error("[SHOPIFY] HTTP error:", resp.status, data);
    throw new Error(`Shopify GraphQL HTTP ${resp.status}`);
  }

  return data;
}

// ---------------------------------------------------
// Helper: Upload in R2
// ---------------------------------------------------
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

  const cmd = new PutObjectCommand({
    Bucket: R2_BUCKET_NAME,
    Key: key,
    Body: buffer,
    ContentType: "image/png"
  });

  const result = await r2Client.send(cmd);
  console.log("[JOB] R2 PutObject result:", result);

  const base = R2_PUBLIC_BASE_URL.replace(/\/$/, "");
  const bucketSegment = `/${R2_BUCKET_NAME}`;
  const baseWithBucket = base.endsWith(bucketSegment)
    ? base
    : `${base}${bucketSegment}`;

  const finalUrl = `${baseWithBucket}/${key}`;
  console.log("[JOB] Uploaded to R2 URL:", finalUrl);
  return finalUrl;
}

// ---------------------------------------------------
// Helper: Produkt-Metafeld setzen (machen wir weiter)
// ---------------------------------------------------
async function setProductMetafield(productId, namespace, key, value) {
  const ownerId = `gid://shopify/Product/${productId}`;

  const mutation = `
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

  const metafields = [
    {
      ownerId,
      namespace,
      key,
      type: "single_line_text_field",
      value
    }
  ];

  const data = await shopifyGraphQL(mutation, { metafields });
  console.log(
    "[JOB] Shopify metafieldsSet response:",
    JSON.stringify(data, null, 2)
  );

  const userErrors =
    data?.data?.metafieldsSet?.userErrors || data?.errors || [];
  if (userErrors.length > 0) {
    console.error("[JOB] Shopify Metafield Errors:", userErrors);
    throw new Error("MetafieldsSet returned userErrors");
  }
}

// ---------------------------------------------------
// Helper: Line-Item-Property in Bestellung überschreiben
//  - orderIdNumeric: z.B. 12509549003127 (payload.id)
//  - lineItemIdNumeric: payload.line_items[i].id
//  - propertyName: "_tib_design_link_1"
//  - newValue: neue R2-URL
// ---------------------------------------------------
async function updateOrderLineItemProperty(
  orderIdNumeric,
  lineItemIdNumeric,
  propertyName,
  newValue
) {
  const orderGid = `gid://shopify/Order/${orderIdNumeric}`;
  const originalLineItemGid = `gid://shopify/LineItem/${lineItemIdNumeric}`;

  console.log("[JOB] Starting order edit for order:", orderGid);
  console.log("[JOB] Original line item GID:", originalLineItemGid);

  // 1) orderEditBegin
  const beginMutation = `
    mutation orderEditBegin($id: ID!) {
      orderEditBegin(id: $id) {
        calculatedOrder {
          id
          lineItems(first: 50) {
            edges {
              node {
                id
                originalLineItem {
                  id
                }
              }
            }
          }
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const beginData = await shopifyGraphQL(beginMutation, { id: orderGid });
  console.log(
    "[JOB] orderEditBegin response:",
    JSON.stringify(beginData, null, 2)
  );

  const beginErrors =
    beginData?.data?.orderEditBegin?.userErrors || beginData?.errors || [];
  if (beginErrors.length > 0) {
    console.error("[JOB] orderEditBegin userErrors:", beginErrors);
    throw new Error("orderEditBegin returned userErrors");
  }

  const calculatedOrder =
    beginData?.data?.orderEditBegin?.calculatedOrder || null;

  if (!calculatedOrder) {
    throw new Error("No calculatedOrder returned from orderEditBegin");
  }

  const calculatedOrderId = calculatedOrder.id;

  // 2) passende calculatedLineItem-ID finden
  const edges = calculatedOrder?.lineItems?.edges || [];
  let calculatedLineItemId = null;

  for (const edge of edges) {
    const node = edge.node;
    const origId = node?.originalLineItem?.id;
    if (!origId) continue;

    if (origId === originalLineItemGid) {
      calculatedLineItemId = node.id;
      break;
    }
  }

  if (!calculatedLineItemId) {
    console.error(
      "[JOB] Could not map original line item to calculatedOrder line item"
    );
    throw new Error("No matching calculatedLineItem found");
  }

  console.log(
    "[JOB] Found calculatedLineItemId:",
    calculatedLineItemId,
    "for original line item:",
    originalLineItemGid
  );

  // 3) Properties setzen
  const setPropsMutation = `
    mutation orderEditSetLineItemProperties(
      $id: ID!
      $lineItemId: ID!
      $properties: [OrderLineItemPropertyInput!]!
    ) {
      orderEditSetLineItemProperties(
        id: $id
        lineItemId: $lineItemId
        properties: $properties
      ) {
        calculatedOrder {
          id
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const properties = [
    {
      name: propertyName,
      value: newValue
    }
  ];

  const setData = await shopifyGraphQL(setPropsMutation, {
    id: calculatedOrderId,
    lineItemId: calculatedLineItemId,
    properties
  });

  console.log(
    "[JOB] orderEditSetLineItemProperties response:",
    JSON.stringify(setData, null, 2)
  );

  const setErrors =
    setData?.data?.orderEditSetLineItemProperties?.userErrors ||
    setData?.errors ||
    [];
  if (setErrors.length > 0) {
    console.error("[JOB] orderEditSetLineItemProperties userErrors:", setErrors);
    throw new Error("orderEditSetLineItemProperties returned userErrors");
  }

  // 4) Commit
  const commitMutation = `
    mutation orderEditCommit($id: ID!, $notifyCustomer: Boolean!, $staffNote: String) {
      orderEditCommit(id: $id, notifyCustomer: $notifyCustomer, staffNote: $staffNote) {
        order {
          id
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const commitData = await shopifyGraphQL(commitMutation, {
    id: calculatedOrderId,
    notifyCustomer: false,
    staffNote: "Updated _tib_design_link_1 via webhook"
  });

  console.log(
    "[JOB] orderEditCommit response:",
    JSON.stringify(commitData, null, 2)
  );

  const commitErrors =
    commitData?.data?.orderEditCommit?.userErrors || commitData?.errors || [];
  if (commitErrors.length > 0) {
    console.error("[JOB] orderEditCommit userErrors:", commitErrors);
    throw new Error("orderEditCommit returned userErrors");
  }

  console.log(
    "[JOB] Successfully updated line item property",
    propertyName,
    "for order",
    orderIdNumeric
  );
}

// ---------------------------------------------------
// Hauptjob: Bild drehen, in R2 laden, Produkt-Metafeld & Order-Property updaten
// ---------------------------------------------------
async function processRotateAndUpdateJob(payload) {
  console.log("=== [JOB] Start rotate-and-update job ===");
  console.log("Job payload:", JSON.stringify(payload, null, 2));

  const {
    image_url,
    product_id,
    metafield_namespace,
    metafield_key,
    rotation,
    order_id,
    line_item_id
  } = payload;

  if (!image_url || !product_id || !metafield_namespace || !metafield_key) {
    console.log(
      "[JOB] Missing required fields (image_url, product_id, metafield_namespace, metafield_key)"
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

    // 1) Bild herunterladen + drehen
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
      console.error("[JOB] All attempts to rotate image failed:", lastError);
      return;
    }

    // 2) In R2 hochladen
    const filename = `rotated-${product_id}-${Date.now()}.png`;
    const finalImageUrl = await uploadToR2(rotatedImageBuffer, filename);

    // 3) Produkt-Metafeld setzen (optional, aber lassen wir drin)
    await setProductMetafield(
      product_id,
      metafield_namespace,
      metafield_key,
      finalImageUrl
    );

    // 4) Wenn Order-Infos vorhanden sind: Property im Line Item überschreiben
    if (order_id && line_item_id) {
      console.log(
        "[JOB] Updating line item property _tib_design_link_1 in order",
        order_id,
        "for line_item_id",
        line_item_id
      );

      await updateOrderLineItemProperty(
        String(order_id),
        String(line_item_id),
        "_tib_design_link_1",
        finalImageUrl
      );
    } else {
      console.log(
        "[JOB] No order_id / line_item_id in payload -> skipping order property update"
      );
    }

    console.log("[JOB] Finished job successfully, new URL:", finalImageUrl);
  } catch (err) {
    console.error("[JOB] Unexpected error in processRotateAndUpdateJob:", err);
  } finally {
    console.log("=== [JOB] End rotate-and-update job ===");
  }
}

// ---------------------------------------------------
// Routes
// ---------------------------------------------------
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
