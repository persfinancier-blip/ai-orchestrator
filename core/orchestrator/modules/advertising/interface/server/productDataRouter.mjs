import express from 'express';
import { pool } from './db.mjs';
import { materializeCleanDataProduct } from './dataProductMaterializer.mjs';
import { readContainerId } from './productWorkspaceRouter.mjs';

export const productDataRouter = express.Router();
productDataRouter.use(express.json({ limit: '1mb' }));

function positiveId(value) {
  const id = Math.trunc(Number(value || 0));
  return id > 0 ? id : 0;
}

productDataRouter.post('/product/data-products/materialize', async (req, res) => {
  const id = positiveId(req.body?.id);
  if (!id) return res.status(400).json({ error: 'data_product_id_required' });
  const selectedContainerId = readContainerId(req);
  const client = await pool.connect();
  try {
    const result = await client.query(
      `SELECT id,container_id,name,source_schema,source_table,stage,semantic_schema_json,normalization_json,status
         FROM ao_product.data_products_store
        WHERE id=$1
          AND ($2::bigint=0 OR container_id=$2)
          AND status <> 'archived'
        LIMIT 1`,
      [id, selectedContainerId || 0]
    );
    const dataProduct = result.rows?.[0] || null;
    if (!dataProduct) return res.status(404).json({ error: 'data_product_not_found' });
    const materialized = await materializeCleanDataProduct(client, dataProduct);
    const normalization = dataProduct.normalization_json && typeof dataProduct.normalization_json === 'object'
      ? { ...dataProduct.normalization_json }
      : {};
    normalization.materialized = materialized;
    normalization.materialized_at = new Date().toISOString();
    const saved = await client.query(
      `UPDATE ao_product.data_products_store
          SET stage='clean',status='ready',normalization_json=$2::jsonb,updated_at=now()
        WHERE id=$1
        RETURNING id,container_id,name,source_schema,source_table,stage,normalization_json,status,updated_at`,
      [id, JSON.stringify(normalization)]
    );
    return res.json({ ok: true, data_product: saved.rows?.[0] || null, materialized });
  } catch (error) {
    return res.status(500).json({ error: 'data_product_materialize_failed', details: String(error?.message || error) });
  } finally {
    client.release();
  }
});
