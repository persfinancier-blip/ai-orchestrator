import express from 'express';
import { pool } from './db.mjs';
import { monteCarloForecast } from './productAnalyticsCore.mjs';

function ident(value) {
  const raw = String(value || '').trim();
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(raw)) throw new Error('invalid_identifier');
  return `"${raw}"`;
}

function aggregationFor(metric) {
  const key = String(metric || '').trim().toLowerCase();
  return /(drr|roi|roas|ctr|cr|rate|ratio|share|position|price|avg|mean|percent|margin_pct)/.test(key) ? 'avg' : 'sum';
}

function period(value) {
  const raw = String(value || '').trim().toLowerCase();
  return ['day', 'week', 'month'].includes(raw) ? raw : 'day';
}

export const productForecastRouter = express.Router();
productForecastRouter.use(express.json({ limit: '1mb' }));

productForecastRouter.post('/product/forecast/business', async (req, res) => {
  const schema = String(req.body?.schema || '').trim();
  const table = String(req.body?.table || '').trim();
  const metric = String(req.body?.value_field || '').trim();
  const timeField = String(req.body?.order_field || '').trim();
  const bucket = period(req.body?.period);
  let schemaQ, tableQ, metricQ, timeQ;
  try {
    schemaQ = ident(schema); tableQ = ident(table); metricQ = ident(metric); timeQ = ident(timeField);
  } catch {
    return res.status(400).json({ error: 'invalid_forecast_source' });
  }
  const aggregation = aggregationFor(metric);
  const client = await pool.connect();
  try {
    const columns = await client.query(
      `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 AND column_name IN ($3,$4)`,
      [schema, table, metric, timeField]
    );
    if ((columns.rows || []).length < 2) return res.status(400).json({ error: 'forecast_fields_not_found' });
    const metricMeta = columns.rows.find((row) => row.column_name === metric);
    const timeMeta = columns.rows.find((row) => row.column_name === timeField);
    if (!/(int|numeric|decimal|double|real|money)/i.test(String(metricMeta?.data_type || ''))) return res.status(400).json({ error: 'forecast_metric_not_numeric' });
    if (!/(date|time)/i.test(String(timeMeta?.data_type || ''))) return res.status(400).json({ error: 'forecast_order_not_time' });

    const aggregateSql = aggregation === 'avg' ? `AVG(${metricQ})` : `SUM(${metricQ})`;
    const source = await client.query(
      `SELECT date_trunc($1, ${timeQ}) AS bucket, ${aggregateSql} AS value
         FROM ${schemaQ}.${tableQ}
        WHERE ${metricQ} IS NOT NULL AND ${timeQ} IS NOT NULL
        GROUP BY 1
        ORDER BY 1 ASC
        LIMIT 5000`,
      [bucket]
    );
    const series = (source.rows || []).map((row) => Number(row.value)).filter(Number.isFinite);
    const forecast = monteCarloForecast(series, {
      horizon: req.body?.horizon,
      simulations: req.body?.simulations,
      seed: req.body?.seed,
      target: req.body?.target
    });
    return res.json({
      source: { schema, table, value_field: metric, order_field: timeField, period: bucket, aggregation, observations: series.length },
      model_selection: { model: forecast.model, reason: `time_series_${aggregation}_${bucket}` },
      forecast
    });
  } catch (error) {
    const details = String(error?.message || error);
    return res.status(details === 'forecast_series_too_short' ? 400 : 500).json({ error: 'business_forecast_failed', details });
  } finally {
    client.release();
  }
});

export const productForecastTestkit = Object.freeze({ aggregationFor, period });
