// core/orchestrator/modules/advertising/interface/server/db.mjs
import pg from 'pg';

const { Pool } = pg;

function required(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env: ${name}`);
  return v;
}

const pool = new Pool({
  host: process.env.PGHOST,
  port: Number(process.env.PGPORT),
  user: process.env.PGUSER || required('PGUSER'),
  password: process.env.PGPASSWORD || required('PGPASSWORD'),
  database: process.env.PGDATABASE || required('PGDATABASE'),
  max: Number(process.env.PGPOOL_MAX || 10),
  idleTimeoutMillis: Number(process.env.PGPOOL_IDLE_MS || 30_000),
  connectionTimeoutMillis: Number(process.env.PGPOOL_CONN_MS || 10_000),
});

export { pool };
