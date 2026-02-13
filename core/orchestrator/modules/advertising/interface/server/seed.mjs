import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { pool } from './db.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const files = ['001_space_schema.sql', '002_space_seed.sql'];

for (const file of files) {
  const sql = fs.readFileSync(path.join(__dirname, 'sql', file), 'utf8');
  await pool.query(sql);
  console.log(`Выполнен ${file}`);
}

await pool.end();
