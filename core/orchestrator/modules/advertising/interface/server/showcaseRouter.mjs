import express from 'express';
import { pool } from './db.mjs';

export const showcaseRouter = express.Router();

showcaseRouter.get('/space', async (req, res) => {
  const limit = Math.max(1, Math.min(Number(req.query.limit ?? 500), 5000));
  const offset = Math.max(0, Number(req.query.offset ?? 0));
  try {
    const result = await pool.query('SELECT * FROM showcase.advertising LIMIT $1::int OFFSET $2::int', [limit, offset]);
    res.json({ points: result.rows, limit, offset });
  } catch (error) {
    console.error('showcase.advertising read error:', error);
    res.status(500).json({ error: 'showcase_read_failed' });
  }
});
