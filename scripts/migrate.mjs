import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import pg from 'pg';

if (!process.env.DATABASE_URL) throw new Error('Set DATABASE_URL before running migrations');
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const client = await pool.connect();
try {
  const sql = (await readFile(new URL('../sql/001_queue.sql', import.meta.url), 'utf8')).replaceAll('\r\n', '\n');
  const checksum = createHash('sha256').update(sql).digest('hex');
  await client.query('BEGIN');
  await client.query("SELECT pg_advisory_xact_lock(hashtext('harness-queue-migrations'))");
  await client.query('CREATE TABLE IF NOT EXISTS queue_migrations (version text PRIMARY KEY, checksum text NOT NULL)');
  const existing = await client.query('SELECT checksum FROM queue_migrations WHERE version = $1', ['001']);
  if (existing.rowCount === 0) {
    await client.query(sql);
    await client.query('INSERT INTO queue_migrations(version, checksum) VALUES ($1, $2)', ['001', checksum]);
    console.log('Applied migration 001');
  } else if (existing.rows[0].checksum !== checksum) {
    throw new Error('Migration 001 checksum changed; create a new migration instead');
  } else {
    console.log('Migration 001 already applied');
  }
  await client.query('COMMIT');
} catch (error) {
  await client.query('ROLLBACK');
  throw error;
} finally {
  client.release();
  await pool.end();
}
