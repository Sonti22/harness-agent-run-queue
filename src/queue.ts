import { randomUUID } from 'node:crypto';
import type { Pool, PoolClient } from 'pg';

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type Status = 'queued' | 'running' | 'completed' | 'failed';

export interface Run {
  id: string;
  dialog_id: string;
  // pg returns bigint as a string: do not lose precision by casting it to Number.
  dialog_sequence: string;
  idempotency_key: string;
  payload: Json;
  status: Status;
  attempts: number;
  worker_id: string | null;
  lease_token: string | null;
  lease_until: Date | null;
  result: Json;
  error: string | null;
  created_at: Date;
  updated_at: Date;
}

export interface ClaimedRun extends Run {
  status: 'running';
  worker_id: string;
  lease_token: string;
  lease_until: Date;
}

export interface Ownership {
  workerId: string;
  leaseToken: string;
}

export class IdempotencyConflictError extends Error {
  constructor() {
    super('The idempotency key already exists with a different payload');
    this.name = 'IdempotencyConflictError';
  }
}

export class LeaseLostError extends Error {
  constructor() {
    super('Run is missing, already finished, or the lease is expired / owned by another attempt');
    this.name = 'LeaseLostError';
  }
}

function identifier(value: string, name: string): void {
  if (typeof value !== 'string' || value.length === 0 || [...value].length > 256 || value.includes('\0')) {
    throw new TypeError(`${name} must contain 1..256 characters and no NUL`);
  }
}

// Read data descriptors once and encode only their validated values. Never call
// a getter or stringify the original objects (which could invoke toJSON()).
// JSONB equality then defines idempotency payload equality.
function encodeJson(value: Json): string {
  const ancestors = new Set<object>();
  function serialize(item: unknown): string {
    if (item === null || typeof item === 'string' || typeof item === 'boolean') return JSON.stringify(item);
    if (typeof item === 'number' && Number.isFinite(item)) return JSON.stringify(item);
    if (typeof item !== 'object') throw new TypeError('Expected a JSON value');
    if (ancestors.has(item)) throw new TypeError('Cyclic JSON value');
    const isArray = Array.isArray(item);
    const prototype = Object.getPrototypeOf(item);
    if (!isArray && prototype !== Object.prototype && prototype !== null) {
      throw new TypeError('Expected a plain JSON object');
    }
    const properties = Object.getOwnPropertyDescriptors(item);
    for (const [key, descriptor] of Object.entries(properties)) {
      if ('get' in descriptor || 'set' in descriptor) throw new TypeError('JSON properties must contain data, not accessors');
      if (key === 'toJSON' && typeof descriptor.value === 'function') throw new TypeError('Custom toJSON is not supported');
    }
    ancestors.add(item);
    try {
      if (isArray) {
        const elements: string[] = [];
        const length = properties.length!.value as number;
        for (let i = 0; i < length; i++) elements.push(serialize(properties[String(i)]?.value));
        return `[${elements.join(',')}]`;
      }
      const fields = Object.entries(properties)
        .filter(([, descriptor]) => descriptor.enumerable)
        .map(([key, descriptor]) => `${JSON.stringify(key)}:${serialize(descriptor.value)}`);
      return `{${fields.join(',')}}`;
    } finally {
      ancestors.delete(item);
    }
  }
  return serialize(value);
}

export class RunQueue {
  private readonly leaseMs: number;

  constructor(private readonly pool: Pool, options: { leaseMs?: number } = {}) {
    this.leaseMs = options.leaseMs ?? 30_000;
    if (!Number.isSafeInteger(this.leaseMs) || this.leaseMs < 1 || this.leaseMs > 86_400_000) {
      throw new TypeError('leaseMs must be an integer between 1 and 86400000');
    }
  }

  private async transaction<T>(action: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    let broken: Error | undefined;
    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      const result = await action(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        // A disconnected / unusable client must not go back into the pool.
        broken = rollbackError instanceof Error ? rollbackError : new Error('Rollback failed');
      }
      throw error;
    } finally {
      client.release(broken);
    }
  }

  async enqueue(dialogId: string, idempotencyKey: string, payload: Json): Promise<Run> {
    identifier(dialogId, 'dialogId');
    identifier(idempotencyKey, 'idempotencyKey');
    const encoded = encodeJson(payload);
    return this.transaction(async (client) => {
      await client.query('INSERT INTO dialogs(id) VALUES ($1) ON CONFLICT DO NOTHING', [dialogId]);
      await client.query('SELECT id FROM dialogs WHERE id = $1 FOR UPDATE', [dialogId]);
      const existing = await client.query<Run & { same_payload: boolean }>(
        'SELECT *, payload = $3::jsonb AS same_payload FROM runs WHERE dialog_id = $1 AND idempotency_key = $2',
        [dialogId, idempotencyKey, encoded],
      );
      const previous = existing.rows[0];
      if (previous) {
        if (!previous.same_payload) throw new IdempotencyConflictError();
        const { same_payload: _, ...run } = previous;
        return run;
      }
      const sequence = await client.query<{ last_sequence: string }>(
        'UPDATE dialogs SET last_sequence = last_sequence + 1 WHERE id = $1 RETURNING last_sequence', [dialogId],
      );
      const inserted = await client.query<Run>(
        `INSERT INTO runs(id, dialog_id, dialog_sequence, idempotency_key, payload)
         VALUES ($1, $2, $3, $4, $5::jsonb) RETURNING *`,
        [randomUUID(), dialogId, sequence.rows[0]!.last_sequence, idempotencyKey, encoded],
      );
      return inserted.rows[0]!;
    });
  }

  async claim(workerId: string): Promise<ClaimedRun | null> {
    identifier(workerId, 'workerId');
    await this.recoverExpired();
    return this.transaction(async (client) => {
      const visited: string[] = [];
      // A bounded search avoids a long transaction under pathological contention.
      // null means no work claimable now; workers should poll again with a delay.
      for (let scan = 0; scan < 64; scan++) {
        const candidates = await client.query<{ id: string }>(
          `SELECT d.id FROM dialogs d
           WHERE NOT (d.id = ANY($1::text[]))
             AND EXISTS (SELECT 1 FROM runs r WHERE r.dialog_id = d.id AND r.status = 'queued')
             AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.dialog_id = d.id AND r.status = 'running')
           ORDER BY d.id LIMIT 1 FOR UPDATE OF d SKIP LOCKED`, [visited],
        );
        const dialog = candidates.rows[0];
        if (!dialog) return null;
        visited.push(dialog.id);
        // Deliberately a separate statement: the candidate query's snapshot might
        // predate another claimer's COMMIT even though we have acquired its lock.
        const pending = await client.query<Run>(
          `SELECT * FROM runs WHERE dialog_id = $1 AND status IN ('queued', 'running')
           ORDER BY dialog_sequence LIMIT 1 FOR UPDATE`, [dialog.id],
        );
        const first = pending.rows[0];
        if (!first || first.status !== 'queued') continue;
        const claimed = await client.query<ClaimedRun>(
          `UPDATE runs SET status = 'running', attempts = attempts + 1,
             worker_id = $2, lease_token = $3,
             lease_until = clock_timestamp() + $4::double precision * interval '1 millisecond',
             updated_at = clock_timestamp()
           WHERE id = $1 AND status = 'queued' AND attempts < 3 RETURNING *`,
          [first.id, workerId, randomUUID(), this.leaseMs],
        );
        return claimed.rows[0] ?? null;
      }
      return null;
    });
  }

  private async lockRunDialog(client: PoolClient, runId: string): Promise<void> {
    const run = await client.query<{ dialog_id: string }>('SELECT dialog_id FROM runs WHERE id = $1', [runId]);
    if (!run.rows[0]) throw new LeaseLostError();
    // Every mutator locks dialog -> run, never the other way round.
    await client.query('SELECT id FROM dialogs WHERE id = $1 FOR UPDATE', [run.rows[0].dialog_id]);
    await client.query('SELECT id FROM runs WHERE id = $1 FOR UPDATE', [runId]);
  }

  async complete(runId: string, owner: Ownership, result: Json = null): Promise<Run> {
    const encoded = encodeJson(result);
    return this.transaction(async (client) => {
      await this.lockRunDialog(client, runId);
      const finished = await client.query<Run>(
        `UPDATE runs SET status = 'completed', result = $4::jsonb, error = NULL,
           worker_id = NULL, lease_token = NULL, lease_until = NULL, updated_at = clock_timestamp()
         WHERE id = $1 AND status = 'running' AND worker_id = $2 AND lease_token = $3
           AND lease_until > clock_timestamp() RETURNING *`, [runId, owner.workerId, owner.leaseToken, encoded],
      );
      if (!finished.rows[0]) throw new LeaseLostError();
      return finished.rows[0];
    });
  }

  async fail(runId: string, error: string, owner: Ownership): Promise<Run> {
    if (typeof error !== 'string' || error.includes('\0')) throw new TypeError('error must be a string without NUL');
    return this.transaction(async (client) => {
      await this.lockRunDialog(client, runId);
      const failed = await client.query<Run>(
        `UPDATE runs SET status = CASE WHEN attempts >= 3 THEN 'failed' ELSE 'queued' END,
           error = $4, worker_id = NULL, lease_token = NULL, lease_until = NULL, updated_at = clock_timestamp()
         WHERE id = $1 AND status = 'running' AND worker_id = $2 AND lease_token = $3
           AND lease_until > clock_timestamp() RETURNING *`, [runId, owner.workerId, owner.leaseToken, error.slice(0, 16_384)],
      );
      const run = failed.rows[0];
      if (!run) throw new LeaseLostError();
      if (run.status === 'failed') await this.recordTerminalError(client, run);
      return run;
    });
  }

  async renew(runId: string, owner: Ownership): Promise<ClaimedRun> {
    return this.transaction(async (client) => {
      await this.lockRunDialog(client, runId);
      const renewed = await client.query<ClaimedRun>(
        `UPDATE runs SET lease_until = GREATEST(lease_until,
             clock_timestamp() + $4::double precision * interval '1 millisecond'), updated_at = clock_timestamp()
         WHERE id = $1 AND status = 'running' AND worker_id = $2 AND lease_token = $3
           AND lease_until > clock_timestamp() RETURNING *`, [runId, owner.workerId, owner.leaseToken, this.leaseMs],
      );
      if (!renewed.rows[0]) throw new LeaseLostError();
      return renewed.rows[0];
    });
  }

  async recoverExpired(limit = 100): Promise<number> {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 10_000) throw new TypeError('limit must be 1..10000');
    return this.transaction(async (client) => {
      const dialogs = await client.query<{ id: string }>(
        `SELECT d.id FROM dialogs d WHERE EXISTS (
           SELECT 1 FROM runs r WHERE r.dialog_id = d.id AND r.status = 'running'
             AND r.lease_until <= clock_timestamp())
         ORDER BY d.id LIMIT $1 FOR UPDATE OF d SKIP LOCKED`, [limit],
      );
      if (dialogs.rows.length === 0) return 0;
      // Lock before evaluating lease validity, so time spent waiting for a row
      // lock cannot turn an expired lease into an accepted completion/heartbeat.
      await client.query(
        `SELECT id FROM runs WHERE dialog_id = ANY($1::text[]) AND status = 'running' FOR UPDATE`,
        [dialogs.rows.map((dialog) => dialog.id)],
      );
      const recovered = await client.query<Run>(
        `UPDATE runs SET status = CASE WHEN attempts >= 3 THEN 'failed' ELSE 'queued' END,
           error = 'Lease expired', worker_id = NULL, lease_token = NULL, lease_until = NULL,
           updated_at = clock_timestamp()
         WHERE dialog_id = ANY($1::text[]) AND status = 'running'
           AND lease_until <= clock_timestamp() RETURNING *`, [dialogs.rows.map((dialog) => dialog.id)],
      );
      for (const run of recovered.rows) {
        if (run.status === 'failed') await this.recordTerminalError(client, run);
      }
      return recovered.rows.length;
    });
  }

  private async recordTerminalError(client: PoolClient, run: Run): Promise<void> {
    await client.query(
      'UPDATE dialogs SET last_error = $2::jsonb WHERE id = $1',
      [run.dialog_id, JSON.stringify({ runId: run.id, attempts: run.attempts, message: run.error })],
    );
  }
}
