import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { after, before, beforeEach, test } from 'node:test';
import pg from 'pg';
import { IdempotencyConflictError, LeaseLostError, RunQueue, type ClaimedRun, type Json } from '../src/queue.js';

if (!process.env.DATABASE_URL) throw new Error('Set DATABASE_URL to an isolated PostgreSQL test database');
const schema = `queue_test_${randomUUID().replaceAll('-', '')}`;
const admin = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 16,
  options: `-c search_path=${schema} -c statement_timeout=10000 -c lock_timeout=5000`,
});
const queue = new RunQueue(pool, { leaseMs: 30_000 });
const owner = (run: ClaimedRun) => ({ workerId: run.worker_id, leaseToken: run.lease_token });

async function take(worker = 'worker'): Promise<ClaimedRun> {
  const run = await queue.claim(worker);
  assert.ok(run, 'expected claimable work');
  return run;
}

async function expire(run: ClaimedRun): Promise<void> {
  await pool.query("UPDATE runs SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1", [run.id]);
}

async function waitUntil(check: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 4000;
  while (!(await check())) {
    assert.ok(Date.now() < deadline, 'timed out waiting for PostgreSQL barrier');
    await delay(10);
  }
}

before(async () => {
  await admin.query(`CREATE SCHEMA ${schema}`);
  const sql = await readFile(new URL('../../sql/001_queue.sql', import.meta.url), 'utf8');
  await pool.query(sql);
});
beforeEach(async () => { await pool.query('TRUNCATE runs, dialogs'); });
after(async () => {
  await pool.end();
  await admin.query(`DROP SCHEMA ${schema} CASCADE`);
  await admin.end();
});

test('same key and JSONB-equivalent payload return the existing run without a sequence gap', async () => {
  const first = await queue.enqueue('dialog', 'key', { a: 1, b: [true, null] });
  const duplicate = await queue.enqueue('dialog', 'key', { b: [true, null], a: 1 });
  assert.equal(duplicate.id, first.id);
  assert.equal(duplicate.dialog_sequence, '1');
  const second = await queue.enqueue('dialog', 'key-2', {});
  assert.equal(second.dialog_sequence, '2');
  assert.equal((await pool.query('SELECT count(*) FROM runs')).rows[0].count, '2');
});

test('24 simultaneous identical enqueues create one row', async () => {
  const runs = await Promise.all(Array.from({ length: 24 }, () => queue.enqueue('dialog', 'key', { text: 'Привет' })));
  assert.equal(new Set(runs.map((run) => run.id)).size, 1);
  assert.equal((await pool.query('SELECT count(*) FROM runs')).rows[0].count, '1');
});

test('same key with a different payload is a conflict and rolls back', async () => {
  await queue.enqueue('dialog', 'key', { text: 'one' });
  await assert.rejects(queue.enqueue('dialog', 'key', { text: 'two' }), IdempotencyConflictError);
  assert.equal((await pool.query('SELECT last_sequence FROM dialogs')).rows[0].last_sequence, '1');
});

test('idempotency keys are scoped to the dialog and survive completion', async () => {
  const a = await queue.enqueue('a', 'key', null);
  const b = await queue.enqueue('b', 'key', null);
  assert.notEqual(a.id, b.id);
  const run = await take();
  await queue.complete(run.id, owner(run), { answer: 'ok' });
  const again = await queue.enqueue(run.dialog_id, 'key', null);
  assert.equal(again.status, 'completed');
  assert.deepEqual(again.result, { answer: 'ok' });
});

test('invalid JSON and identifiers do not enqueue altered requests', async () => {
  for (const value of [undefined, NaN, Infinity, { missing: undefined }, new Date(), [, 1]]) {
    await assert.rejects(queue.enqueue('a', 'key', value as Json), TypeError);
  }
  const cyclic: { self?: unknown } = {};
  cyclic.self = cyclic;
  await assert.rejects(queue.enqueue('a', 'key', cyclic as Json), TypeError);
  await assert.rejects(queue.enqueue('', 'key', {}), TypeError);
  await assert.rejects(queue.enqueue('a', '\0', {}), TypeError);
  await assert.rejects(queue.enqueue('a'.repeat(257), 'key', {}), TypeError);
  assert.equal((await pool.query('SELECT count(*) FROM runs')).rows[0].count, '0');
});

test('three simultaneous claimers cannot take the same run', async () => {
  await queue.enqueue('dialog', 'key', {});
  const runs = await Promise.all(['w1', 'w2', 'w3'].map((worker) => queue.claim(worker)));
  assert.equal(runs.filter(Boolean).length, 1);
  assert.equal((await pool.query("SELECT count(*) FROM runs WHERE status = 'running'")).rows[0].count, '1');
});

test('different dialogs can run in parallel; the second message waits', async () => {
  const a1 = await queue.enqueue('a', '1', {});
  await queue.enqueue('a', '2', {});
  const b1 = await queue.enqueue('b', '1', {});
  const claims = (await Promise.all(['w1', 'w2', 'w3'].map((worker) => queue.claim(worker)))).filter((run) => run !== null);
  assert.deepEqual(new Set(claims.map((run) => run.id)), new Set([a1.id, b1.id]));
  assert.equal(await queue.claim('w4'), null);
});

test('FIFO does not let the second message overtake the first during retries', async () => {
  const first = await queue.enqueue('dialog', '1', {});
  const second = await queue.enqueue('dialog', '2', {});
  const attempt1 = await take();
  assert.equal(attempt1.id, first.id);
  await queue.fail(attempt1.id, 'temporary', owner(attempt1));
  const attempt2 = await take();
  assert.equal(attempt2.id, first.id);
  assert.equal(attempt2.attempts, 2);
  await queue.complete(attempt2.id, owner(attempt2));
  assert.equal((await take()).id, second.id);
});

test('three failures terminate the run, record the dialog error, and unblock the next message', async () => {
  const first = await queue.enqueue('dialog', '1', {});
  const second = await queue.enqueue('dialog', '2', {});
  for (let attempt = 1; attempt <= 3; attempt++) {
    const run = await take();
    assert.equal(run.id, first.id);
    assert.equal(run.attempts, attempt);
    const failed = await queue.fail(run.id, `error-${attempt}`, owner(run));
    assert.equal(failed.status, attempt === 3 ? 'failed' : 'queued');
    assert.equal(failed.worker_id, null);
    assert.equal(failed.lease_token, null);
  }
  const state = (await pool.query('SELECT last_error FROM dialogs')).rows[0].last_error;
  assert.deepEqual(state, { runId: first.id, attempts: 3, message: 'error-3' });
  assert.equal((await take()).id, second.id);
});

test('expired leases are recovered automatically by claim and receive a new token', async () => {
  await queue.enqueue('dialog', '1', {});
  const dead = await take('dead-worker');
  await expire(dead);
  const replacement = await take('replacement');
  assert.equal(replacement.id, dead.id);
  assert.equal(replacement.attempts, 2);
  assert.notEqual(replacement.lease_token, dead.lease_token);
});

test('three expired attempts are failed, never claimed for a fourth time', async () => {
  const first = await queue.enqueue('dialog', '1', {});
  const second = await queue.enqueue('dialog', '2', {});
  for (let attempt = 1; attempt <= 3; attempt++) {
    const run = await take();
    assert.equal(run.id, first.id);
    assert.equal(run.attempts, attempt);
    await expire(run);
    assert.equal(await queue.recoverExpired(), 1);
  }
  assert.equal((await pool.query('SELECT status FROM runs WHERE id = $1', [first.id])).rows[0].status, 'failed');
  assert.equal((await take()).id, second.id);
});

test('expired owner cannot complete, fail or renew even before recovery runs', async () => {
  await queue.enqueue('dialog', '1', {});
  const run = await take();
  await expire(run);
  await assert.rejects(queue.complete(run.id, owner(run)), LeaseLostError);
  await assert.rejects(queue.fail(run.id, 'late', owner(run)), LeaseLostError);
  await assert.rejects(queue.renew(run.id, owner(run)), LeaseLostError);
});

test('an old token is rejected after reassignment, even if worker_id is reused', async () => {
  await queue.enqueue('dialog', '1', {});
  const old = await take('same-worker');
  await expire(old);
  const current = await take('same-worker');
  await assert.rejects(queue.complete(old.id, owner(old), { bad: true }), LeaseLostError);
  await assert.rejects(queue.fail(old.id, 'late', owner(old)), LeaseLostError);
  await assert.rejects(queue.renew(old.id, owner(old)), LeaseLostError);
  const completed = await queue.complete(current.id, owner(current), { good: true });
  assert.deepEqual(completed.result, { good: true });
});

test('heartbeat preserves ownership and recovery leaves a valid lease alone', async () => {
  await queue.enqueue('dialog', '1', {});
  const run = await take();
  const renewed = await queue.renew(run.id, owner(run));
  assert.ok(renewed.lease_until >= run.lease_until);
  assert.equal(renewed.lease_token, run.lease_token);
  assert.equal(await queue.recoverExpired(), 0);
  await assert.rejects(queue.complete(run.id, { ...owner(run), workerId: 'wrong' }), LeaseLostError);
});

test('completion and failure are mutually exclusive terminal decisions', async () => {
  await queue.enqueue('dialog', '1', {});
  const run = await take();
  const results = await Promise.allSettled([
    queue.complete(run.id, owner(run)),
    queue.fail(run.id, 'competing failure', owner(run)),
  ]);
  assert.equal(results.filter((result) => result.status === 'fulfilled').length, 1);
  const rejected = results.find((result) => result.status === 'rejected');
  assert.ok(rejected && rejected.reason instanceof LeaseLostError);
});

test('concurrent recovery processes recover one expired attempt only once', async () => {
  await queue.enqueue('dialog', '1', {});
  const run = await take();
  await expire(run);
  const recovered = await Promise.all([queue.recoverExpired(), queue.recoverExpired(), queue.recoverExpired()]);
  assert.equal(recovered.reduce((sum, count) => sum + count, 0), 1);
  assert.equal((await pool.query('SELECT attempts FROM runs')).rows[0].attempts, 1);
});

test('SQL partial unique index prevents a second running row if a caller bypasses the module', async () => {
  await queue.enqueue('dialog', '1', {});
  const second = await queue.enqueue('dialog', '2', {});
  await take();
  await assert.rejects(pool.query(
    `UPDATE runs SET status = 'running', attempts = 1, worker_id = 'rogue', lease_token = $2,
      lease_until = clock_timestamp() + interval '1 minute' WHERE id = $1`, [second.id, randomUUID()],
  ), (error: unknown) => (error as { code: string }).code === '23505');
});

test('three workers process 72 runs with real overlap across dialogs and no overlap within any dialog', async () => {
  const dialogs = 6;
  const perDialog = 12;
  for (let message = 0; message < perDialog; message++) {
    await Promise.all(Array.from({ length: dialogs }, (_, dialog) => queue.enqueue(`dialog-${dialog}`, `${message}`, { message })));
  }
  const active = new Set<string>();
  const completed = new Set<string>();
  const sequences = new Map<string, number[]>();
  let peak = 0;
  await Promise.all(['w1', 'w2', 'w3'].map(async (worker) => {
    const deadline = Date.now() + 10_000;
    while (completed.size < dialogs * perDialog) {
      assert.ok(Date.now() < deadline, 'workers stalled');
      const run = await queue.claim(worker);
      if (!run) { await delay(2); continue; }
      assert.equal(active.has(run.dialog_id), false, `overlap in ${run.dialog_id}`);
      assert.equal(completed.has(run.id), false, 'duplicate successful execution');
      active.add(run.dialog_id);
      peak = Math.max(peak, active.size);
      const history = sequences.get(run.dialog_id) ?? [];
      history.push(Number(run.dialog_sequence));
      sequences.set(run.dialog_id, history);
      await delay(5);
      // Remove the in-process marker before completion unblocks the next claim.
      active.delete(run.dialog_id);
      await queue.complete(run.id, owner(run), { worker });
      completed.add(run.id);
    }
  }));
  assert.equal(completed.size, 72);
  assert.ok(peak >= 2, 'test must observe actual parallel work across dialogs');
  for (const history of sequences.values()) assert.deepEqual(history, Array.from({ length: perDialog }, (_, i) => i + 1));
  assert.equal((await pool.query("SELECT count(*) FROM runs WHERE status = 'completed'")).rows[0].count, '72');
});

test('candidate selection rechecks a fresh snapshot after another claim commits', async () => {
  await queue.enqueue('dialog', '1', {});
  await queue.enqueue('dialog', '2', {});
  const barrier = await pool.connect();
  const originalConnect = pool.connect.bind(pool);
  const intercepted = new pg.Pool();
  // A test-only advisory barrier pauses candidate evaluation BEFORE row locking.
  // Another transaction commits a claim during this query's old snapshot.
  intercepted.connect = (async () => {
    const client = await originalConnect();
    const originalQuery = client.query.bind(client);
    client.query = ((sql: string, values?: unknown[]) => {
      if (sql.includes('SELECT d.id FROM dialogs d') && sql.includes('ANY($1')) {
        return originalQuery(sql.replace('SELECT d.id FROM', 'SELECT d.id, pg_advisory_xact_lock(81264321) FROM') + ' /* snapshot_barrier */', values);
      }
      return originalQuery(sql, values);
    }) as typeof client.query;
    const originalRelease = client.release.bind(client);
    client.release = (error?: Error | boolean) => {
      client.query = originalQuery as typeof client.query;
      originalRelease(error);
    };
    return client;
  }) as typeof intercepted.connect;
  let pending: Promise<ClaimedRun | null> | undefined;
  try {
    await barrier.query('SELECT pg_advisory_lock(81264321)');
    pending = new RunQueue(intercepted).claim('stale-snapshot');
    await waitUntil(async () => (await admin.query(
      "SELECT 1 FROM pg_stat_activity WHERE query LIKE '%/* snapshot_barrier */%' AND wait_event = 'advisory'",
    )).rowCount !== 0);
    const winner = await take('winner');
    await barrier.query('SELECT pg_advisory_unlock(81264321)');
    assert.equal(await pending, null);
    assert.equal((await pool.query("SELECT id FROM runs WHERE status = 'running'")).rows[0].id, winner.id);
  } finally {
    await barrier.query('SELECT pg_advisory_unlock_all()');
    if (pending) await pending.catch(() => undefined);
    barrier.release();
    await intercepted.end();
  }
});

test('completion checks expiry AFTER waiting for the dialog lock', async () => {
  await queue.enqueue('dialog', '1', {});
  const run = await take();
  const blocker = await pool.connect();
  let completion: Promise<unknown> | undefined;
  try {
    await blocker.query('BEGIN');
    await blocker.query('SELECT id FROM dialogs WHERE id = $1 FOR UPDATE', [run.dialog_id]);
    completion = queue.complete(run.id, owner(run));
    // Attach an immediate handler to avoid an unhandled rejection during cleanup.
    const outcome = completion.then(() => null, (error: unknown) => error);
    await waitUntil(async () => (await admin.query(
      "SELECT 1 FROM pg_stat_activity WHERE query LIKE 'SELECT id FROM dialogs WHERE id =%' AND wait_event_type = 'Lock'",
    )).rowCount !== 0);
    await blocker.query("UPDATE runs SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1", [run.id]);
    await blocker.query('COMMIT');
    assert.ok((await outcome) instanceof LeaseLostError);
  } finally {
    await blocker.query('ROLLBACK');
    if (completion) await completion.catch(() => undefined);
    blocker.release();
  }
});

test('recoverExpired honors the batch size and skips dialogs being mutated', async () => {
  await queue.enqueue('a', '1', {});
  await queue.enqueue('b', '1', {});
  const a = await take();
  const b = await take();
  await expire(a);
  await expire(b);
  const blocker = await pool.connect();
  try {
    await blocker.query('BEGIN');
    await blocker.query('SELECT id FROM dialogs WHERE id = $1 FOR UPDATE', [a.dialog_id]);
    assert.equal(await queue.recoverExpired(1), 1);
    assert.equal((await pool.query('SELECT status FROM runs WHERE id = $1', [a.id])).rows[0].status, 'running');
    await blocker.query('COMMIT');
    assert.equal(await queue.recoverExpired(1), 1);
  } finally {
    await blocker.query('ROLLBACK');
    blocker.release();
  }
});
