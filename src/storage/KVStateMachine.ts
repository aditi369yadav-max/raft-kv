// ============================================================
// KV State Machine
//
// This is what Raft is protecting. The state machine applies
// committed log entries in order.
//
// Key insight: if every node starts from the same initial state
// and applies the same commands in the same order, they all
// end up with the same state. This is "state machine replication."
//
// Features beyond basic KV:
//   - TTL (time-to-live) for cache-like behavior
//   - CAS (compare-and-swap) for atomic updates
//   - Snapshots for log compaction
// ============================================================

import fs   from 'fs';
import path from 'path';
import { Command } from '../raft/types';
import { logger }  from '../utils/logger';

interface KVEntry {
  value:    string;
  expireAt: number | null; // null = no expiry
  version:  number;        // increments on each update
}

export class KVStateMachine {
  private store    = new Map<string, KVEntry>();
  private version  = 0; // global version counter
  private snapPath: string;

  constructor(dataDir: string, nodeId: number) {
    this.snapPath = path.join(dataDir, `node-${nodeId}`, 'snapshot.json');
    this.loadSnapshot();

    // TTL cleanup every second
    setInterval(() => this.evictExpired(), 1000);
  }

  // ── Apply Command ─────────────────────────────────────────
  // Called by Raft when an entry is committed
  // MUST be deterministic — same input = same output always

  apply(command: Command): string | null {
    switch (command.type) {
      case 'SET': {
        const expireAt = command.ttl
          ? Date.now() + command.ttl * 1000
          : null;
        this.store.set(command.key, {
          value: command.value, expireAt, version: ++this.version
        });
        return command.value;
      }

      case 'DELETE': {
        const existed = this.store.has(command.key);
        this.store.delete(command.key);
        return existed ? 'OK' : null;
      }

      case 'NOOP':
        return null;

      default:
        return null;
    }
  }

  // ── Read Operations ───────────────────────────────────────
  // Reads don't go through Raft log (linearizable reads need
  // lease mechanism — simplified here)

  get(key: string): string | null {
    const entry = this.store.get(key);
    if (!entry) return null;
    if (entry.expireAt && Date.now() > entry.expireAt) {
      this.store.delete(key);
      return null;
    }
    return entry.value;
  }

  // Compare-and-swap: only update if current value matches expected
  cas(key: string, expected: string, newValue: string): boolean {
    const current = this.get(key);
    if (current !== expected) return false;
    this.store.set(key, { value: newValue, expireAt: null, version: ++this.version });
    return true;
  }

  keys(pattern?: string): string[] {
    const now  = Date.now();
    const keys = [];
    for (const [key, entry] of this.store.entries()) {
      if (entry.expireAt && now > entry.expireAt) continue;
      if (!pattern || key.includes(pattern)) keys.push(key);
    }
    return keys.sort();
  }

  size(): number { return this.store.size; }

  // ── Snapshot ──────────────────────────────────────────────
  // Periodically save state to disk so we don't replay entire log

  snapshot(): void {
    try {
      const data: Record<string, KVEntry> = {};
      for (const [k, v] of this.store.entries()) data[k] = v;
      fs.writeFileSync(this.snapPath, JSON.stringify({ version: this.version, data }, null, 2));
    } catch (e: any) {
      logger.error('Snapshot failed', { error: e.message });
    }
  }

  private loadSnapshot(): void {
    try {
      if (!fs.existsSync(this.snapPath)) return;
      const { version, data } = JSON.parse(fs.readFileSync(this.snapPath, 'utf8'));
      this.version = version;
      for (const [k, v] of Object.entries(data as Record<string, KVEntry>)) {
        this.store.set(k, v);
      }
      logger.info('Loaded snapshot', { keys: this.store.size, version: this.version });
    } catch (e: any) {
      logger.error('Failed to load snapshot', { error: e.message });
    }
  }

  private evictExpired(): void {
    const now = Date.now();
    for (const [key, entry] of this.store.entries()) {
      if (entry.expireAt && now > entry.expireAt) this.store.delete(key);
    }
  }

  getStats() {
    return { size: this.store.size, version: this.version };
  }
}
