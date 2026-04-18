// ============================================================
// Persistent Storage
//
// Raft requires 3 things to survive crashes:
//   1. currentTerm — never forget what term you're in
//   2. votedFor    — never vote twice in the same term
//   3. log         — never lose committed entries
//
// If these aren't persisted, the cluster can elect multiple
// leaders in the same term — violating safety guarantees.
// ============================================================

import fs   from 'fs';
import path from 'path';
import { PersistentState, LogEntry } from './types';
import { logger } from '../utils/logger';

export class RaftStorage {
  private statePath: string;
  private logPath:   string;
  private state:     PersistentState;

  constructor(dataDir: string, nodeId: number) {
    const dir = path.join(dataDir, `node-${nodeId}`);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    this.statePath = path.join(dir, 'state.json');
    this.logPath   = path.join(dir, 'raft.log');
    this.state     = this.load();
  }

  // ── Load ──────────────────────────────────────────────────

  private load(): PersistentState {
    try {
      if (fs.existsSync(this.statePath)) {
        const state = JSON.parse(fs.readFileSync(this.statePath, 'utf8')) as PersistentState;
        logger.info('Recovered Raft state', {
          term:     state.currentTerm,
          votedFor: state.votedFor,
          logLen:   state.log.length,
        });
        return state;
      }
    } catch (e: any) {
      logger.error('Failed to load state', { error: e.message });
    }
    return { currentTerm: 0, votedFor: null, log: [] };
  }

  // ── Save ──────────────────────────────────────────────────
  // Must be called before replying to any RPC
  // This is what "durable" means in distributed systems

  save(): void {
    try {
      fs.writeFileSync(this.statePath, JSON.stringify(this.state, null, 2));
    } catch (e: any) {
      logger.error('CRITICAL: Failed to persist state', { error: e.message });
      // In production: crash the node rather than risk inconsistency
    }
  }

  // ── Accessors ─────────────────────────────────────────────

  getCurrentTerm():  number        { return this.state.currentTerm; }
  getVotedFor():     number | null { return this.state.votedFor;    }
  getLog():          LogEntry[]    { return this.state.log;         }
  getLogLength():    number        { return this.state.log.length;  }

  getLastLogIndex(): number {
    return this.state.log.length - 1;
  }

  getLastLogTerm(): number {
    if (this.state.log.length === 0) return 0;
    return this.state.log[this.state.log.length - 1].term;
  }

  getEntry(index: number): LogEntry | null {
    return this.state.log[index] ?? null;
  }

  getEntriesFrom(index: number): LogEntry[] {
    return this.state.log.slice(index);
  }

  // ── Mutations ─────────────────────────────────────────────
  // All mutations persist immediately

  setCurrentTerm(term: number): void {
    this.state.currentTerm = term;
    this.state.votedFor    = null; // reset vote on new term
    this.save();
  }

  setVotedFor(candidateId: number): void {
    this.state.votedFor = candidateId;
    this.save();
  }

  appendEntries(entries: LogEntry[]): void {
    this.state.log.push(...entries);
    this.save();
  }

  // Truncate log from index onwards (conflict resolution)
  truncateFrom(index: number): void {
    this.state.log = this.state.log.slice(0, index);
    this.save();
  }
}
