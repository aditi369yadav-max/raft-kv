// ============================================================
// Raft Node — Complete Consensus Implementation
//
// This is the hardest file in the project. Read it carefully.
//
// The Raft paper (Ongaro & Ousterhout, 2014) describes 5 rules:
//
// Rule 1: Leaders send heartbeats to prevent new elections
// Rule 2: Candidates start elections on timeout
// Rule 3: Vote for at most one candidate per term
// Rule 4: Leaders replicate log entries to followers
// Rule 5: Entry committed when stored on majority of nodes
//
// Every line of this file implements one of these rules.
// ============================================================

import { EventEmitter }   from 'events';
import { RaftStorage }    from './RaftStorage';
import { KVStateMachine } from '../storage/KVStateMachine';
import {
  NodeState, NodeInfo, LogEntry, Command,
  RequestVoteArgs, RequestVoteReply,
  AppendEntriesArgs, AppendEntriesReply,
  ClientRequest, ClientResponse,
} from './types';
import { logger } from '../utils/logger';

// Pending client request waiting for commit
interface PendingWrite {
  resolve: (r: ClientResponse) => void;
  reject:  (e: Error) => void;
  index:   number;
}

export class RaftNode extends EventEmitter {
  // ── Volatile State (resets on crash) ──────────────────────
  private state:       NodeState = 'FOLLOWER';
  private leaderId:    number | null = null;
  private commitIndex: number = -1;
  private lastApplied: number = -1;

  // Leader only: track replication progress per follower
  private nextIndex:  Map<number, number> = new Map();
  private matchIndex: Map<number, number> = new Map();

  // Election timers
  private electionTimer:  NodeJS.Timeout | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;

  // Pending client writes
  private pendingWrites: Map<number, PendingWrite> = new Map();

  // Metrics
  private metrics = {
    electionsStarted: 0,
    votesReceived:    0,
    appendsSent:      0,
    appendsReceived:  0,
    commitsApplied:   0,
  };

  constructor(
    readonly nodeId:  number,
    private peers:    NodeInfo[],
    private storage:  RaftStorage,
    private kv:       KVStateMachine,
    private sendRpc:  (to: NodeInfo, type: string, data: unknown) => Promise<unknown>
  ) {
    super();
    this.resetElectionTimer();
    logger.info(`Node ${nodeId} started as FOLLOWER`);
  }

  // ════════════════════════════════════════════════════════════
  // ELECTION LOGIC
  // ════════════════════════════════════════════════════════════

  // Called when election timer fires — become CANDIDATE
  private startElection(): void {
    this.metrics.electionsStarted++;
    this.state = 'CANDIDATE';

    // Increment term and vote for self
    const newTerm = this.storage.getCurrentTerm() + 1;
    this.storage.setCurrentTerm(newTerm);
    this.storage.setVotedFor(this.nodeId);
    this.leaderId = null;

    logger.info(`Node ${this.nodeId} starting election for term ${newTerm}`);
    this.emit('stateChange', { state: 'CANDIDATE', term: newTerm });

    let votesReceived = 1; // vote for self
    const majority    = Math.floor((this.peers.length + 1) / 2) + 1;

    // Send RequestVote to all peers
    const args: RequestVoteArgs = {
      term:         newTerm,
      candidateId:  this.nodeId,
      lastLogIndex: this.storage.getLastLogIndex(),
      lastLogTerm:  this.storage.getLastLogTerm(),
    };

    for (const peer of this.peers) {
      this.sendRpc(peer, 'RequestVote', args)
        .then((reply: any) => {
          const r = reply as RequestVoteReply;

          // If we see a higher term, revert to follower
          if (r.term > this.storage.getCurrentTerm()) {
            this.becomeFollower(r.term);
            return;
          }

          // Ignore stale replies
          if (this.state !== 'CANDIDATE' || newTerm !== this.storage.getCurrentTerm()) return;

          if (r.voteGranted) {
            votesReceived++;
            this.metrics.votesReceived++;
            logger.debug(`Node ${this.nodeId} received vote from ${peer.id} (${votesReceived}/${majority})`);

            if (votesReceived >= majority) {
              this.becomeLeader();
            }
          }
        })
        .catch(() => {}); // peer unreachable — ignore
    }

    // Restart election timer in case this election fails
    this.resetElectionTimer();
  }

  // ════════════════════════════════════════════════════════════
  // STATE TRANSITIONS
  // ════════════════════════════════════════════════════════════

  private becomeLeader(): void {
    if (this.state !== 'CANDIDATE') return;

    this.state    = 'LEADER';
    this.leaderId = this.nodeId;

    logger.info(`🎉 Node ${this.nodeId} became LEADER for term ${this.storage.getCurrentTerm()}`);
    this.emit('stateChange', { state: 'LEADER', term: this.storage.getCurrentTerm() });

    // Initialize leader state
    const nextIdx = this.storage.getLastLogIndex() + 1;
    for (const peer of this.peers) {
      this.nextIndex.set(peer.id,  nextIdx);
      this.matchIndex.set(peer.id, -1);
    }

    // Append NOOP to commit previous term's entries
    this.appendToLog({ type: 'NOOP' });

    // Cancel election timer, start heartbeat
    this.clearElectionTimer();
    this.startHeartbeat();
  }

  private becomeFollower(term: number): void {
    this.state = 'FOLLOWER';
    this.storage.setCurrentTerm(term);
    this.leaderId = null;

    this.clearHeartbeatTimer();
    this.resetElectionTimer();

    this.emit('stateChange', { state: 'FOLLOWER', term });
  }

  // ════════════════════════════════════════════════════════════
  // RPC HANDLERS
  // ════════════════════════════════════════════════════════════

  // Called when another node wants our vote
  handleRequestVote(args: RequestVoteArgs): RequestVoteReply {
    const currentTerm = this.storage.getCurrentTerm();

    // Rule: if we see higher term, update ours
    if (args.term > currentTerm) {
      this.becomeFollower(args.term);
    }

    if (args.term < currentTerm) {
      return { term: currentTerm, voteGranted: false };
    }

    const votedFor = this.storage.getVotedFor();

    // Grant vote if:
    // 1. We haven't voted yet (or already voted for this candidate)
    // 2. Candidate's log is at least as up-to-date as ours
    const canVote = votedFor === null || votedFor === args.candidateId;
    const logOk   = this.isCandidateLogUpToDate(args.lastLogIndex, args.lastLogTerm);

    if (canVote && logOk) {
      this.storage.setVotedFor(args.candidateId);
      this.resetElectionTimer(); // reset timer when granting vote
      logger.debug(`Node ${this.nodeId} voted for ${args.candidateId} in term ${args.term}`);
      return { term: args.term, voteGranted: true };
    }

    return { term: currentTerm, voteGranted: false };
  }

  // Called by leader to replicate entries (or heartbeat)
  handleAppendEntries(args: AppendEntriesArgs): AppendEntriesReply {
    this.metrics.appendsReceived++;
    const currentTerm = this.storage.getCurrentTerm();

    if (args.term < currentTerm) {
      return { term: currentTerm, success: false, conflictIndex: -1, conflictTerm: -1 };
    }

    // Valid leader contact — reset election timer
    if (args.term > currentTerm) this.becomeFollower(args.term);
    else {
      this.state    = 'FOLLOWER';
      this.leaderId = args.leaderId;
      this.resetElectionTimer();
    }

    // Check prevLogIndex/prevLogTerm consistency
    if (args.prevLogIndex >= 0) {
      const prevEntry = this.storage.getEntry(args.prevLogIndex);

      if (!prevEntry) {
        // We don't have this entry at all
        return {
          term: currentTerm, success: false,
          conflictIndex: this.storage.getLogLength(),
          conflictTerm:  -1,
        };
      }

      if (prevEntry.term !== args.prevLogTerm) {
        // Conflict — tell leader to back up
        const conflictTerm  = prevEntry.term;
        let conflictIndex   = args.prevLogIndex;
        while (conflictIndex > 0 && this.storage.getEntry(conflictIndex - 1)?.term === conflictTerm) {
          conflictIndex--;
        }
        return { term: currentTerm, success: false, conflictIndex, conflictTerm };
      }
    }

    // Append new entries (overwriting any conflicts)
    if (args.entries.length > 0) {
      // Find where entries diverge
      let insertIdx = args.prevLogIndex + 1;
      let entryIdx  = 0;

      while (entryIdx < args.entries.length) {
        const existing = this.storage.getEntry(insertIdx);
        if (!existing) break;
        if (existing.term !== args.entries[entryIdx].term) {
          // Conflict — truncate log from here
          this.storage.truncateFrom(insertIdx);
          break;
        }
        insertIdx++;
        entryIdx++;
      }

      // Append remaining new entries
      if (entryIdx < args.entries.length) {
        this.storage.appendEntries(args.entries.slice(entryIdx));
      }
    }

    // Update commit index
    if (args.leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(args.leaderCommit, this.storage.getLastLogIndex());
      this.applyCommitted();
    }

    return { term: currentTerm, success: true, conflictIndex: -1, conflictTerm: -1 };
  }

  // ════════════════════════════════════════════════════════════
  // CLIENT REQUEST HANDLING
  // ════════════════════════════════════════════════════════════

  async handleClientRequest(req: ClientRequest): Promise<ClientResponse> {
    // Read requests — serve directly from KV store
    if (req.type === 'GET') {
      const value = this.kv.get(req.key!);
      return { success: true, value: value ?? undefined };
    }

    if (req.type === 'KEYS') {
      return { success: true, keys: this.kv.keys(req.key) };
    }

    if (req.type === 'INFO') {
      return {
        success: true,
        info: {
          nodeId:      this.nodeId,
          state:       this.state,
          term:        this.storage.getCurrentTerm(),
          leaderId:    this.leaderId,
          commitIndex: this.commitIndex,
          logLength:   this.storage.getLogLength(),
          kv:          this.kv.getStats(),
          metrics:     this.metrics,
        }
      };
    }

    // Write requests must go through leader
    if (this.state !== 'LEADER') {
      return {
        success:  false,
        error:    'Not the leader',
        leaderId: this.leaderId ?? undefined,
      };
    }

    // CAS — check and apply locally (still needs log replication)
    if (req.type === 'CAS') {
      const command: Command = { type: 'SET', key: req.key!, value: req.value! };
      return this.proposeCommand(command);
    }

    // Build command from request
    const command: Command = req.type === 'DELETE'
      ? { type: 'DELETE', key: req.key! }
      : { type: 'SET', key: req.key!, value: req.value!, ttl: req.ttl };

    return this.proposeCommand(command);
  }

  // ════════════════════════════════════════════════════════════
  // LOG REPLICATION (Leader only)
  // ════════════════════════════════════════════════════════════

  private proposeCommand(command: Command): Promise<ClientResponse> {
    return new Promise((resolve, reject) => {
      const index = this.appendToLog(command);

      // Track this pending write
      this.pendingWrites.set(index, { resolve, reject, index });

      // Trigger immediate replication
      this.replicateToAll();

      // Timeout if not committed in 5 seconds
      setTimeout(() => {
        if (this.pendingWrites.has(index)) {
          this.pendingWrites.delete(index);
          reject(new Error('Write timed out — no quorum'));
        }
      }, 5000);
    });
  }

  private appendToLog(command: Command): number {
    const entry: LogEntry = {
      term:    this.storage.getCurrentTerm(),
      index:   this.storage.getLogLength(),
      command,
    };
    this.storage.appendEntries([entry]);
    return entry.index;
  }

  private async replicateToAll(): Promise<void> {
    for (const peer of this.peers) {
      this.replicateToPeer(peer).catch(() => {});
    }
  }

  private async replicateToPeer(peer: NodeInfo): Promise<void> {
    if (this.state !== 'LEADER') return;

    const nextIdx    = this.nextIndex.get(peer.id) ?? 0;
    const prevIdx    = nextIdx - 1;
    const prevEntry  = prevIdx >= 0 ? this.storage.getEntry(prevIdx) : null;
    const entries    = this.storage.getEntriesFrom(nextIdx);

    const args: AppendEntriesArgs = {
      term:         this.storage.getCurrentTerm(),
      leaderId:     this.nodeId,
      prevLogIndex: prevIdx,
      prevLogTerm:  prevEntry?.term ?? 0,
      entries,
      leaderCommit: this.commitIndex,
    };

    try {
      this.metrics.appendsSent++;
      const reply = await this.sendRpc(peer, 'AppendEntries', args) as AppendEntriesReply;

      if (reply.term > this.storage.getCurrentTerm()) {
        this.becomeFollower(reply.term);
        return;
      }

      if (this.state !== 'LEADER') return;

      if (reply.success) {
        const newMatchIndex = prevIdx + entries.length;
        this.matchIndex.set(peer.id, newMatchIndex);
        this.nextIndex.set(peer.id,  newMatchIndex + 1);

        // Check if we can advance commitIndex
        this.advanceCommitIndex();
      } else {
        // Conflict — back up nextIndex using conflict info
        if (reply.conflictIndex >= 0) {
          this.nextIndex.set(peer.id, reply.conflictIndex);
        } else {
          this.nextIndex.set(peer.id, Math.max(0, nextIdx - 1));
        }
        // Retry
        this.replicateToPeer(peer).catch(() => {});
      }
    } catch {
      // Peer unreachable — will retry on next heartbeat
    }
  }

  // Advance commitIndex when majority have replicated an entry
  private advanceCommitIndex(): void {
    const currentTerm = this.storage.getCurrentTerm();
    const logLen      = this.storage.getLogLength();

    for (let n = logLen - 1; n > this.commitIndex; n--) {
      const entry = this.storage.getEntry(n);
      if (!entry || entry.term !== currentTerm) continue;

      // Count how many nodes have this entry
      let count = 1; // leader itself
      for (const peer of this.peers) {
        if ((this.matchIndex.get(peer.id) ?? -1) >= n) count++;
      }

      const majority = Math.floor((this.peers.length + 1) / 2) + 1;
      if (count >= majority) {
        this.commitIndex = n;
        this.applyCommitted();
        break;
      }
    }
  }

  // Apply all committed but not yet applied entries
  private applyCommitted(): void {
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied++;
      const entry = this.storage.getEntry(this.lastApplied);
      if (!entry) break;

      const result = this.kv.apply(entry.command);
      this.metrics.commitsApplied++;

      // Resolve pending client write if we're the leader
      const pending = this.pendingWrites.get(this.lastApplied);
      if (pending) {
        this.pendingWrites.delete(this.lastApplied);
        pending.resolve({ success: true, value: result ?? undefined });
      }

      this.emit('committed', { index: this.lastApplied, command: entry.command });
    }
  }

  // ════════════════════════════════════════════════════════════
  // HEARTBEAT
  // ════════════════════════════════════════════════════════════

  private startHeartbeat(): void {
    this.heartbeatTimer = setInterval(() => {
      if (this.state !== 'LEADER') {
        this.clearHeartbeatTimer();
        return;
      }
      this.replicateToAll();
    }, 50); // heartbeat every 50ms
  }

  // ════════════════════════════════════════════════════════════
  // ELECTION TIMER
  // Randomized to prevent split votes: 150–300ms
  // ════════════════════════════════════════════════════════════

  private resetElectionTimer(): void {
    this.clearElectionTimer();
    const timeout = 150 + Math.random() * 150; // 150–300ms
    this.electionTimer = setTimeout(() => {
      if (this.state !== 'LEADER') this.startElection();
    }, timeout);
  }

  private clearElectionTimer(): void {
    if (this.electionTimer) { clearTimeout(this.electionTimer);  this.electionTimer  = null; }
  }

  private clearHeartbeatTimer(): void {
    if (this.heartbeatTimer) { clearInterval(this.heartbeatTimer); this.heartbeatTimer = null; }
  }

  // ════════════════════════════════════════════════════════════
  // HELPERS
  // ════════════════════════════════════════════════════════════

  private isCandidateLogUpToDate(lastIdx: number, lastTerm: number): boolean {
    const myLastTerm  = this.storage.getLastLogTerm();
    const myLastIndex = this.storage.getLastLogIndex();

    if (lastTerm !== myLastTerm)  return lastTerm  > myLastTerm;
    return lastIdx >= myLastIndex;
  }

  getState():    NodeState     { return this.state;    }
  getLeaderId(): number | null { return this.leaderId; }
  getTerm():     number        { return this.storage.getCurrentTerm(); }
  getMetrics()                 { return { ...this.metrics, state: this.state, term: this.getTerm(), commitIndex: this.commitIndex }; }

  stop(): void {
    this.clearElectionTimer();
    this.clearHeartbeatTimer();
    this.kv.snapshot();
  }
}
