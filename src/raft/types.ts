// ============================================================
// Raft Consensus — Core Types
//
// Raft is a consensus algorithm designed to be understandable.
// It's used by etcd (Kubernetes), CockroachDB, TiKV, Consul.
//
// Core idea: elect a leader, all writes go through leader,
// leader replicates to followers, commit when majority confirms.
//
// A cluster of 3 nodes can survive 1 failure.
// A cluster of 5 nodes can survive 2 failures.
// Formula: need (n/2 + 1) nodes alive — called "quorum"
// ============================================================

// ── Node States ───────────────────────────────────────────────
// Every Raft node is in exactly one of these states at any time

export type NodeState =
  | 'FOLLOWER'    // Default state. Waits for heartbeats from leader.
  | 'CANDIDATE'   // Started an election. Asking others to vote for it.
  | 'LEADER';     // Won election. Handles all client writes.

// ── Log Entry ─────────────────────────────────────────────────
// The Raft log is a sequence of commands.
// Once committed, these commands are NEVER changed.
// This is what makes distributed systems "consistent."

export interface LogEntry {
  readonly term:    number;   // which leader created this entry
  readonly index:   number;   // position in the log
  readonly command: Command;  // what to do (SET/DELETE)
}

// ── Commands ──────────────────────────────────────────────────
// The operations our KV store supports

export type Command =
  | { readonly type: 'SET';    readonly key: string; readonly value: string; readonly ttl?: number }
  | { readonly type: 'DELETE'; readonly key: string }
  | { readonly type: 'NOOP'  };  // used by leader on election

// ── RPC Messages ──────────────────────────────────────────────
// The 2 RPCs that Raft uses. That's it. Just 2.

// 1. RequestVote: sent by CANDIDATE to all other nodes
export interface RequestVoteArgs {
  readonly term:         number;  // candidate's current term
  readonly candidateId:  number;  // who is asking for votes
  readonly lastLogIndex: number;  // how up-to-date is candidate's log
  readonly lastLogTerm:  number;  // term of candidate's last log entry
}

export interface RequestVoteReply {
  readonly term:        number;   // respondent's current term
  readonly voteGranted: boolean;  // did they vote for the candidate?
}

// 2. AppendEntries: sent by LEADER to all followers
//    Used for BOTH log replication AND heartbeats (empty entries)
export interface AppendEntriesArgs {
  readonly term:         number;      // leader's current term
  readonly leaderId:     number;      // so followers can redirect clients
  readonly prevLogIndex: number;      // index before new entries
  readonly prevLogTerm:  number;      // term of prevLogIndex entry
  readonly entries:      LogEntry[];  // empty for heartbeat
  readonly leaderCommit: number;      // leader's commit index
}

export interface AppendEntriesReply {
  readonly term:          number;   // respondent's current term
  readonly success:       boolean;  // true if follower accepted entries
  readonly conflictIndex: number;   // optimization: skip back faster
  readonly conflictTerm:  number;
}

// ── Node Info ─────────────────────────────────────────────────
export interface NodeInfo {
  readonly id:   number;
  readonly host: string;
  readonly port: number;
}

// ── Client RPC ────────────────────────────────────────────────
// What clients send to the cluster

export type ClientRequestType = 'GET' | 'SET' | 'DELETE' | 'CAS' | 'KEYS' | 'INFO';

export interface ClientRequest {
  readonly id:      string;             // for deduplication
  readonly type:    ClientRequestType;
  readonly key?:    string;
  readonly value?:  string;
  readonly ttl?:    number;             // time-to-live in seconds
  readonly expected?: string;           // for CAS (compare-and-swap)
}

export interface ClientResponse {
  readonly success:   boolean;
  readonly value?:    string;
  readonly error?:    string;
  readonly leaderId?: number;           // redirect if not leader
  readonly keys?:     string[];
  readonly info?:     unknown;
}

// ── Persistent State ──────────────────────────────────────────
// These MUST survive crashes. Written to disk before any RPC reply.

export interface PersistentState {
  currentTerm: number;      // latest term node has seen
  votedFor:    number | null; // candidateId voted for in current term
  log:         LogEntry[];  // the actual log entries
}
