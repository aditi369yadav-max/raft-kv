// ============================================================
// KV Server — Ties Everything Together
//
// Wires up:
//   - RaftNode (consensus)
//   - KVStateMachine (storage)
//   - RpcTransport (inter-node communication)
//   - Client TCP server (handles client requests)
// ============================================================

import net          from 'net';
import path         from 'path';
import { v4 as uuid } from 'uuid';
import { RaftNode }      from '../raft/RaftNode';
import { RaftStorage }   from '../raft/RaftStorage';
import { KVStateMachine } from '../storage/KVStateMachine';
import { RpcTransport }  from './RpcTransport';
import { NodeInfo, ClientRequest, ClientResponse } from '../raft/types';
import { logger } from '../utils/logger';

export interface ServerConfig {
  nodeId:   number;
  host:     string;
  rpcPort:  number;  // inter-node communication
  kvPort:   number;  // client communication
  peers:    NodeInfo[];
  dataDir:  string;
}

export class KVServer {
  private raft:      RaftNode;
  private transport: RpcTransport;
  private storage:   RaftStorage;
  private kv:        KVStateMachine;
  private server:    net.Server;

  constructor(private config: ServerConfig) {
    const dataDir = path.join(config.dataDir, `node-${config.nodeId}`);

    this.storage   = new RaftStorage(config.dataDir, config.nodeId);
    this.kv        = new KVStateMachine(config.dataDir, config.nodeId);
    this.transport = new RpcTransport(config.nodeId);

    // Wire up Raft with transport
    this.raft = new RaftNode(
      config.nodeId,
      config.peers,
      this.storage,
      this.kv,
      (to, type, data) => this.transport.send(to, type, data)
    );

    // Register RPC handlers
    this.transport.on('RequestVote',   (data) => Promise.resolve(this.raft.handleRequestVote(data as any)));
    this.transport.on('AppendEntries', (data) => Promise.resolve(this.raft.handleAppendEntries(data as any)));

    // Client TCP server
    this.server = net.createServer(socket => this.handleClient(socket));

    // Log state changes
    this.raft.on('stateChange', ({ state, term }) => {
      logger.info(`Node ${config.nodeId} → ${state} (term ${term})`);
    });

    // Periodic snapshot
    setInterval(() => this.kv.snapshot(), 30_000);
  }

  async start(): Promise<void> {
    // Start RPC server (for other nodes)
    await this.transport.listen(this.config.rpcPort, this.config.host);

    // Start client server
    await new Promise<void>(resolve => {
      this.server.listen(this.config.kvPort, this.config.host, () => {
        logger.info(`KV Node ${this.config.nodeId} ready`, {
          rpcPort: this.config.rpcPort,
          kvPort:  this.config.kvPort,
        });
        resolve();
      });
    });
  }

  // ── Client Handler ────────────────────────────────────────

  private handleClient(socket: net.Socket): void {
    let buffer = '';

    socket.on('data', (data) => {
      buffer += data.toString();
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';

      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const req = JSON.parse(line) as ClientRequest;
          this.raft.handleClientRequest(req)
            .then(res => socket.write(JSON.stringify(res) + '\n'))
            .catch(err => socket.write(JSON.stringify({ success: false, error: err.message }) + '\n'));
        } catch {
          socket.write(JSON.stringify({ success: false, error: 'Invalid request' }) + '\n');
        }
      }
    });

    socket.on('error', () => {});
  }

  stop(): void {
    this.raft.stop();
    this.server.close();
  }
}
