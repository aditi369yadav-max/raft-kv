// ============================================================
// RPC Transport — TCP-based communication between nodes
//
// Raft nodes need to talk to each other.
// We use TCP with newline-delimited JSON (same as Mini Kafka).
//
// Each RPC has a correlationId for matching responses.
// Timeouts prevent blocked RPCs from stalling the cluster.
// ============================================================

import net          from 'net';
import { v4 as uuid } from 'uuid';
import { NodeInfo } from '../raft/types';
import { logger }   from '../utils/logger';

interface PendingRpc {
  resolve: (data: unknown) => void;
  reject:  (err: Error)    => void;
  timeout: NodeJS.Timeout;
}

export class RpcTransport {
  private connections = new Map<number, net.Socket>();
  private pending     = new Map<string, PendingRpc>();
  private buffers     = new Map<number, string>();
  private handlers    = new Map<string, (data: unknown) => Promise<unknown>>();

  constructor(private nodeId: number) {}

  // ── Register Handler ──────────────────────────────────────
  // Called when we receive an RPC of this type

  on(type: string, handler: (data: unknown) => Promise<unknown>): void {
    this.handlers.set(type, handler);
  }

  // ── Send RPC ──────────────────────────────────────────────

  async send(to: NodeInfo, type: string, data: unknown): Promise<unknown> {
    const socket = await this.getConnection(to);
    const correlationId = uuid();

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(correlationId);
        reject(new Error(`RPC timeout: ${type} to node ${to.id}`));
      }, 200); // 200ms RPC timeout

      this.pending.set(correlationId, { resolve, reject, timeout });

      const msg = JSON.stringify({ correlationId, type, data, from: this.nodeId }) + '\n';
      socket.write(msg, (err) => {
        if (err) {
          clearTimeout(timeout);
          this.pending.delete(correlationId);
          this.connections.delete(to.id);
          reject(err);
        }
      });
    });
  }

  // ── Handle Incoming Message ───────────────────────────────

  private async handleMessage(raw: string, socket: net.Socket): Promise<void> {
    let msg: any;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.reply) {
      // This is a reply to our request
      const pending = this.pending.get(msg.correlationId);
      if (pending) {
        clearTimeout(pending.timeout);
        this.pending.delete(msg.correlationId);
        if (msg.error) pending.reject(new Error(msg.error));
        else           pending.resolve(msg.data);
      }
      return;
    }

    // This is a request — handle it and reply
    const handler = this.handlers.get(msg.type);
    if (!handler) {
      socket.write(JSON.stringify({ correlationId: msg.correlationId, reply: true, error: `Unknown RPC: ${msg.type}` }) + '\n');
      return;
    }

    try {
      const result = await handler(msg.data);
      socket.write(JSON.stringify({ correlationId: msg.correlationId, reply: true, data: result }) + '\n');
    } catch (e: any) {
      socket.write(JSON.stringify({ correlationId: msg.correlationId, reply: true, error: e.message }) + '\n');
    }
  }

  // ── Connection Management ─────────────────────────────────

  private async getConnection(to: NodeInfo): Promise<net.Socket> {
    const existing = this.connections.get(to.id);
    if (existing && !existing.destroyed) return existing;

    return new Promise((resolve, reject) => {
      const socket = net.createConnection({ host: to.host, port: to.port }, () => {
        this.connections.set(to.id, socket);
        this.buffers.set(to.id, '');
        resolve(socket);
      });

      socket.on('data', (data) => {
        const peerId = to.id;
        const buf    = (this.buffers.get(peerId) ?? '') + data.toString();
        const lines  = buf.split('\n');
        this.buffers.set(peerId, lines.pop() ?? '');
        for (const line of lines) {
          if (line.trim()) this.handleMessage(line, socket).catch(() => {});
        }
      });

      socket.on('close', () => {
        this.connections.delete(to.id);
        this.buffers.delete(to.id);
      });

      socket.on('error', (err) => {
        this.connections.delete(to.id);
        reject(err);
      });

      socket.setTimeout(100);
    });
  }

  // ── Server (receive incoming RPCs from other nodes) ───────

  listen(port: number, host: string): Promise<void> {
    return new Promise((resolve) => {
      const server = net.createServer(socket => {
        let buffer = '';
        socket.on('data', (data) => {
          buffer += data.toString();
          const lines = buffer.split('\n');
          buffer = lines.pop() ?? '';
          for (const line of lines) {
            if (line.trim()) this.handleMessage(line, socket).catch(() => {});
          }
        });
        socket.on('error', () => {});
      });

      server.listen(port, host, () => {
        logger.debug(`Node ${this.nodeId} RPC server listening on ${host}:${port}`);
        resolve();
      });
    });
  }
}
