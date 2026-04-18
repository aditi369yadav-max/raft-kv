// ============================================================
// KV Client CLI
// Connects to any node in the cluster.
// If connected node is not leader, it will tell us who is.
// ============================================================

import net          from 'net';
import readline     from 'readline';
import { v4 as uuid } from 'uuid';
import { KV_PORTS }  from '../server/clusterConfig';
import { logger }    from '../utils/logger';

class KVClient {
  private socket:    net.Socket | null = null;
  private buffer:    string = '';
  private pending:   Map<string, (r: any) => void> = new Map();
  private nodePort:  number;

  constructor(nodeId: number = 1) {
    this.nodePort = KV_PORTS[nodeId];
  }

  async connect(port = this.nodePort): Promise<void> {
    return new Promise((resolve, reject) => {
      this.socket = net.createConnection({ host: '127.0.0.1', port }, () => resolve());
      this.socket.on('data', data => {
        this.buffer += data.toString();
        const lines  = this.buffer.split('\n');
        this.buffer  = lines.pop() ?? '';
        for (const line of lines) {
          if (!line.trim()) continue;
          try {
            const res = JSON.parse(line);
            const cb  = this.pending.get('next');
            if (cb) { this.pending.delete('next'); cb(res); }
          } catch {}
        }
      });
      this.socket.on('error', reject);
    });
  }

  async send(req: object): Promise<any> {
    return new Promise((resolve) => {
      this.pending.set('next', resolve);
      this.socket!.write(JSON.stringify({ id: uuid(), ...req }) + '\n');
    });
  }

  close() { this.socket?.destroy(); }
}

const run = async () => {
  const nodeId = parseInt(process.argv[2] ?? '1');
  const client = new KVClient(nodeId);

  try {
    await client.connect();
    console.log(`\nConnected to KV node ${nodeId}`);
    console.log('Commands: SET key value [ttl], GET key, DELETE key, KEYS [pattern], INFO\n');
  } catch {
    console.error(`Cannot connect to node ${nodeId}. Is it running?`);
    process.exit(1);
  }

  const rl = readline.createInterface({ input: process.stdin, output: process.stdout, prompt: 'raft-kv> ' });
  rl.prompt();

  rl.on('line', async (line) => {
    const parts = line.trim().split(/\s+/);
    const cmd   = parts[0]?.toUpperCase();

    if (!cmd) { rl.prompt(); return; }

    let req: object;
    switch (cmd) {
      case 'SET':    req = { type: 'SET',    key: parts[1], value: parts[2], ttl: parts[3] ? parseInt(parts[3]) : undefined }; break;
      case 'GET':    req = { type: 'GET',    key: parts[1] }; break;
      case 'DELETE': req = { type: 'DELETE', key: parts[1] }; break;
      case 'KEYS':   req = { type: 'KEYS',   key: parts[1] }; break;
      case 'INFO':   req = { type: 'INFO' };                   break;
      case 'EXIT':   client.close(); process.exit(0);
      default:
        console.log('Unknown command. Try: SET key value, GET key, DELETE key, KEYS, INFO');
        rl.prompt(); return;
    }

    try {
      const res = await client.send(req);
      if (res.success) {
        if (res.value   !== undefined) console.log(`→ ${res.value}`);
        else if (res.keys)             console.log(`→ [${res.keys.join(', ')}]`);
        else if (res.info)             console.log(JSON.stringify(res.info, null, 2));
        else                           console.log('→ OK');
      } else {
        console.log(`✗ Error: ${res.error}${res.leaderId ? ` (leader is node ${res.leaderId})` : ''}`);
      }
    } catch (e: any) {
      console.log(`✗ ${e.message}`);
    }

    rl.prompt();
  });
};

run().catch(console.error);
