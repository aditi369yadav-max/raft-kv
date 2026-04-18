import net          from 'net';
import { v4 as uuid } from 'uuid';
import { KV_PORTS }  from '../src/server/clusterConfig';

const OPS     = 1000;
const KV_PORT = KV_PORTS[1]; // connect to node 1

const send = (socket: net.Socket, req: object): Promise<any> =>
  new Promise(resolve => {
    const onData = (data: Buffer) => {
      socket.removeListener('data', onData);
      try { resolve(JSON.parse(data.toString().split('\n')[0])); } catch { resolve({}); }
    };
    socket.on('data', onData);
    socket.write(JSON.stringify({ id: uuid(), ...req }) + '\n');
  });

const bench = async () => {
  console.log('\n════════════════════════════════════════');
  console.log('       RAFT KV — BENCHMARK');
  console.log('════════════════════════════════════════');
  console.log(`Operations: ${OPS.toLocaleString()} writes + ${OPS.toLocaleString()} reads`);
  console.log('════════════════════════════════════════\n');

  const socket = net.createConnection({ host: '127.0.0.1', port: KV_PORT });
  await new Promise(r => socket.on('connect', r));
  console.log('Connected to cluster\n');

  // ── Write Benchmark ───────────────────────────────────────
  console.log('📝 Running write benchmark...');
  const writeStart = Date.now();
  for (let i = 0; i < OPS; i++) {
    const res = await send(socket, { type: 'SET', key: `key:${i}`, value: `value:${i}` });
    if (!res.success && i === 0) {
      console.log('Not leader — connect to the leader node');
      process.exit(1);
    }
  }
  const writeTime       = Date.now() - writeStart;
  const writeThroughput = Math.round(OPS / (writeTime / 1000));

  console.log(`\n✅ Write Results:`);
  console.log(`   Operations: ${OPS.toLocaleString()}`);
  console.log(`   Time:       ${writeTime}ms`);
  console.log(`   Throughput: ${writeThroughput.toLocaleString()} ops/sec`);
  console.log(`   Latency:    ${(writeTime / OPS).toFixed(2)}ms avg`);

  // ── Read Benchmark ────────────────────────────────────────
  console.log('\n📖 Running read benchmark...');
  const readStart = Date.now();
  for (let i = 0; i < OPS; i++) {
    await send(socket, { type: 'GET', key: `key:${i % OPS}` });
  }
  const readTime       = Date.now() - readStart;
  const readThroughput = Math.round(OPS / (readTime / 1000));

  console.log(`\n✅ Read Results:`);
  console.log(`   Operations: ${OPS.toLocaleString()}`);
  console.log(`   Time:       ${readTime}ms`);
  console.log(`   Throughput: ${readThroughput.toLocaleString()} ops/sec`);

  console.log('\n════════════════════════════════════════');
  console.log('              SUMMARY');
  console.log('════════════════════════════════════════');
  console.log(`Write: ${writeThroughput.toLocaleString()} ops/sec (consensus replicated)`);
  console.log(`Read:  ${readThroughput.toLocaleString()} ops/sec`);
  console.log('════════════════════════════════════════\n');

  socket.destroy();
  process.exit(0);
};

bench().catch(e => { console.error(e.message); process.exit(1); });
