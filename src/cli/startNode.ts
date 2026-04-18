import { KVServer }       from '../server/KVServer';
import { getNodeConfig }  from '../server/clusterConfig';
import { logger }         from '../utils/logger';

const nodeId = parseInt(process.argv[2] ?? '1');

if (![1, 2, 3].includes(nodeId)) {
  console.error('Usage: npm run node1 | node2 | node3');
  process.exit(1);
}

const config = getNodeConfig(nodeId);
const server = new KVServer(config);

server.start().then(() => {
  logger.info('════════════════════════════════════════');
  logger.info(`  Raft KV Node ${nodeId} is running!`);
  logger.info('════════════════════════════════════════');
  logger.info(`  RPC port: ${config.rpcPort}`);
  logger.info(`  KV  port: ${config.kvPort}`);
  logger.info(`  Peers:    ${config.peers.map(p => p.id).join(', ')}`);
  logger.info('════════════════════════════════════════');
  logger.info('  Waiting for leader election...');
}).catch(err => {
  logger.error('Failed to start node', { error: err.message });
  process.exit(1);
});

process.on('SIGINT',  () => { server.stop(); process.exit(0); });
process.on('SIGTERM', () => { server.stop(); process.exit(0); });
