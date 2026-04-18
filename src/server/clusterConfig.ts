import { NodeInfo } from '../raft/types';

// 3-node cluster configuration
// Node 1: RPC=7001, KV=8001
// Node 2: RPC=7002, KV=8002
// Node 3: RPC=7003, KV=8003

export const CLUSTER_NODES: NodeInfo[] = [
  { id: 1, host: '127.0.0.1', port: 7001 },
  { id: 2, host: '127.0.0.1', port: 7002 },
  { id: 3, host: '127.0.0.1', port: 7003 },
];

export const KV_PORTS: Record<number, number> = {
  1: 8001,
  2: 8002,
  3: 8003,
};

export const DATA_DIR = process.env.DATA_DIR || './data';

export const getNodeConfig = (nodeId: number) => ({
  nodeId,
  host:    '127.0.0.1',
  rpcPort: CLUSTER_NODES.find(n => n.id === nodeId)!.port,
  kvPort:  KV_PORTS[nodeId],
  peers:   CLUSTER_NODES.filter(n => n.id !== nodeId),
  dataDir: DATA_DIR,
});
