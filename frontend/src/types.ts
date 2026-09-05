export interface Topic {
  id: string;
  name: string;
  partitions: number;
  replication_factor: number;
  retention_ms: number;
  created_at: string;
}

export interface ConsumerLagRow {
  group_name: string;
  topic_name: string;
  partition: number;
  committed_offset: number;
  latest_offset: number;
  lag: number;
}

export interface DLQRow {
  id: string;
  original_topic: string;
  original_partition: number;
  original_offset: number;
  reason: string;
  failed_at: string;
}

export interface TailEvent {
  topic: string;
  partition: number;
  offset: number;
  key: string;
  value: unknown;
  headers?: Record<string, string>;
  timestamp: number;
}
