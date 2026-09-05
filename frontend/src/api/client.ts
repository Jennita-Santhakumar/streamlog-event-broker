import type { ConsumerLagRow, DLQRow, Topic } from "../types";

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8060";

export function wsBaseURL(): string {
  return BASE_URL.replace(/^http/, "ws");
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  const res = await fetch(`${BASE_URL}${path}`, { headers, ...init });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${body}`);
  }
  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}

export const api = {
  listTopics: () => request<Topic[]>("/api/v1/topics/"),
  createTopic: (payload: { name: string; partitions: number; replication_factor: number; retention_ms: number }) =>
    request<Topic>("/api/v1/topics/", { method: "POST", body: JSON.stringify(payload) }),
  deleteTopic: (name: string) => request<void>(`/api/v1/topics/${name}`, { method: "DELETE" }),
  publishEvent: (topic: string, payload: { key: string; value: unknown; headers?: Record<string, string> }) =>
    request<{ topic: string; partition: number; offset: number; timestamp: number }>(`/api/v1/events/${topic}`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  consumerLag: () => request<ConsumerLagRow[]>("/api/v1/lag"),
  listDLQ: () => request<DLQRow[]>("/api/v1/dlq"),
};
