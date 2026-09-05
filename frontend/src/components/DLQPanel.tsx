import { useEffect, useState } from "react";
import { api } from "../api/client";
import type { DLQRow } from "../types";

export default function DLQPanel() {
  const [rows, setRows] = useState<DLQRow[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const refresh = () => api.listDLQ().then(setRows).catch((e) => setError(String(e)));
    refresh();
    const interval = setInterval(refresh, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="panel">
      <h2>Dead Letter Queue</h2>
      {error && <p className="error">{error}</p>}
      {rows.length === 0 ? (
        <p>No messages have been routed to the DLQ.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Topic</th>
              <th>Partition</th>
              <th>Offset</th>
              <th>Reason</th>
              <th>Failed At</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.id}>
                <td>{r.original_topic}</td>
                <td>{r.original_partition}</td>
                <td>{r.original_offset}</td>
                <td>{r.reason}</td>
                <td>{new Date(r.failed_at).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
