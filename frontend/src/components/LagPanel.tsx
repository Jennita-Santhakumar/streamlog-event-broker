import { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from "recharts";
import { api } from "../api/client";
import type { ConsumerLagRow } from "../types";

export default function LagPanel() {
  const [rows, setRows] = useState<ConsumerLagRow[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const refresh = () => api.consumerLag().then(setRows).catch((e) => setError(String(e)));
    refresh();
    const interval = setInterval(refresh, 4000);
    return () => clearInterval(interval);
  }, []);

  const chartData = rows.map((r) => ({
    name: `${r.group_name}/${r.topic_name}/p${r.partition}`,
    lag: r.lag,
  }));

  return (
    <div className="panel">
      <h2>Consumer Lag</h2>
      {error && <p className="error">{error}</p>}
      {rows.length === 0 ? (
        <p>No consumer groups have committed offsets yet.</p>
      ) : (
        <>
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="name" tick={{ fontSize: 11 }} interval={0} angle={-20} textAnchor="end" height={70} />
              <YAxis />
              <Tooltip />
              <Bar dataKey="lag" fill="#4f8ef7" />
            </BarChart>
          </ResponsiveContainer>

          <table>
            <thead>
              <tr>
                <th>Group</th>
                <th>Topic</th>
                <th>Partition</th>
                <th>Committed</th>
                <th>Latest</th>
                <th>Lag</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={i}>
                  <td>{r.group_name}</td>
                  <td>{r.topic_name}</td>
                  <td>{r.partition}</td>
                  <td>{r.committed_offset}</td>
                  <td>{r.latest_offset}</td>
                  <td>{r.lag}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}
