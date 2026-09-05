import { useEffect, useState } from "react";
import { api } from "../api/client";
import type { Topic } from "../types";

export default function TopicsPanel({ onSelectTopic }: { onSelectTopic: (name: string) => void }) {
  const [topics, setTopics] = useState<Topic[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [partitions, setPartitions] = useState(3);
  const [publishKey, setPublishKey] = useState("");
  const [publishValue, setPublishValue] = useState('{"example": true}');
  const [publishTarget, setPublishTarget] = useState<string | null>(null);
  const [publishStatus, setPublishStatus] = useState<string | null>(null);

  const refresh = () => {
    api.listTopics().then(setTopics).catch((e) => setError(String(e)));
  };

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 5000);
    return () => clearInterval(interval);
  }, []);

  const createTopic = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    try {
      await api.createTopic({ name, partitions, replication_factor: 1, retention_ms: 86400000 });
      setName("");
      refresh();
    } catch (e) {
      setError(String(e));
    }
  };

  const deleteTopic = async (topicName: string) => {
    try {
      await api.deleteTopic(topicName);
      refresh();
    } catch (e) {
      setError(String(e));
    }
  };

  const publish = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!publishTarget) return;
    setPublishStatus(null);
    try {
      const value = JSON.parse(publishValue);
      const result = await api.publishEvent(publishTarget, { key: publishKey, value });
      setPublishStatus(`published to partition ${result.partition}, offset ${result.offset}`);
    } catch (e) {
      setPublishStatus(`error: ${String(e)}`);
    }
  };

  return (
    <div className="panel">
      <h2>Topics</h2>
      {error && <p className="error">{error}</p>}

      <form onSubmit={createTopic} className="inline-form">
        <input placeholder="topic name" value={name} onChange={(e) => setName(e.target.value)} required />
        <input
          type="number"
          min={1}
          value={partitions}
          onChange={(e) => setPartitions(Number(e.target.value))}
          title="partitions"
        />
        <button type="submit">Create Topic</button>
      </form>

      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Partitions</th>
            <th>Replication</th>
            <th>Retention (ms)</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {topics.map((t) => (
            <tr key={t.id}>
              <td>
                <button className="link" onClick={() => onSelectTopic(t.name)}>
                  {t.name}
                </button>
              </td>
              <td>{t.partitions}</td>
              <td>{t.replication_factor}</td>
              <td>{t.retention_ms}</td>
              <td>
                <button onClick={() => setPublishTarget(t.name)}>Publish test event</button>
                <button className="danger" onClick={() => deleteTopic(t.name)}>
                  Delete
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {publishTarget && (
        <form onSubmit={publish} className="publish-form">
          <h3>Publish to {publishTarget}</h3>
          <input placeholder="key (optional)" value={publishKey} onChange={(e) => setPublishKey(e.target.value)} />
          <textarea value={publishValue} onChange={(e) => setPublishValue(e.target.value)} rows={3} />
          <button type="submit">Send</button>
          <button type="button" onClick={() => setPublishTarget(null)}>
            Cancel
          </button>
          {publishStatus && <p>{publishStatus}</p>}
        </form>
      )}
    </div>
  );
}
