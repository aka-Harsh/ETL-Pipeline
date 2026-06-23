import React, { useState, useCallback } from 'react';
import axios from 'axios';

const TAB_PG = 0, TAB_S3 = 1, TAB_KAFKA = 2;
const TABS = [
  { label: '🐘 Postgres', id: TAB_PG },
  { label: '🪣 S3 Files', id: TAB_S3 },
  { label: '📨 Kafka Topics', id: TAB_KAFKA },
];

function fmt(bytes) { return bytes > 1024 ? `${(bytes / 1024).toFixed(1)} KB` : `${bytes} B`; }

// ── Shared error banner ──────────────────────────────────────────────────────
function ErrorBar({ msg }) {
  if (!msg) return null;
  return <div style={{ padding: '7px 20px', background: 'rgba(239,68,68,0.12)', color: '#ef4444', fontSize: 12, flexShrink: 0 }}>{msg}</div>;
}

// ── Empty state helper ───────────────────────────────────────────────────────
function Empty({ icon, children }) {
  return (
    <div style={{ color: 'var(--color-text-muted)', fontSize: 13, marginTop: 50, textAlign: 'center' }}>
      <div style={{ fontSize: 36, marginBottom: 12 }}>{icon}</div>
      {children}
    </div>
  );
}

// ── Data grid (reused by Postgres and Kafka tabs) ────────────────────────────
function DataGrid({ columns, rows }) {
  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={{ borderCollapse: 'collapse', fontSize: 12, width: '100%' }}>
        <thead>
          <tr>
            {columns.map(col => (
              <th key={col} style={{
                padding: '6px 12px', background: 'var(--color-bg)',
                borderBottom: '2px solid var(--color-border)',
                textAlign: 'left', fontWeight: 600, whiteSpace: 'nowrap',
                color: 'var(--color-text-muted)', position: 'sticky', top: 0,
              }}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} style={{ background: i % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.02)' }}>
              {columns.map(col => (
                <td key={col} style={{
                  padding: '5px 12px', borderBottom: '1px solid var(--color-border)',
                  whiteSpace: 'nowrap', maxWidth: 320, overflow: 'hidden', textOverflow: 'ellipsis',
                }}>{String(row[col] ?? '')}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ══════════════════════════════════════════════════════════════════════════════
// POSTGRES TAB
// ══════════════════════════════════════════════════════════════════════════════
function PostgresTab() {
  const [tables, setTables] = useState([]);
  const [selected, setSelected] = useState(null);
  const [preview, setPreview] = useState(null);
  const [loading, setLoading] = useState(false);
  const [listLoading, setListLoading] = useState(false);
  const [dropping, setDropping] = useState(null);
  const [error, setError] = useState(null);

  const fetchTables = useCallback(async () => {
    setListLoading(true); setError(null);
    try { const r = await axios.get('/api/query/tables'); setTables(r.data); }
    catch { setError('Could not load tables'); }
    finally { setListLoading(false); }
  }, []);

  React.useEffect(() => { fetchTables(); }, [fetchTables]);

  const loadTable = async (table) => {
    setSelected(table); setLoading(true); setError(null); setPreview(null);
    try { const r = await axios.get('/api/query/preview', { params: { table } }); setPreview(r.data); }
    catch (e) { setError(e.response?.data?.error || 'Query failed'); }
    finally { setLoading(false); }
  };

  const dropTable = async (table) => {
    if (!window.confirm(`Drop table "${table}"? All data will be permanently deleted.`)) return;
    setDropping(table);
    try {
      await axios.delete('/api/query/table', { params: { table } });
      if (selected === table) { setSelected(null); setPreview(null); }
      await fetchTables();
    } catch (e) { setError(e.response?.data?.error || 'Drop failed'); }
    finally { setDropping(null); }
  };

  return (
    <>
      <ErrorBar msg={error} />
      <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        {/* Sidebar */}
        <div style={{ width: 210, borderRight: '1px solid var(--color-border)', padding: 12, overflowY: 'auto', flexShrink: 0 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
            <span style={{ fontSize: 11, color: 'var(--color-text-muted)', textTransform: 'uppercase', letterSpacing: 1 }}>Tables ({tables.length})</span>
            <button className="btn btn-ghost btn-sm" onClick={fetchTables} style={{ fontSize: 12 }}>{listLoading ? '…' : '🔄'}</button>
          </div>
          {tables.length === 0 && !listLoading && (
            <div style={{ fontSize: 12, color: 'var(--color-text-muted)', lineHeight: 1.7 }}>
              No output tables yet.<br />Run a pipeline with a <strong>PostgreSQL Sink</strong> to create one.
            </div>
          )}
          {tables.map(t => (
            <div key={t} style={{ display: 'flex', alignItems: 'center', gap: 4, marginBottom: 3 }}>
              <button className="btn btn-ghost"
                style={{ flex: 1, textAlign: 'left', padding: '6px 10px', fontSize: 13, borderRadius: 6, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', background: selected === t ? 'var(--color-primary)' : 'transparent', color: selected === t ? '#fff' : 'var(--color-text)' }}
                onClick={() => loadTable(t)} title={t}>{t}
              </button>
              <button className="btn btn-ghost btn-sm"
                style={{ color: '#ef4444', padding: '4px 6px', fontSize: 12, flexShrink: 0 }}
                onClick={() => dropTable(t)} disabled={dropping === t} title="Drop table">
                {dropping === t ? '…' : '🗑'}
              </button>
            </div>
          ))}
        </div>

        {/* Grid */}
        <div style={{ flex: 1, overflow: 'auto', padding: 16 }}>
          {!selected && <Empty icon="👈">Click a table on the left to preview its rows</Empty>}
          {loading && <div style={{ color: 'var(--color-text-muted)', fontSize: 13, marginTop: 40, textAlign: 'center' }}>Loading…</div>}
          {preview && !loading && (
            <>
              <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 12 }}>
                <span style={{ fontWeight: 600, fontSize: 14 }}>{preview.table}</span>
                <span style={{ fontSize: 12, color: 'var(--color-text-muted)' }}>{preview.count} rows</span>
                <button className="btn btn-ghost btn-sm" onClick={() => loadTable(selected)}>🔄 Reload</button>
              </div>
              <DataGrid columns={preview.columns} rows={preview.rows} />
            </>
          )}
        </div>
      </div>
    </>
  );
}

// ══════════════════════════════════════════════════════════════════════════════
// S3 TAB
// ══════════════════════════════════════════════════════════════════════════════
function S3Tab() {
  const [bucket, setBucket] = useState('flowforge-output');
  const [prefix, setPrefix] = useState('');
  const [files, setFiles] = useState(null);
  const [listLoading, setListLoading] = useState(false);
  const [selectedFile, setSelectedFile] = useState(null);
  const [content, setContent] = useState(null);
  const [contentLoading, setContentLoading] = useState(false);
  const [deleting, setDeleting] = useState(null);
  const [error, setError] = useState(null);

  const listFiles = async () => {
    setListLoading(true); setError(null); setFiles(null); setSelectedFile(null); setContent(null);
    try { const r = await axios.get('/api/query/s3', { params: { bucket, prefix } }); setFiles(r.data); }
    catch (e) { setError(e.response?.data?.error || 'List failed'); }
    finally { setListLoading(false); }
  };

  const loadContent = async (key) => {
    setSelectedFile(key); setContent(null); setContentLoading(true); setError(null);
    try { const r = await axios.get('/api/query/s3/content', { params: { bucket, key } }); setContent(r.data.content); }
    catch (e) { setError(e.response?.data?.error || 'Read failed'); }
    finally { setContentLoading(false); }
  };

  const deleteFile = async (key) => {
    if (!window.confirm(`Delete s3://${bucket}/${key}?`)) return;
    setDeleting(key);
    try {
      await axios.delete('/api/query/s3/object', { params: { bucket, key } });
      if (selectedFile === key) { setSelectedFile(null); setContent(null); }
      await listFiles();
    } catch (e) { setError(e.response?.data?.error || 'Delete failed'); }
    finally { setDeleting(null); }
  };

  return (
    <>
      <ErrorBar msg={error} />
      {/* Controls */}
      <div style={{ display: 'flex', gap: 10, alignItems: 'flex-end', padding: '12px 16px', borderBottom: '1px solid var(--color-border)', flexShrink: 0 }}>
        <div>
          <div style={{ fontSize: 11, color: 'var(--color-text-muted)', marginBottom: 4 }}>Bucket</div>
          <select className="navbar-select" value={bucket} onChange={e => setBucket(e.target.value)}>
            <option value="flowforge-data">flowforge-data (inputs)</option>
            <option value="flowforge-output">flowforge-output (results)</option>
          </select>
        </div>
        <div>
          <div style={{ fontSize: 11, color: 'var(--color-text-muted)', marginBottom: 4 }}>Prefix filter</div>
          <input className="navbar-input" value={prefix} onChange={e => setPrefix(e.target.value)} placeholder="e.g. exports/" style={{ width: 180 }} />
        </div>
        <button className="btn btn-primary" onClick={listFiles} disabled={listLoading}>
          {listLoading ? 'Loading…' : '🔍 List Files'}
        </button>
      </div>

      <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        {/* File list */}
        <div style={{ width: 340, borderRight: '1px solid var(--color-border)', overflowY: 'auto', flexShrink: 0 }}>
          {!files && !listLoading && (
            <Empty icon="🪣">
              Select a bucket and click <strong>List Files</strong><br /><br />
              <span style={{ fontSize: 12 }}>
                <strong>flowforge-output</strong> = S3 Sink results<br />
                <strong>flowforge-data</strong> = your input files
              </span>
            </Empty>
          )}
          {files && files.files.length === 0 && (
            <Empty icon="📭">No files found in this bucket/prefix</Empty>
          )}
          {files && files.files.map((f, i) => (
            <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '8px 12px', borderBottom: '1px solid var(--color-border)', background: selectedFile === f.key ? 'rgba(99,102,241,0.1)' : 'transparent' }}>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontSize: 11, fontFamily: 'monospace', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: selectedFile === f.key ? 'var(--color-primary)' : 'var(--color-text)', cursor: 'pointer' }}
                  onClick={() => loadContent(f.key)} title={f.key}>{f.key}</div>
                <div style={{ fontSize: 10, color: 'var(--color-text-muted)', marginTop: 2 }}>{fmt(f.size)} · {f.lastModified.slice(0, 19).replace('T', ' ')}</div>
              </div>
              <button className="btn btn-ghost btn-sm" style={{ color: '#ef4444', padding: '3px 6px', fontSize: 12, flexShrink: 0 }}
                onClick={() => deleteFile(f.key)} disabled={deleting === f.key} title="Delete file">
                {deleting === f.key ? '…' : '🗑'}
              </button>
            </div>
          ))}
        </div>

        {/* Content pane */}
        <div style={{ flex: 1, overflow: 'auto', padding: 16 }}>
          {!selectedFile && <Empty icon="👈">Click a file to view its contents</Empty>}
          {contentLoading && <div style={{ color: 'var(--color-text-muted)', fontSize: 13 }}>Loading content…</div>}
          {content !== null && !contentLoading && (
            <>
              <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 10, fontFamily: 'monospace', color: 'var(--color-text-muted)' }}>
                s3://{bucket}/{selectedFile}
              </div>
              <pre style={{ fontSize: 11, lineHeight: 1.6, background: 'var(--color-bg)', padding: 14, borderRadius: 8, overflow: 'auto', border: '1px solid var(--color-border)', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                {(() => {
                  try { return JSON.stringify(JSON.parse(content), null, 2); }
                  catch { return content; }
                })()}
              </pre>
            </>
          )}
        </div>
      </div>
    </>
  );
}

// ══════════════════════════════════════════════════════════════════════════════
// KAFKA TAB
// ══════════════════════════════════════════════════════════════════════════════
function KafkaTab() {
  const [topics, setTopics] = useState([]);
  const [topicsLoading, setTopicsLoading] = useState(false);
  const [selected, setSelected] = useState(null);
  const [messages, setMessages] = useState(null);
  const [msgLoading, setMsgLoading] = useState(false);
  const [deleting, setDeleting] = useState(null);
  const [error, setError] = useState(null);

  const fetchTopics = useCallback(async () => {
    setTopicsLoading(true); setError(null);
    try { const r = await axios.get('/api/query/kafka/topics'); setTopics(r.data); }
    catch { setError('Could not connect to Kafka'); }
    finally { setTopicsLoading(false); }
  }, []);

  React.useEffect(() => { fetchTopics(); }, [fetchTopics]);

  const loadMessages = async (topic) => {
    setSelected(topic); setMsgLoading(true); setError(null); setMessages(null);
    try { const r = await axios.get('/api/query/kafka/messages', { params: { topic } }); setMessages(r.data); }
    catch (e) { setError(e.response?.data?.error || 'Read failed'); }
    finally { setMsgLoading(false); }
  };

  const deleteTopic = async (topic) => {
    if (!window.confirm(`Delete Kafka topic "${topic}"? All messages will be lost.`)) return;
    setDeleting(topic);
    try {
      await axios.delete('/api/query/kafka/topic', { params: { topic } });
      if (selected === topic) { setSelected(null); setMessages(null); }
      await fetchTopics();
    } catch (e) { setError(e.response?.data?.error || 'Delete failed'); }
    finally { setDeleting(null); }
  };

  // Try to pretty-print a JSON value, else show raw
  const renderValue = (val) => {
    try { return JSON.stringify(JSON.parse(val), null, 2); }
    catch { return val; }
  };

  return (
    <>
      <ErrorBar msg={error} />
      <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        {/* Topic list */}
        <div style={{ width: 280, borderRight: '1px solid var(--color-border)', padding: 12, overflowY: 'auto', flexShrink: 0 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
            <span style={{ fontSize: 11, color: 'var(--color-text-muted)', textTransform: 'uppercase', letterSpacing: 1 }}>Topics ({topics.length})</span>
            <button className="btn btn-ghost btn-sm" onClick={fetchTopics} style={{ fontSize: 12 }}>{topicsLoading ? '…' : '🔄'}</button>
          </div>
          {topics.length === 0 && !topicsLoading && (
            <div style={{ fontSize: 12, color: 'var(--color-text-muted)', lineHeight: 1.7 }}>
              No topics yet.<br />Run a pipeline with a <strong>Kafka Sink</strong> to create one.
            </div>
          )}
          {topics.map(t => (
            <div key={t} style={{ display: 'flex', alignItems: 'center', gap: 4, marginBottom: 3 }}>
              <button className="btn btn-ghost"
                style={{ flex: 1, textAlign: 'left', padding: '6px 10px', fontSize: 12, borderRadius: 6, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontFamily: 'monospace', background: selected === t ? 'var(--color-primary)' : 'transparent', color: selected === t ? '#fff' : 'var(--color-text)' }}
                onClick={() => loadMessages(t)} title={t}>{t}
              </button>
              <button className="btn btn-ghost btn-sm"
                style={{ color: '#ef4444', padding: '4px 6px', fontSize: 12, flexShrink: 0 }}
                onClick={() => deleteTopic(t)} disabled={deleting === t} title="Delete topic">
                {deleting === t ? '…' : '🗑'}
              </button>
            </div>
          ))}
        </div>

        {/* Message viewer */}
        <div style={{ flex: 1, overflow: 'auto', padding: 16 }}>
          {!selected && <Empty icon="👈">Click a topic to view its messages<br /><br /><span style={{ fontSize: 12 }}>Topics named <code>flowforge.*</code> are pipeline execution channels — they hold the records passing between nodes.</span></Empty>}
          {msgLoading && <div style={{ color: 'var(--color-text-muted)', fontSize: 13, marginTop: 40, textAlign: 'center' }}>Reading messages (this may take a few seconds)…</div>}
          {messages && !msgLoading && (
            <>
              <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 12 }}>
                <span style={{ fontWeight: 600, fontSize: 13, fontFamily: 'monospace' }}>{messages.topic}</span>
                <span style={{ fontSize: 12, color: 'var(--color-text-muted)' }}>{messages.count} message(s)</span>
                <button className="btn btn-ghost btn-sm" onClick={() => loadMessages(selected)}>🔄 Reload</button>
              </div>
              {messages.count === 0 && <div style={{ fontSize: 13, color: 'var(--color-text-muted)' }}>Topic is empty (messages may have expired).</div>}
              {messages.messages.map((m, i) => (
                <div key={i} style={{ marginBottom: 8, background: 'var(--color-bg)', border: '1px solid var(--color-border)', borderRadius: 6, overflow: 'hidden' }}>
                  <div style={{ padding: '4px 12px', fontSize: 10, color: 'var(--color-text-muted)', borderBottom: '1px solid var(--color-border)', display: 'flex', gap: 16 }}>
                    <span>offset: {m.offset}</span>
                    <span>partition: {m.partition}</span>
                    {m.key && <span>key: {m.key}</span>}
                  </div>
                  <pre style={{ margin: 0, padding: '8px 12px', fontSize: 11, lineHeight: 1.5, whiteSpace: 'pre-wrap', wordBreak: 'break-all', maxHeight: 200, overflow: 'auto' }}>
                    {renderValue(m.value)}
                  </pre>
                </div>
              ))}
            </>
          )}
        </div>
      </div>
    </>
  );
}

// ══════════════════════════════════════════════════════════════════════════════
// MAIN COMPONENT
// ══════════════════════════════════════════════════════════════════════════════
export default function DataViewer({ onClose }) {
  const [tab, setTab] = useState(TAB_PG);

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.6)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }}>
      <div style={{ background: 'var(--color-surface)', border: '1px solid var(--color-border)', borderRadius: 12, width: '90vw', maxWidth: 1200, height: '80vh', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>

        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '12px 20px', borderBottom: '1px solid var(--color-border)', flexShrink: 0 }}>
          <span style={{ fontWeight: 700, fontSize: 15, marginRight: 10 }}>🗄️ Data Viewer</span>
          {TABS.map(t => (
            <button key={t.id} className="btn btn-ghost btn-sm"
              style={{ background: tab === t.id ? 'var(--color-primary)' : 'transparent', color: tab === t.id ? '#fff' : 'inherit', fontWeight: tab === t.id ? 600 : 400 }}
              onClick={() => setTab(t.id)}>{t.label}
            </button>
          ))}
          <div style={{ flex: 1 }} />
          <button className="btn btn-ghost btn-sm" onClick={onClose}>✕ Close</button>
        </div>

        {tab === TAB_PG    && <PostgresTab />}
        {tab === TAB_S3    && <S3Tab />}
        {tab === TAB_KAFKA && <KafkaTab />}
      </div>
    </div>
  );
}
