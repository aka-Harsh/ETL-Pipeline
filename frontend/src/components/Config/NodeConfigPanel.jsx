import React, { useState, useEffect } from 'react';

const FIELD_CONFIGS = {
  S3_SOURCE: [
    { key: 'bucket', label: 'Bucket Name', type: 'text', placeholder: 'flowforge-data' },
    { key: 'key', label: 'File Key', type: 'text', placeholder: 'input/sales.csv' },
    { key: 'format', label: 'Format', type: 'select', options: ['csv', 'json'] },
    { key: 'hasHeader', label: 'Has Header Row', type: 'checkbox' },
  ],
  KAFKA_SOURCE: [
    { key: 'topic', label: 'Topic Name', type: 'text', placeholder: 'my-topic' },
    { key: 'consumerGroup', label: 'Consumer Group', type: 'text', placeholder: 'my-group' },
  ],
  FILTER: [
    { key: 'field', label: 'Field Name', type: 'text', placeholder: 'amount' },
    { key: 'operator', label: 'Operator', type: 'select', options: ['==', '!=', '>', '<', 'contains'] },
    { key: 'value', label: 'Value', type: 'text', placeholder: '1000' },
  ],
  FIELD_MAPPER: [], // handled specially (mappings array)
  AGGREGATOR: [
    { key: 'groupBy', label: 'Group By Field', type: 'text', placeholder: 'category' },
    { key: 'function', label: 'Function', type: 'select', options: ['count', 'sum', 'avg'] },
    { key: 'field', label: 'Aggregate Field', type: 'text', placeholder: 'amount' },
  ],
  POSTGRES_SINK: [
    { key: 'table', label: 'Table Name', type: 'text', placeholder: 'output_table' },
    { key: 'writeMode', label: 'Write Mode', type: 'select', options: ['append', 'upsert'] },
  ],
  S3_SINK: [
    { key: 'bucket', label: 'Bucket Name', type: 'text', placeholder: 'flowforge-output' },
    { key: 'keyPrefix', label: 'Key Prefix', type: 'text', placeholder: 'output/' },
    { key: 'format', label: 'Format', type: 'select', options: ['csv', 'json'] },
  ],
  KAFKA_SINK: [
    { key: 'topic', label: 'Topic Name', type: 'text', placeholder: 'output-topic' },
  ],
};

export default function NodeConfigPanel({ node, onConfigChange, onLabelChange, onDelete }) {
  const nodeType = node?.data?.nodeType;
  const [config, setConfig] = useState(node?.data?.config || {});
  const [label, setLabel] = useState(node?.data?.label || '');
  const [mappings, setMappings] = useState(node?.data?.config?.mappings || [{ from: '', to: '' }]);

  useEffect(() => {
    setConfig(node?.data?.config || {});
    setLabel(node?.data?.label || '');
    setMappings(node?.data?.config?.mappings || [{ from: '', to: '' }]);
  }, [node?.id]);

  const handleChange = (key, value) => {
    const next = { ...config, [key]: value };
    setConfig(next);
    onConfigChange?.(node.id, next);
  };

  const handleLabelChange = (v) => {
    setLabel(v);
    onLabelChange?.(node.id, v);
  };

  const handleMappingChange = (i, field, value) => {
    const next = mappings.map((m, idx) => idx === i ? { ...m, [field]: value } : m);
    setMappings(next);
    const nextConfig = { ...config, mappings: next };
    setConfig(nextConfig);
    onConfigChange?.(node.id, nextConfig);
  };

  const addMapping = () => setMappings([...mappings, { from: '', to: '' }]);
  const removeMapping = (i) => {
    const next = mappings.filter((_, idx) => idx !== i);
    setMappings(next);
    onConfigChange?.(node.id, { ...config, mappings: next });
  };

  if (!node) return (
    <div style={{ padding: 20, textAlign: 'center', color: 'var(--color-text-muted)', fontSize: 13 }}>
      Click a node to configure it
    </div>
  );

  const fields = FIELD_CONFIGS[nodeType] || [];

  return (
    <div>
      <div className="panel-header">
        <span>⚙️ Configure Node</span>
        <button className="btn btn-danger btn-sm" onClick={() => onDelete?.(node.id)}>Delete</button>
      </div>
      <div className="panel-body">
        <div className="form-group">
          <label className="form-label">Label</label>
          <input
            className="form-input"
            value={label}
            onChange={e => handleLabelChange(e.target.value)}
            placeholder="Node label..."
          />
        </div>

        <div className="divider" />

        {nodeType === 'FIELD_MAPPER' ? (
          <div className="form-group">
            <label className="form-label">Field Mappings</label>
            {mappings.map((m, i) => (
              <div key={i} style={{ display: 'flex', gap: 6, marginBottom: 6, alignItems: 'center' }}>
                <input
                  className="form-input"
                  placeholder="from"
                  value={m.from}
                  onChange={e => handleMappingChange(i, 'from', e.target.value)}
                />
                <span style={{ color: 'var(--color-text-muted)' }}>→</span>
                <input
                  className="form-input"
                  placeholder="to"
                  value={m.to}
                  onChange={e => handleMappingChange(i, 'to', e.target.value)}
                />
                <button className="btn btn-ghost btn-sm" onClick={() => removeMapping(i)}>×</button>
              </div>
            ))}
            <button className="btn btn-ghost btn-sm" onClick={addMapping}>+ Add mapping</button>
          </div>
        ) : (
          fields.map(f => (
            <div className="form-group" key={f.key}>
              <label className="form-label">{f.label}</label>
              {f.type === 'select' ? (
                <select
                  className="form-select"
                  value={config[f.key] || f.options[0]}
                  onChange={e => handleChange(f.key, e.target.value)}
                >
                  {f.options.map(o => <option key={o} value={o}>{o}</option>)}
                </select>
              ) : f.type === 'checkbox' ? (
                <label className="form-checkbox">
                  <input
                    type="checkbox"
                    checked={!!config[f.key]}
                    onChange={e => handleChange(f.key, e.target.checked)}
                  />
                  {f.label}
                </label>
              ) : (
                <input
                  className="form-input"
                  type="text"
                  value={config[f.key] || ''}
                  placeholder={f.placeholder}
                  onChange={e => handleChange(f.key, e.target.value)}
                />
              )}
            </div>
          ))
        )}

        <div style={{ marginTop: 8, padding: '8px 10px', background: 'var(--color-surface-2)', borderRadius: 6, fontSize: 11, color: 'var(--color-text-muted)' }}>
          Type: <strong style={{ color: 'var(--color-text)' }}>{nodeType}</strong>
          &nbsp;·&nbsp;ID: <code style={{ fontFamily: 'JetBrains Mono, monospace' }}>{node.id}</code>
        </div>
      </div>
    </div>
  );
}
