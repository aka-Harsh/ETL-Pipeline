import React from 'react';
import { Handle, Position } from 'reactflow';

const CATEGORY_ICONS = {
  S3_SOURCE: '☁️',
  KAFKA_SOURCE: '📡',
  FILTER: '🔍',
  FIELD_MAPPER: '🔀',
  AGGREGATOR: '📊',
  POSTGRES_SINK: '🗄️',
  S3_SINK: '💾',
  KAFKA_SINK: '📤',
};

const TYPE_TO_CATEGORY = {
  S3_SOURCE: 'source', KAFKA_SOURCE: 'source',
  FILTER: 'transform', FIELD_MAPPER: 'transform', AGGREGATOR: 'transform',
  POSTGRES_SINK: 'sink', S3_SINK: 'sink', KAFKA_SINK: 'sink',
};

const STATUS_DOTS = {
  WAITING:   { color: '#64748b', label: '○' },
  RUNNING:   { color: '#fb923c', label: '◉' },
  COMPLETED: { color: '#4ade80', label: '●' },
  FAILED:    { color: '#f87171', label: '✕' },
};

export default function BaseNode({ data, selected, showTargetHandle = true, showSourceHandle = true }) {
  const nodeType = data.nodeType || 'UNKNOWN';
  const category = TYPE_TO_CATEGORY[nodeType] || 'transform';
  const icon = CATEGORY_ICONS[nodeType] || '⚙️';
  const status = data.status || 'WAITING';
  const dot = STATUS_DOTS[status] || STATUS_DOTS.WAITING;

  return (
    <div className={`flow-node ${category} ${selected ? 'selected' : ''}`}>
      {showTargetHandle && (
        <Handle type="target" position={Position.Left} style={{ background: '#475569' }} />
      )}

      <div className="node-header">
        <span className="node-icon">{icon}</span>
        <div>
          <div className="node-label">{data.label || nodeType}</div>
          <div className="node-type">{nodeType}</div>
        </div>
      </div>

      {status !== 'WAITING' && (
        <div className="node-status-row">
          <span style={{ color: dot.color, fontWeight: 700 }}>{dot.label}</span>
          <span>{status}</span>
          {data.recordsProcessed != null && data.recordsProcessed > 0 && (
            <span style={{ marginLeft: 'auto', fontFamily: 'JetBrains Mono, monospace', fontSize: 10 }}>
              {data.recordsProcessed.toLocaleString()} rec
            </span>
          )}
        </div>
      )}

      {showSourceHandle && (
        <Handle type="source" position={Position.Right} style={{ background: '#475569' }} />
      )}
    </div>
  );
}
