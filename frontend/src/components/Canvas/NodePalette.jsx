import React from 'react';

const NODE_PALETTE = [
  {
    category: 'Sources',
    cssClass: 'source',
    nodes: [
      { type: 'S3_SOURCE', icon: '☁️', label: 'S3 File Source', desc: 'Read CSV/JSON from S3' },
      { type: 'KAFKA_SOURCE', icon: '📡', label: 'Kafka Topic', desc: 'Consume from Kafka' },
    ],
  },
  {
    category: 'Transforms',
    cssClass: 'transform',
    nodes: [
      { type: 'FILTER', icon: '🔍', label: 'Filter', desc: 'Filter by field condition' },
      { type: 'FIELD_MAPPER', icon: '🔀', label: 'Field Mapper', desc: 'Rename / select fields' },
      { type: 'AGGREGATOR', icon: '📊', label: 'Aggregator', desc: 'Group & aggregate' },
    ],
  },
  {
    category: 'Sinks',
    cssClass: 'sink',
    nodes: [
      { type: 'POSTGRES_SINK', icon: '🗄️', label: 'PostgreSQL', desc: 'Write to Postgres table' },
      { type: 'S3_SINK', icon: '💾', label: 'S3 Output', desc: 'Write CSV/JSON to S3' },
      { type: 'KAFKA_SINK', icon: '📤', label: 'Kafka Output', desc: 'Publish to Kafka topic' },
    ],
  },
];

export default function NodePalette() {
  const onDragStart = (event, nodeType) => {
    event.dataTransfer.setData('application/flowforge-node-type', nodeType);
    event.dataTransfer.effectAllowed = 'move';
  };

  return (
    <div style={{ padding: '8px 0' }}>
      <div style={{ padding: '8px 12px 4px', fontSize: 11, fontWeight: 700, color: 'var(--color-text-muted)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
        Node Types
      </div>
      {NODE_PALETTE.map(group => (
        <div key={group.category} className="palette-section">
          <div className="palette-section-title">{group.category}</div>
          {group.nodes.map(node => (
            <div
              key={node.type}
              className={`palette-node ${group.cssClass}`}
              draggable
              onDragStart={e => onDragStart(e, node.type)}
              title={node.desc}
            >
              <span className="palette-node-icon">{node.icon}</span>
              <div>
                <div className="palette-node-name">{node.label}</div>
                <div style={{ fontSize: 10, color: 'var(--color-text-muted)', marginTop: 1 }}>{node.desc}</div>
              </div>
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}
