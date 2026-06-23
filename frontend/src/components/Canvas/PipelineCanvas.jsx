import React, { useCallback, useRef } from 'react';
import ReactFlow, {
  MiniMap,
  Controls,
  Background,
  BackgroundVariant,
  addEdge,
  useNodesState,
  useEdgesState,
  useReactFlow,
} from 'reactflow';

import S3SourceNode from '../Nodes/S3SourceNode';
import KafkaSourceNode from '../Nodes/KafkaSourceNode';
import FilterNode from '../Nodes/FilterNode';
import MapperNode from '../Nodes/MapperNode';
import AggregatorNode from '../Nodes/AggregatorNode';
import PostgresSinkNode from '../Nodes/PostgresSinkNode';
import S3SinkNode from '../Nodes/S3SinkNode';
import KafkaSinkNode from '../Nodes/KafkaSinkNode';
import { isValidConnection } from './EdgeValidator';

const NODE_TYPE_MAP = {
  S3_SOURCE: S3SourceNode,
  KAFKA_SOURCE: KafkaSourceNode,
  FILTER: FilterNode,
  FIELD_MAPPER: MapperNode,
  AGGREGATOR: AggregatorNode,
  POSTGRES_SINK: PostgresSinkNode,
  S3_SINK: S3SinkNode,
  KAFKA_SINK: KafkaSinkNode,
};

const NODE_LABELS = {
  S3_SOURCE: 'S3 Source', KAFKA_SOURCE: 'Kafka Source',
  FILTER: 'Filter', FIELD_MAPPER: 'Field Mapper', AGGREGATOR: 'Aggregator',
  POSTGRES_SINK: 'PostgreSQL', S3_SINK: 'S3 Output', KAFKA_SINK: 'Kafka Output',
};

let idCounter = 1;

export default function PipelineCanvas({
  initialNodes = [], initialEdges = [],
  onNodesChange: onNodesChangeProp,
  onEdgesChange: onEdgesChangeProp,
  onNodeSelect,
  nodeStatuses = {},
}) {
  const reactFlowWrapper = useRef(null);
  const { screenToFlowPosition } = useReactFlow();

  // Merge node status into node data
  const enrichNodes = (nodes) => nodes.map(n => ({
    ...n,
    data: {
      ...n.data,
      status: nodeStatuses[n.id]?.status || nodeStatuses[n.id] || n.data?.status,
      recordsProcessed: nodeStatuses[n.id]?.recordsProcessed || n.data?.recordsProcessed,
    },
  }));

  const [nodes, setNodes, onNodesChange] = useNodesState(enrichNodes(initialNodes));
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  // Sync external changes (e.g., loading a pipeline or AI generation)
  React.useEffect(() => {
    setNodes(enrichNodes(initialNodes));
  }, [initialNodes]);

  React.useEffect(() => {
    setEdges(initialEdges);
  }, [initialEdges]);

  // Update node statuses when they change
  React.useEffect(() => {
    setNodes(prev => enrichNodes(prev));
  }, [nodeStatuses]);

  const onConnect = useCallback((params) => {
    setEdges(eds => addEdge({ ...params, animated: true, style: { stroke: '#6366f1' } }, eds));
    onEdgesChangeProp?.([...edges, params]);
  }, [edges, setEdges, onEdgesChangeProp]);

  const onDrop = useCallback((event) => {
    event.preventDefault();
    const nodeType = event.dataTransfer.getData('application/flowforge-node-type');
    if (!nodeType) return;

    const position = screenToFlowPosition({ x: event.clientX, y: event.clientY });
    const id = `node_${idCounter++}_${Date.now()}`;

    const newNode = {
      id,
      type: nodeType,
      position,
      data: {
        label: NODE_LABELS[nodeType] || nodeType,
        nodeType,
        config: {},
        status: 'WAITING',
      },
    };

    setNodes(nds => [...nds, newNode]);
    onNodesChangeProp?.([...nodes, newNode]);
  }, [screenToFlowPosition, nodes, setNodes, onNodesChangeProp]);

  const onDragOver = useCallback((event) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onNodeClick = useCallback((_, node) => {
    onNodeSelect?.(node);
  }, [onNodeSelect]);

  const onPaneClick = useCallback(() => {
    onNodeSelect?.(null);
  }, [onNodeSelect]);

  const handleNodesChange = useCallback((changes) => {
    onNodesChange(changes);
    const removedIds = new Set(changes.filter(c => c.type === 'remove').map(c => c.id));
    if (removedIds.size > 0) {
      onNodesChangeProp?.(prev => prev.filter(n => !removedIds.has(n.id)));
      onEdgesChangeProp?.(prev => prev.filter(e => !removedIds.has(e.source) && !removedIds.has(e.target)));
    }
  }, [onNodesChange, onNodesChangeProp, onEdgesChangeProp]);

  const handleEdgesChange = useCallback((changes) => {
    onEdgesChange(changes);
    const removedIds = new Set(changes.filter(c => c.type === 'remove').map(c => c.id));
    if (removedIds.size > 0) {
      onEdgesChangeProp?.(prev => prev.filter(e => !removedIds.has(e.id)));
    }
  }, [onEdgesChange, onEdgesChangeProp]);

  return (
    <div ref={reactFlowWrapper} style={{ width: '100%', height: '100%' }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={handleNodesChange}
        onEdgesChange={handleEdgesChange}
        onConnect={onConnect}
        onDrop={onDrop}
        onDragOver={onDragOver}
        onNodeClick={onNodeClick}
        onPaneClick={onPaneClick}
        nodeTypes={NODE_TYPE_MAP}
        isValidConnection={(conn) => isValidConnection(conn, nodes)}
        fitView
        deleteKeyCode="Delete"
        multiSelectionKeyCode="Shift"
      >
        <Background variant={BackgroundVariant.Dots} color="#1e293b" gap={24} />
        <MiniMap
          nodeColor={(n) => {
            const t = n.data?.nodeType || '';
            if (t.includes('SOURCE')) return '#3b82f6';
            if (t.includes('SINK')) return '#f97316';
            return '#22c55e';
          }}
          maskColor="rgba(15,23,42,0.7)"
        />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
}
