import React, { useState, useEffect, useCallback } from 'react';
import { ReactFlowProvider } from 'reactflow';

import NodePalette from './components/Canvas/NodePalette';
import PipelineCanvas from './components/Canvas/PipelineCanvas';
import NodeConfigPanel from './components/Config/NodeConfigPanel';
import ExecutionMonitor from './components/Dashboard/ExecutionMonitor';
import AIPipelineChat from './components/AI/AIPipelineChat';
import DataViewer from './components/Dashboard/DataViewer';

import {
  getPipelines, createPipeline, updatePipeline, deletePipeline, executePipeline,
} from './api/pipelineApi';
import { useExecutionStatus } from './hooks/useExecutionStatus';

// ─── Demo pipeline that loads on first open ───────────────────────────────────
const DEMO_PIPELINE = {
  nodes: [
    { id: 'demo_1', type: 'S3_SOURCE',    position: { x: 80,  y: 180 }, data: { label: 'Sales CSV', nodeType: 'S3_SOURCE',    config: { bucket: 'flowforge-data', key: 'input/sales.csv', format: 'csv', hasHeader: true } } },
    { id: 'demo_2', type: 'FILTER',       position: { x: 340, y: 180 }, data: { label: 'High Value', nodeType: 'FILTER',       config: { field: 'amount', operator: '>', value: '1000' } } },
    { id: 'demo_3', type: 'POSTGRES_SINK',position: { x: 600, y: 180 }, data: { label: 'Analytics DB', nodeType: 'POSTGRES_SINK', config: { table: 'high_value_sales', writeMode: 'append' } } },
  ],
  edges: [
    { id: 'e1', source: 'demo_1', target: 'demo_2', animated: true, style: { stroke: '#6366f1' } },
    { id: 'e2', source: 'demo_2', target: 'demo_3', animated: true, style: { stroke: '#6366f1' } },
  ],
};

function toPipelineNodes(nodes) {
  return (nodes || []).map(n => ({
    ...n,
    type: n.data?.nodeType || n.type,
  }));
}

export default function App() {
  // ── Pipeline list state ──
  const [pipelines, setPipelines] = useState([]);
  const [currentPipeline, setCurrentPipeline] = useState(null);
  const [pipelineName, setPipelineName] = useState('Demo Pipeline');

  // ── Canvas state ──
  const [canvasNodes, setCanvasNodes] = useState(toPipelineNodes(DEMO_PIPELINE.nodes));
  const [canvasEdges, setCanvasEdges] = useState(DEMO_PIPELINE.edges);

  // ── UI state ──
  const [selectedNode, setSelectedNode] = useState(null);
  const [aiCollapsed, setAiCollapsed] = useState(false);
  const [saving, setSaving] = useState(false);
  const [runningExecId, setRunningExecId] = useState(null);
  const [isRunning, setIsRunning] = useState(false);
  const [notification, setNotification] = useState(null);
  const [showDataViewer, setShowDataViewer] = useState(false);

  // ── Execution status polling ──
  const { status, nodeStatuses, error: statusError } = useExecutionStatus(
    currentPipeline?.id, runningExecId, isRunning
  );

  // Stop polling when done
  useEffect(() => {
    if (status === 'COMPLETED' || status === 'FAILED') {
      setIsRunning(false);
      showNotification(status === 'COMPLETED' ? '✅ Pipeline completed!' : '❌ Pipeline failed');
    }
  }, [status]);

  // ── Load pipelines on mount ──
  useEffect(() => {
    loadPipelines();
  }, []);

  const loadPipelines = async () => {
    try {
      const list = await getPipelines();
      setPipelines(list);
    } catch (e) {
      console.warn('Could not load pipelines:', e.message);
    }
  };

  const showNotification = (msg) => {
    setNotification(msg);
    setTimeout(() => setNotification(null), 4000);
  };

  // ── Load a saved pipeline ──
  const loadPipeline = (pipeline) => {
    setCurrentPipeline(pipeline);
    setPipelineName(pipeline.name);
    let def;
    try {
      def = typeof pipeline.definition === 'string'
        ? JSON.parse(pipeline.definition)
        : pipeline.definition;
    } catch { def = DEMO_PIPELINE; }
    setCanvasNodes(toPipelineNodes(def.nodes || []));
    setCanvasEdges(def.edges || []);
    setSelectedNode(null);
    setRunningExecId(null);
    setIsRunning(false);
  };

  // ── New pipeline ──
  const handleNew = () => {
    setCurrentPipeline(null);
    setPipelineName('New Pipeline');
    setCanvasNodes([]);
    setCanvasEdges([]);
    setSelectedNode(null);
    setRunningExecId(null);
    setIsRunning(false);
  };

  // ── Save pipeline ──
  const handleSave = async () => {
    setSaving(true);
    try {
      const definition = { nodes: canvasNodes, edges: canvasEdges };
      if (currentPipeline?.id) {
        const updated = await updatePipeline(currentPipeline.id, { name: pipelineName, definition });
        setCurrentPipeline(updated);
        showNotification('💾 Pipeline saved');
      } else {
        const created = await createPipeline({ name: pipelineName, description: '', definition });
        setCurrentPipeline(created);
        showNotification('💾 Pipeline created');
      }
      await loadPipelines();
    } catch (e) {
      showNotification('⚠️ Save failed: ' + e.message);
    } finally {
      setSaving(false);
    }
  };

  // ── Delete pipeline ──
  const handleDelete = async () => {
    if (!currentPipeline?.id) return;
    if (!confirm('Delete this pipeline?')) return;
    try {
      await deletePipeline(currentPipeline.id);
      handleNew();
      await loadPipelines();
      showNotification('🗑️ Pipeline deleted');
    } catch (e) {
      showNotification('⚠️ Delete failed: ' + e.message);
    }
  };

  // ── Run pipeline ──
  const handleRun = async () => {
    if (!currentPipeline?.id) {
      showNotification('⚠️ Save the pipeline before running');
      return;
    }
    try {
      const result = await executePipeline(currentPipeline.id);
      const execId = result.executionId || crypto.randomUUID();
      setRunningExecId(execId);
      setIsRunning(true);
      showNotification('🚀 Pipeline started!');
    } catch (e) {
      showNotification('⚠️ Failed to start: ' + e.message);
    }
  };

  // ── Node config changes ──
  const handleConfigChange = useCallback((nodeId, config) => {
    setCanvasNodes(prev => prev.map(n => n.id === nodeId ? { ...n, data: { ...n.data, config } } : n));
  }, []);

  const handleLabelChange = useCallback((nodeId, label) => {
    setCanvasNodes(prev => prev.map(n => n.id === nodeId ? { ...n, data: { ...n.data, label } } : n));
  }, []);

  const handleDeleteNode = useCallback((nodeId) => {
    setCanvasNodes(prev => prev.filter(n => n.id !== nodeId));
    setCanvasEdges(prev => prev.filter(e => e.source !== nodeId && e.target !== nodeId));
    setSelectedNode(null);
  }, []);

  // ── Download pipeline ──
  const handleDownload = () => {
    const definition = { nodes: canvasNodes, edges: canvasEdges };
    const blob = new Blob([JSON.stringify(definition, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${pipelineName.replace(/\s+/g, '_') || 'pipeline'}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  // ── AI generation ──
  const handleAIPipeline = (def) => {
    // AI returns nodes with label/config/type at top level — normalize into data object
    const nodes = (def.nodes || []).map(n => ({
      id: n.id,
      type: n.data?.nodeType || n.type,
      position: n.position || { x: 100, y: 200 },
      data: n.data ?? {
        label: n.label || n.type,
        nodeType: n.type,
        config: n.config || {},
        status: 'WAITING',
      },
    }));
    const edges = (def.edges || []).map(e => ({ ...e, animated: true, style: { stroke: '#6366f1' } }));
    setCanvasNodes(nodes);
    setCanvasEdges(edges);
    setSelectedNode(null);
    setRunningExecId(null);
    showNotification('✨ AI pipeline generated!');
  };

  const showMonitor = isRunning || (!!runningExecId && !selectedNode);
  const showConfig = !!selectedNode && !isRunning;

  return (
    <div className="app-layout">
      {/* ── Navbar ── */}
      <nav className="app-navbar">
        <span className="navbar-logo">Flow<span>Forge</span></span>

        <select
          className="navbar-select"
          value={currentPipeline?.id || ''}
          onChange={e => {
            const p = pipelines.find(p => p.id === e.target.value);
            if (p) loadPipeline(p);
          }}
        >
          <option value="">— select pipeline —</option>
          {pipelines.map(p => (
            <option key={p.id} value={p.id}>{p.name}</option>
          ))}
        </select>

        <input
          className="navbar-input"
          value={pipelineName}
          onChange={e => setPipelineName(e.target.value)}
          placeholder="Pipeline name…"
        />

        <button className="btn btn-ghost" onClick={handleNew}>+ New</button>
        <button className="btn btn-primary" onClick={handleSave} disabled={saving}>
          {saving ? 'Saving…' : '💾 Save'}
        </button>
        {currentPipeline?.id && (
          <button className="btn btn-danger" onClick={handleDelete}>🗑️</button>
        )}

        <button className="btn btn-ghost" onClick={handleDownload} title="Download pipeline as JSON">⬇️ Export</button>
        <button className="btn btn-ghost" onClick={() => setShowDataViewer(true)}>🗄️ Data</button>
        <div className="navbar-spacer" />

        <button
          className="btn btn-run"
          onClick={handleRun}
          disabled={isRunning || !currentPipeline?.id}
        >
          {isRunning ? <><span className="spinner" /> Running…</> : '▶ Run Pipeline'}
        </button>
      </nav>

      {/* ── Notification toast ── */}
      {notification && (
        <div style={{
          position: 'fixed', top: 60, left: '50%', transform: 'translateX(-50%)',
          background: 'var(--color-surface)', border: '1px solid var(--color-border)',
          padding: '8px 20px', borderRadius: 8, fontSize: 13, fontWeight: 500,
          zIndex: 1000, boxShadow: '0 4px 20px rgba(0,0,0,0.4)',
        }}>
          {notification}
        </div>
      )}

      {/* ── Main body ── */}
      <div className="app-body">
        {/* Left sidebar: node palette */}
        <div className="app-sidebar">
          <NodePalette />
        </div>

        {/* Center: canvas */}
        <div className="app-canvas">
          <ReactFlowProvider>
            <PipelineCanvas
              initialNodes={canvasNodes}
              initialEdges={canvasEdges}
              onNodesChange={setCanvasNodes}
              onEdgesChange={setCanvasEdges}
              onNodeSelect={setSelectedNode}
              nodeStatuses={nodeStatuses}
            />
          </ReactFlowProvider>
        </div>

        {/* Right panel */}
        <div className="app-right-panel">
          {showMonitor ? (
            <ExecutionMonitor
              pipelineId={currentPipeline?.id}
              executionId={runningExecId}
              status={status}
              nodeStatuses={nodeStatuses}
              nodes={canvasNodes}
            />
          ) : showConfig ? (
            <NodeConfigPanel
              node={selectedNode}
              onConfigChange={handleConfigChange}
              onLabelChange={handleLabelChange}
              onDelete={handleDeleteNode}
            />
          ) : (
            <div className="empty-state" style={{ minHeight: 200 }}>
              <div className="empty-state-icon">🎛️</div>
              <div className="empty-state-text">Click a node to configure</div>
              <div style={{ fontSize: 11, color: 'var(--color-text-muted)', textAlign: 'center', padding: '0 16px' }}>
                Drag node types from the left panel onto the canvas
              </div>
            </div>
          )}
        </div>
      </div>

      {/* ── AI chat panel ── */}
      <AIPipelineChat
        onPipelineGenerated={handleAIPipeline}
        collapsed={aiCollapsed}
        onToggle={() => setAiCollapsed(c => !c)}
      />

      {/* ── Data Viewer modal ── */}
      {showDataViewer && <DataViewer onClose={() => setShowDataViewer(false)} />}
    </div>
  );
}
