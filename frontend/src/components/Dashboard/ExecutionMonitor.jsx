import React, { useState, useEffect } from 'react';
import NodeStatusBadge from './NodeStatusBadge';
import { getPipelineHistory } from '../../api/pipelineApi';

export default function ExecutionMonitor({ pipelineId, executionId, status, nodeStatuses, nodes = [] }) {
  const [history, setHistory] = useState([]);
  const [showHistory, setShowHistory] = useState(false);
  const [elapsed, setElapsed] = useState(0);
  const [startTime] = useState(Date.now());

  useEffect(() => {
    if (!status || status === 'COMPLETED' || status === 'FAILED') return;
    const timer = setInterval(() => setElapsed(Math.floor((Date.now() - startTime) / 1000)), 1000);
    return () => clearInterval(timer);
  }, [status, startTime]);

  const loadHistory = async () => {
    try {
      const h = await getPipelineHistory(pipelineId);
      setHistory(h);
      setShowHistory(true);
    } catch (e) {
      console.error('Failed to load history', e);
    }
  };

  const formatElapsed = (s) => `${Math.floor(s / 60)}m ${s % 60}s`;

  return (
    <div>
      <div className="panel-header">
        <span>⚡ Execution Monitor</span>
        <button className="btn btn-ghost btn-sm" onClick={loadHistory}>History</button>
      </div>
      <div className="panel-body">
        <div className="monitor-section">
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 10 }}>
            <NodeStatusBadge status={status || 'IDLE'} />
            {status && status !== 'COMPLETED' && status !== 'FAILED' && (
              <span style={{ fontSize: 12, color: 'var(--color-text-muted)' }}>
                {formatElapsed(elapsed)}
              </span>
            )}
          </div>
          {executionId && (
            <div style={{ fontSize: 10, color: 'var(--color-text-muted)', fontFamily: 'JetBrains Mono, monospace', marginBottom: 10 }}>
              ID: {executionId.substring(0, 16)}…
            </div>
          )}
        </div>

        {nodes.length > 0 && (
          <div className="monitor-section">
            <div className="monitor-section-title">Node Status</div>
            {nodes.map(node => {
              const nodeId = node.id;
              const nodeStatus = nodeStatuses[nodeId] || {};
              const ns = typeof nodeStatus === 'object' ? nodeStatus.status : nodeStatus;
              const records = typeof nodeStatus === 'object' ? nodeStatus.recordsProcessed : null;
              return (
                <div key={nodeId} className="node-status-row-monitor">
                  <span style={{ fontWeight: 500, fontSize: 12 }}>
                    {node.data?.label || nodeId}
                  </span>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    {records != null && records > 0 && (
                      <span className="records-count">{Number(records).toLocaleString()}</span>
                    )}
                    <NodeStatusBadge status={ns || 'WAITING'} />
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {showHistory && history.length > 0 && (
          <div className="monitor-section">
            <div className="monitor-section-title">
              Recent Runs
              <button className="btn btn-ghost btn-sm" style={{ marginLeft: 8 }} onClick={() => setShowHistory(false)}>×</button>
            </div>
            <table className="history-table">
              <thead>
                <tr>
                  <th>Status</th>
                  <th>Duration</th>
                  <th>Records</th>
                </tr>
              </thead>
              <tbody>
                {history.slice(0, 5).map((run, i) => (
                  <tr key={i}>
                    <td><NodeStatusBadge status={run.status} /></td>
                    <td>{run.durationMs ? `${(run.durationMs / 1000).toFixed(1)}s` : '—'}</td>
                    <td>{run.totalRecordsProcessed || '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {showHistory && history.length === 0 && (
          <p style={{ fontSize: 12, color: 'var(--color-text-muted)' }}>No execution history yet.</p>
        )}
      </div>
    </div>
  );
}
