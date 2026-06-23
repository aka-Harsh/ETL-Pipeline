import React, { useState } from 'react';
import { interpretPipelinePrompt, generatePipelineFromAI } from '../../api/pipelineApi';

const EXAMPLE_PROMPTS = [
  'Read sales.csv from S3 bucket flowforge-data, filter rows where amount > 1000, write to Postgres table high_sales',
  'Consume from Kafka topic events, aggregate by category counting items, save results to S3',
  'Read products.json from S3 bucket flowforge-data at key input/products.json, map the name field to product_name, write to Kafka topic processed-products',
];

// ── State machine ─────────────────────────────────────────────────────────────
// idle → interpreting → confirming → generating → idle
// idle → interpreting → confirming (not understood) → idle

export default function AIPipelineChat({ onPipelineGenerated, collapsed, onToggle }) {
  const [prompt, setPrompt] = useState('');
  const [stage, setStage] = useState('idle'); // idle | interpreting | confirming | generating
  const [interpretation, setInterpretation] = useState(null); // { understood, summary, nodeCount }
  const [error, setError] = useState(null);

  // Step 1: interpret (no LLM call — instant)
  const handleInterpret = async () => {
    if (!prompt.trim()) return;
    setStage('interpreting');
    setError(null);
    setInterpretation(null);
    try {
      const result = await interpretPipelinePrompt(prompt);
      setInterpretation(result);
      setStage('confirming');
    } catch (e) {
      setError(e.response?.data?.error || e.message || 'Could not interpret prompt');
      setStage('idle');
    }
  };

  // Step 2: confirmed — use the pre-built pipeline from interpretation directly.
  // Only falls back to LLM if keyword parser didn't produce a pipeline.
  const handleConfirm = async () => {
    setStage('generating');
    setError(null);
    try {
      if (interpretation?.pipeline) {
        // Keyword parser already built a valid pipeline — use it immediately, no LLM call
        onPipelineGenerated?.(interpretation.pipeline);
        setStage('idle');
        setInterpretation(null);
      } else {
        // Keyword parser couldn't build one — fall back to LLM
        const pipelineDef = await generatePipelineFromAI(prompt);
        onPipelineGenerated?.(pipelineDef);
        setStage('idle');
        setInterpretation(null);
      }
    } catch (e) {
      setError(e.response?.data?.error || e.message || 'Generation failed');
      setStage('idle');
    }
  };

  const handleCancel = () => {
    setStage('idle');
    setInterpretation(null);
    setError(null);
  };

  const isInterpreting = stage === 'interpreting';
  const isGenerating   = stage === 'generating';
  const isConfirming   = stage === 'confirming';
  const busy           = isInterpreting || isGenerating;

  return (
    <div className={`app-bottom ${collapsed ? 'collapsed' : ''}`}>
      <div className="ai-panel">
        <div className="ai-panel-header">
          <span>🤖 AI Pipeline Generator</span>
          <button className="btn btn-ghost btn-sm" onClick={onToggle}>
            {collapsed ? '▲' : '▼'}
          </button>
        </div>

        {!collapsed && (
          <>
            <div className="ai-panel-body">
              <textarea
                className="ai-textarea"
                placeholder="Describe your pipeline in plain English… e.g. Read sales.csv from S3, filter rows where amount > 500, write to Postgres"
                value={prompt}
                onChange={e => { setPrompt(e.target.value); if (isConfirming) handleCancel(); }}
                onKeyDown={e => e.key === 'Enter' && e.ctrlKey && !busy && handleInterpret()}
                disabled={busy}
              />
              <div style={{ display: 'flex', flexDirection: 'column', gap: 6, justifyContent: 'center' }}>
                <button
                  className="btn btn-primary"
                  onClick={handleInterpret}
                  disabled={busy || !prompt.trim()}
                  style={{ whiteSpace: 'nowrap' }}
                >
                  {isInterpreting
                    ? <><span className="spinner" /> Interpreting…</>
                    : isGenerating
                    ? <><span className="spinner" /> Generating…</>
                    : '✨ Generate'}
                </button>
                <button
                  className="btn btn-ghost btn-sm"
                  onClick={() => { setPrompt(''); handleCancel(); }}
                  disabled={busy}
                >
                  Clear
                </button>
              </div>
            </div>

            {/* Confirmation card */}
            {isConfirming && interpretation && (
              <div style={{
                margin: '0 12px 10px',
                padding: '10px 14px',
                background: interpretation.understood
                  ? 'rgba(34,197,94,0.08)'
                  : 'rgba(239,68,68,0.08)',
                border: `1px solid ${interpretation.understood ? 'rgba(34,197,94,0.3)' : 'rgba(239,68,68,0.3)'}`,
                borderRadius: 8,
                fontSize: 12,
              }}>
                <div style={{ fontWeight: 600, marginBottom: 6, color: interpretation.understood ? '#22c55e' : '#ef4444' }}>
                  {interpretation.understood
                    ? `✅ I understood a ${interpretation.nodeCount}-node pipeline:`
                    : '⚠️ I couldn\'t fully understand your request:'}
                </div>
                <div style={{ color: 'var(--color-text)', lineHeight: 1.6, marginBottom: 10 }}>
                  {interpretation.summary}
                </div>
                {interpretation.understood && (
                  <div style={{ display: 'flex', gap: 8 }}>
                    <button className="btn btn-primary btn-sm" onClick={handleConfirm}>
                      Yes, generate this pipeline
                    </button>
                    <button className="btn btn-ghost btn-sm" onClick={handleCancel}>
                      No, edit prompt
                    </button>
                  </div>
                )}
                {!interpretation.understood && (
                  <button className="btn btn-ghost btn-sm" onClick={handleCancel}>
                    Edit prompt
                  </button>
                )}
              </div>
            )}

            {/* Example chips (only when idle) */}
            {stage === 'idle' && (
              <div className="ai-chips">
                {EXAMPLE_PROMPTS.map((p, i) => (
                  <button key={i} className="ai-chip" onClick={() => setPrompt(p)}>
                    {p.substring(0, 55)}…
                  </button>
                ))}
              </div>
            )}

            {error && <div className="ai-error">⚠️ {error}</div>}
          </>
        )}
      </div>
    </div>
  );
}
