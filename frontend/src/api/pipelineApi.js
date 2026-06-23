import axios from 'axios';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8080';

const api = axios.create({
  baseURL: API_BASE,
  headers: { 'Content-Type': 'application/json' },
  timeout: 30000,
});

export const getPipelines = () => api.get('/api/pipelines').then(r => r.data);

export const getPipeline = (id) => api.get(`/api/pipelines/${id}`).then(r => r.data);

export const createPipeline = (data) => api.post('/api/pipelines', data).then(r => r.data);

export const updatePipeline = (id, data) => api.put(`/api/pipelines/${id}`, data).then(r => r.data);

export const deletePipeline = (id) => api.delete(`/api/pipelines/${id}`).then(r => r.data);

export const executePipeline = (id) => api.post(`/api/pipelines/${id}/execute`).then(r => r.data);

export const getPipelineStatus = (pipelineId, executionId) => {
  const params = executionId ? { executionId } : {};
  return api.get(`/api/pipelines/${pipelineId}/status`, { params }).then(r => r.data);
};

export const getPipelineHistory = (id) => api.get(`/api/pipelines/${id}/history`).then(r => r.data);

export const getNodeTypes = () => api.get('/api/node-types').then(r => r.data);

export const interpretPipelinePrompt = (prompt) =>
  api.post('/api/ai/interpret-prompt', { prompt }, { timeout: 10000 }).then(r => r.data);

export const generatePipelineFromAI = (prompt) =>
  api.post('/api/ai/generate-pipeline', { prompt }, { timeout: 120000 }).then(r => r.data);
