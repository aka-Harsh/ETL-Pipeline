import { useState, useEffect, useRef } from 'react';
import { getPipelineStatus } from '../api/pipelineApi';

/**
 * Polls the pipeline execution status every 2 seconds while running.
 * Stops automatically when status reaches COMPLETED or FAILED.
 */
export function useExecutionStatus(pipelineId, executionId, isRunning) {
  const [status, setStatus] = useState(null);
  const [nodeStatuses, setNodeStatuses] = useState({});
  const [error, setError] = useState(null);
  const intervalRef = useRef(null);

  useEffect(() => {
    if (!pipelineId || !executionId || !isRunning) {
      clearInterval(intervalRef.current);
      return;
    }

    const poll = async () => {
      try {
        const data = await getPipelineStatus(pipelineId, executionId);
        setStatus(data.overallStatus);
        setNodeStatuses(data.nodeStatuses || {});
        setError(null);
        if (data.overallStatus === 'COMPLETED' || data.overallStatus === 'FAILED') {
          clearInterval(intervalRef.current);
        }
      } catch (err) {
        setError(err.message);
      }
    };

    poll(); // immediate first call
    intervalRef.current = setInterval(poll, 2000);
    return () => clearInterval(intervalRef.current);
  }, [pipelineId, executionId, isRunning]);

  return { status, nodeStatuses, error };
}
