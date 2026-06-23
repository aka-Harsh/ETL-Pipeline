import React from 'react';

export default function NodeStatusBadge({ status }) {
  return (
    <span className={`status-badge ${status || 'IDLE'}`}>
      {status || 'IDLE'}
    </span>
  );
}
