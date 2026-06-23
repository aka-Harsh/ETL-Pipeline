import React from 'react';
import BaseNode from './BaseNode';
export default function PostgresSinkNode(props) {
  return <BaseNode {...props} showSourceHandle={false} />;
}
