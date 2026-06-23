import React from 'react';
import BaseNode from './BaseNode';
export default function S3SinkNode(props) {
  return <BaseNode {...props} showSourceHandle={false} />;
}
