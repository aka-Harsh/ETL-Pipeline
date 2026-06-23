import React from 'react';
import BaseNode from './BaseNode';
export default function S3SourceNode(props) {
  return <BaseNode {...props} showTargetHandle={false} />;
}
