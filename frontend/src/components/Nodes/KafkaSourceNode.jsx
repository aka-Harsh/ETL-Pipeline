import React from 'react';
import BaseNode from './BaseNode';
export default function KafkaSourceNode(props) {
  return <BaseNode {...props} showTargetHandle={false} />;
}
