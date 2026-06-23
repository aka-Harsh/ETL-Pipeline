import React from 'react';
import BaseNode from './BaseNode';
export default function KafkaSinkNode(props) {
  return <BaseNode {...props} showSourceHandle={false} />;
}
