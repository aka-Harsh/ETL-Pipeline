const SOURCE_TYPES = ['S3_SOURCE', 'KAFKA_SOURCE'];
const SINK_TYPES = ['POSTGRES_SINK', 'S3_SINK', 'KAFKA_SINK'];

function getNodeCategory(nodeType) {
  if (SOURCE_TYPES.includes(nodeType)) return 'SOURCE';
  if (SINK_TYPES.includes(nodeType)) return 'SINK';
  return 'TRANSFORM';
}

/**
 * Returns true if the connection from source node to target node is valid.
 * Rules:
 *   - Sinks cannot be sources of connections (no outgoing edges from sinks)
 *   - Sources cannot be targets of connections (no incoming edges to sources)
 */
export function isValidConnection(connection, nodes) {
  const sourceNode = nodes.find(n => n.id === connection.source);
  const targetNode = nodes.find(n => n.id === connection.target);
  if (!sourceNode || !targetNode) return false;

  const sourceType = sourceNode.data?.nodeType || sourceNode.type;
  const targetType = targetNode.data?.nodeType || targetNode.type;
  const sourceCat = getNodeCategory(sourceType);
  const targetCat = getNodeCategory(targetType);

  if (sourceCat === 'SINK') return false;   // sinks can't be sources
  if (targetCat === 'SOURCE') return false; // sources can't be targets
  if (connection.source === connection.target) return false; // no self-loops

  return true;
}
