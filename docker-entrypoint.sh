#!/bin/bash
set -euo pipefail

# Wait for HDFS NameNode
until hdfs dfs -test -e /; do
  echo "Waiting for HDFS NameNode (hdfs://namenode:9000)..."
  sleep 5
done

# Create required directories with open permissions
hdfs dfs -mkdir -p /output/events /checkpoints/kafka_to_hdfs
hdfs dfs -chmod -R 777 /output /checkpoints

echo "HDFS ready – starting Spark job..."

# Execute the original command (passed via docker-compose command:)
exec "$@"