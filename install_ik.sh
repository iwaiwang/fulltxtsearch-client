#!/bin/bash

# 等待 OpenSearch 启动 (可以根据需要调整等待逻辑)
echo "Waiting for OpenSearch to start..."
until curl -s http://localhost:9200/_cluster/health?wait_for_status=yellow | grep '"status":"yellow"'; do
  sleep 5
  echo "Still waiting..."
done
echo "OpenSearch started."

# 执行插件安装命令
echo "Installing analysis-ik plugin..."
bin/opensearch-plugin install https://release.infinilabs.com/analysis-ik/stable/opensearch-analysis-ik-2.18.0.zip

# 保持容器运行 (这是关键，否则脚本执行完容器就退出了)
# 您可以让它执行 OpenSearch 的启动命令，或者是一个无限循环
echo "Starting OpenSearch..."
opensearch # 或者 exec opensearch