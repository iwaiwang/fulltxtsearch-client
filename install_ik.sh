#!/bin/bash



# 执行插件安装命令
echo "Installing analysis-ik plugin..."
bin/opensearch-plugin install -b https://release.infinilabs.com/analysis-ik/stable/opensearch-analysis-ik-2.18.0.zip


# 保持容器运行，执行 OpenSearch 的主入口点脚本
echo "Executing OpenSearch entrypoint..."
# 更正入口点脚本路径
exec /usr/share/opensearch/opensearch-docker-entrypoint.sh opensearch