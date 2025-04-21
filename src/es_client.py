import logging
import json

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError, RequestError # 导入 ConnectionError 和 RequestError 以便更好处理
# ... 其他导入和日志设置
# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



class ESClient:
    def __init__(self, config_path="config/es_config.json"):
        self.es = None
        self.index_name = None

        try:
            with open(config_path, encoding='utf-8') as f:
                config = json.load(f)

            required_keys = ["host", "user", "password", "index_name"]
            print(config)
            if not all(key in config for key in required_keys):
                 raise KeyError(f"Config file missing required keys. Needs: {required_keys}")

            self.index_name = config["index_name"]

            logger.info(f"Attempting to connect to Elasticsearch at {config['host']}...")
            self.es = Elasticsearch(
                hosts=config["host"],
                # 修正 DeprecationWarning,必须要用http_auth
                http_auth=(config["user"], config["password"]),
                # === 解决 SSL 证书验证失败的核心代码 ===
                # 警告: 在生产环境或安全敏感环境不推荐使用此设置
                verify_certs=False,
                ssl_show_warn=False # 可选：禁用 SSL 警告
                # ======================================
            )

            if not self.es.ping():
                 # ping 失败可能是连接问题，也可能是认证/授权问题
                 # 更详细的错误判断需要查看 ping 失败的具体异常
                 raise ESConnectionError("Ping to Elasticsearch failed. Check connectivity and credentials.")

            logger.info("Successfully connected to Elasticsearch.")

        except FileNotFoundError:
             logger.error(f"Config file not found: {config_path}")
             # raise
        except json.JSONDecodeError:
             logger.error(f"Error decoding JSON from config file: {config_path}")
             # raise
        except KeyError as e:
             logger.error(f"Missing key in config file: {e}")
             # raise
        except ESConnectionError as e: # 使用导入的 ConnectionError
             logger.error(f"Failed to connect or ping Elasticsearch: {e}")
             # raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during ES client initialization: {e}")
            # raise

    def create_index(self):
        """创建Elasticsearch索引（如果不存在）"""
        mapping = {
            'mappings': {
                'properties': {
                    '患者名': {'type': 'text'},
                    '住院号': {'type': 'long'},
                    '住院日期': {'type': 'date'},
                    '出院日期': {'type': 'date'},
                    '文件目录': {'type': 'text'},
                    '文件名称': {'type': 'text'},
                    '页号': {'type': 'long'},
                    '页内容': {
                        'type': 'text',
                        'analyzer': 'ik_max_word',
                        'search_analyzer': 'ik_smart'
                    }
                }
            }
        }
        try:
            if not self.es.indices.exists(index=self.index_name):
                self.es.indices.create(index=self.index_name, body=mapping)
                logger.info(f"Created index: {self.index_name}")
            else:
                logger.info(f"Index {self.index_name} already exists")
        except RequestError as e:
            logger.error(f"Error creating index: {e}")
            raise

    def search(self, query, size=10):
        """搜索Elasticsearch中的内容，返回页面信息"""
        search_body = {
            'query': {
                'match': {
                    '页内容': query
                }
            },
            'highlight': {
                'fields': {
                    '页内容': {
                        'pre_tags': ['<mark>'],
                        'post_tags': ['</mark>'],
                        'fragment_size': 200,
                        'number_of_fragments': 1
                    }
                }
            },
            '_source': ['文件名称', '页号', '页内容'],  # 仅返回必要字段
            'size': size
        }
        try:
            response = self.es.search(index=self.index_name, body=search_body)
            results = []
            for hit in response['hits']['hits']:
                highlight = hit.get('highlight', {}).get('页内容', [''])[0]
                results.append({
                    '文件名称': hit['_source']['文件名称'],
                    '页号': hit['_source']['页号'],
                    '页内容': hit['_source']['页内容'],
                    'score': hit['_score'],
                    'content_snippet': highlight or ''
                })
            return results
        except Exception as e:
            logger.error(f"Search error: {e}")
            return []




def main():
    es_client = ESClient()
    logger.info("Starting PDF processing")
    es_client.create_index()



if __name__ == "__main__":
    main()