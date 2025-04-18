import os
import pdfplumber
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import RequestError
import logging
import json

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ESClient:
    def __init__(self, config_path="config/es_config.json"):
        """初始化Elasticsearch客户端和索引配置"""

        with open(config_path) as f:
            config = json.load(f)
        self.es = Elasticsearch(
            hosts=config["host"],
            port=config["port"],
            scheme=config["scheme"],
            http_auth=(config["user"], config["password"])
        )
        self.index_name = config["index_name"]

    def _create_index(self):
        """创建Elasticsearch索引（如果不存在）"""
        mapping = {
            'mappings': {
                'properties': {
                    'filename': {'type': 'keyword'},
                    'path': {'type': 'keyword'},
                    'page_number': {'type': 'integer'},
                    'content': {'type': 'text'}
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

    def extract_pdf_content(self, pdf_path):
        """提取PDF文件每页的文本内容"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                pages = []
                for page_num, page in enumerate(pdf.pages, start=1):
                    text = page.extract_text() or ''
                    if text.strip():
                        pages.append({
                            'page_number': page_num,
                            'content': text.strip()
                        })
                return pages
        except Exception as e:
            logger.error(f"Error extracting content from {pdf_path}: {e}")
            return []

    def index_pdf(self, pdf_path):
        """将PDF文件的每页内容批量索引到Elasticsearch"""
        pages = self.extract_pdf_content(pdf_path)
        if not pages:
            return False

        actions = [
            {
                '_index': self.index_name,
                '_source': {
                    'filename': os.path.basename(pdf_path),
                    'path': pdf_path,
                    'page_number': page['page_number'],
                    'content': page['content']
                }
            }
            for page in pages
        ]

        try:
            helpers.bulk(self.es, actions)
            logger.info(f"Indexed {len(pages)} pages of {pdf_path}")
            return True
        except Exception as e:
            logger.error(f"Error bulk indexing {pdf_path}: {e}")
            return False

    def index_directory(self, directory):
        """索引指定目录中的所有PDF文件"""
        if not os.path.isdir(directory):
            logger.error(f"Directory {directory} does not exist")
            return

        for root, _, files in os.walk(directory):
            for file in files:
                if file.lower().endswith('.pdf'):
                    pdf_path = os.path.join(root, file)
                    self.index_pdf(pdf_path)

    def search(self, query, size=10):
        """搜索Elasticsearch中的内容，返回页面信息"""
        search_body = {
            'query': {
                'match': {
                    'content': query
                }
            },
            'highlight': {
                'fields': {
                    'content': {
                        'pre_tags': ['<mark>'],
                        'post_tags': ['</mark>'],
                        'fragment_size': 200,
                        'number_of_fragments': 1
                    }
                }
            },
            '_source': ['filename', 'path', 'page_number'],  # 仅返回必要字段
            'size': size
        }
        try:
            response = self.es.search(index=self.index_name, body=search_body)
            results = []
            for hit in response['hits']['hits']:
                highlight = hit.get('highlight', {}).get('content', [''])[0]
                results.append({
                    'filename': hit['_source']['filename'],
                    'path': hit['_source']['path'],
                    'page_number': hit['_source']['page_number'],
                    'score': hit['_score'],
                    'content_snippet': highlight or ''
                })
            return results
        except Exception as e:
            logger.error(f"Search error: {e}")
            return []

def main():
    # 配置
    PDF_DIRECTORY = './pdf_files'  # 替换为你的PDF文件目录

    # 初始化索引器
    indexer = ESClient()
    indexer._create_index()

    # 索引目录中的所有PDF文件
    logger.info(f"Starting to index PDFs in {PDF_DIRECTORY}")
    indexer.index_directory(PDF_DIRECTORY)
    logger.info("Indexing completed")

    # 示例搜索
    query = "example keyword"  # 替换为你的搜索关键词
    logger.info(f"Searching for: {query}")
    results = indexer.search(query)
    
    # 打印搜索结果
    for result in results:
        print(f"\nFile: {result['filename']}")
        print(f"Path: {result['path']}")
        print(f"Page: {result['page_number']}")
        print(f"Score: {result['score']}")
        print(f"Content Snippet: {result['content_snippet']}")

if __name__ == '__main__':
    main()