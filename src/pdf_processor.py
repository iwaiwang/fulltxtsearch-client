import pdfplumber
import os
import logging
from search_model import SearchModel
from opensearch_client import OSClient
from opensearchpy import OpenSearch, helpers # <-- 导入 helpers
from pypdf import PdfReader # 从 pypdf 导入 PdfReader

# 设置日志
#logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PDFProcessor:
    """处理PDF文件的类，提取文本内容并获取文件元数据"""
    def __init__(self, os_client):
        self.os_client = os_client
    def extract_pdf_content_pypdf(self, pdf_path):
        """使用 pypdf 提取PDF文件每页的文本内容"""
        pages = []
        try:
            # 创建 PdfReader 对象
            reader = PdfReader(pdf_path)
            # 检查 PDF 是否加密且无法打开
            if reader.is_encrypted:
                 logger.warning(f"PDF file is encrypted and cannot be read: {pdf_path}")
                 # 尝试用空密码打开，如果失败则需要用户提供密码
                 try:
                     reader.decrypt('') # 尝试用空密码解密
                 except Exception:
                      logger.error(f"Failed to decrypt PDF file: {pdf_path}. It might require a password.")
                      return [] # 无法解密，返回空列表

            # 遍历每一页
            for page_num, page in enumerate(reader.pages, start=1):
                try:
                    # 提取文本
                    text = page.extract_text()
                    # 检查提取到的文本是否有效
                    if text and text.strip(): # 确保不是 None 或空字符串
                        pages.append({
                            '页号': page_num,
                            '页内容': text.strip()
                        })
                except Exception as page_e:
                     logger.error(f"Error extracting text from page {page_num} of {pdf_path}: {page_e}")
                     # 可以选择跳过这一页或将该页内容标记为错误

            if not pages:
                 logger.warning(f"No text content extracted from {pdf_path}. It might be an image-based PDF.")

            return pages

        except FileNotFoundError:
            logger.error(f"PDF file not found: {pdf_path}")
            return []
        except Exception as e:
            logger.error(f"Error processing PDF file {pdf_path} with pypdf: {e}")
            return []

        
    def index_pdf(self, pdf_path):
        """将PDF文件的每页内容批量索引到Elasticsearch"""
        logger.info(f"Indexing PDF: {pdf_path}")
        esmodel = SearchModel()
        esmodel.parse_fname(pdf_path,os.path.basename(pdf_path))
        pages = self.extract_pdf_content_pypdf(pdf_path)
        if not pages:
            logger.error(f"Failed to extract content from PDF: {pdf_path}")
            return False

        documents_for_bulk = []
        for page in pages:
            # 1. 获取实例的变量名称和值
            # vars(model_instance) 返回一个字典，键是属性名，值是属性值
            document_data = vars(esmodel)

            # 移除我们不希望作为文档字段直接存储的属性，例如 doc_id，因为它用作 Elasticsearch 的 _id
            # 创建一个新的字典，不包含 doc_id
            source_data = {key: value for key, value in document_data.items() if key != '页号' or key != '页内容'}
            source_data['页号'] = page['页号']
            source_data['页内容'] = page['页内容']

            # 2. 构建符合 Elasticsearch 批量索引格式的字典
            bulk_item = {
                "_index": self.os_client.index_name, # 使用实例的索引名称
                "_source": source_data # 将变量名称和值构成的字典作为文档源数据
                # 如果你想确保是创建新文档而不是更新，可以添加 "_op_type": "create"
                # "_op_type": "create"
            }
            documents_for_bulk.append(bulk_item)

        try:
            # bulk(self.es, actions)
            #opensearchpy.helpers.bulk(self.es, documents_for_bulk)
            success_count, errors = helpers.bulk(self.os_client.os, documents_for_bulk)
            if errors:
                logger.error(f"Bulk indexing for {pdf_path} finished with errors. Success count: {success_count}")
                # 您可能需要进一步检查 errors 列表以查看具体哪些文档索引失败了
                # logger.error(f"Bulk errors: {errors}") # 注意：errors 可能很大，谨慎打印
            else:
                logger.info(f"Successfully bulk indexed {success_count} documents from {pdf_path}")

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


def main():
    # 配置
    PDF_DIRECTORY = './pdf_files'  # 替换为你的PDF文件目录

    # 初始化索引器
    indexer = OSClient()
    indexer.create_index()

    # 索引目录中的所有PDF文件
    pdf_processor = PDFProcessor(indexer)
    pdf_processor.index_directory(PDF_DIRECTORY)
    
    # 示例搜索
    query = "住院"  # 替换为你的搜索关键词
    logger.info(f"Searching for: {query}")
    results = indexer.search(query)
    
    # 打印搜索结果
    for result in results:
        print(f"\nFile: {result['文件名称']}")
        print(f"Path: {result['文件目录']}")
        print(f"Page: {result['页号']}")
        print(f"Score: {result['score']}")
        print(f"Content Snippet: {result['content_snippet']}")

if __name__ == '__main__':
    main()