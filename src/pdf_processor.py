import pdfplumber
import os
import logging


# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PDFProcessor:
    """处理PDF文件的类，提取文本内容并获取文件元数据"""
    @staticmethod
    def get_file_metadata(file_path):
        return {
            "last_modified": os.path.getmtime(file_path),
            "file_path": file_path
        }
    
    @staticmethod
    def extract_pdf_content(pdf_path):
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
