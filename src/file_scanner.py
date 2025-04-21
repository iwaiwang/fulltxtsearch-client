# file_scanner.py
import os
import time
import logging
from db_manager import IndexedFileManager
# 假设您的 OpenSearch 客户端和 PDF 处理器类在这里或可以被导入
# from opensearch_client import OSClient
# from pdf_processor import PDFProcessor


logger = logging.getLogger(__name__)

class FileScanner:
    def __init__(self, db_manager: IndexedFileManager, pdf_processor): # Pass the PDFProcessor instance
        self.db_manager = db_manager
        self.pdf_processor = pdf_processor # Needs an instance of PDFProcessor
        self._stop_scanning = False

    def scan_and_index_directory(self, directory_path):
        """
        扫描指定目录下的所有 PDF 文件，检查其索引状态并进行索引。

        Args:
            directory_path (str): 要扫描的根目录路径。
        """
        if not os.path.isdir(directory_path):
            logger.error(f"Scan directory does not exist: {directory_path}")
            return

        logger.info(f"Starting scan and index for directory: {directory_path}")
        processed_count = 0
        skipped_count = 0
        error_count = 0

        # TODO: Add logic here to handle potential deletion of files from DB/OpenSearch
        # based on files that are in the DB but not found on the filesystem.
        # This requires getting all files from DB first, then iterating files on disk,
        # and finally checking which DB files were not encountered.

        for root, _, files in os.walk(directory_path):
            if self._stop_scanning:
                logger.info("Scanning stopped by user request.")
                break

            for file in files:
                if self._stop_scanning:
                     logger.info("Scanning stopped by user request.")
                     break

                if file.lower().endswith('.pdf'):
                    pdf_path = os.path.join(root, file)

                    try:
                        # 获取文件的最后修改时间
                        modification_time = os.path.getmtime(pdf_path)

                        # 检查文件是否需要索引（新文件或修改文件）
                        if not self.db_manager.is_indexed(pdf_path, modification_time):
                            logger.info(f"Processing file: {pdf_path}")

                            # --- 在这里调用 PDF 文本提取和 OpenSearch 索引逻辑 ---
                            # 这部分应该由传入的 pdf_processor 实例来完成
                            # pdf_processor.index_pdf(pdf_path) 方法应该包含提取和bulk索引的逻辑
                            # 您可能需要从文件名解析患者信息等，这取决于您的 SearchDocument 结构

                            # 示例调用 (假设 pdf_processor 有 index_pdf 方法)
                            # 您可能需要根据实际情况调整 index_pdf 的参数
                            # 例如，如果 patient_info 需要从文件名解析，可以在这里处理
                            # patient_info = self._parse_patient_info_from_path(pdf_path) # 实现这个方法
                            # success = self.pdf_processor.index_pdf(pdf_path, patient_info) # 传递patient_info

                            # 简化的调用，假设 index_pdf 自己处理从文件名获取 info 或只需要 path
                            success = self.pdf_processor.index_pdf(pdf_path)
                            if success:
                                # 索引成功后，标记文件为已索引
                                self.db_manager.mark_as_indexed(pdf_path, modification_time)
                                processed_count += 1
                            else:
                                logger.error(f"Failed to index file: {pdf_path}")
                                error_count += 1
                        else:
                            # 文件已索引且未修改，跳过
                            # logger.debug(f"Skipping already indexed file: {pdf_path}")
                            skipped_count += 1

                    except FileNotFoundError:
                        # 文件在扫描后但在处理前被删除，跳过
                        logger.warning(f"File not found during processing (might have been deleted): {pdf_path}")
                        # 可以选择从数据库中移除此记录 if needed
                        # self.db_manager.remove_indexed_record(pdf_path)
                        error_count += 1 # 视为处理过程中的错误/异常情况
                    except Exception as e:
                        # 处理文件时发生其他错误（如PDF解析错误，OpenSearch连接错误等）
                        logger.error(f"Error processing file {pdf_path}: {e}")
                        error_count += 1
                        # 这里的错误处理取决于需求，是否重试、记录失败日志等


        logger.info(f"Scan and index finished for directory: {directory_path}")
        logger.info(f"Processed: {processed_count}, Skipped (already indexed): {skipped_count}, Errors: {error_count}")


    def stop_scanning(self):
        """设置标志以停止正在进行的扫描"""
        self._stop_scanning = True
        logger.info("Stop scanning requested.")

    # TODO: 实现文件路径解析patient_info的方法，如果需要的话
    # def _parse_patient_info_from_path(self, pdf_path):
    #     """
    #     从文件路径或文件名中解析患者相关信息。
    #     需要根据您的文件命名规则来实现。
    #     """
    #     # 示例：从路径中提取文件名和目录
    #     file_name = os.path.basename(pdf_path)
    #     file_directory = os.path.dirname(pdf_path)
    #     # 根据文件名解析患者名、住院号等（这部分是您业务相关的逻辑）
    #     patient_name = "解析失败" # 替换为您的解析逻辑
    #     hospitalization_number = 0 # 替换为您的解析逻辑

    #     return {
    #         '患者名': patient_name,
    #         '住院号': hospitalization_number,
    #         '文件目录': file_directory,
    #         '文件名称': file_name,
    #         # ... 其他需要解析的信息 ...
    #     }


# # 简单的测试
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#     # 模拟一个假的 PDFProcessor 和 OSClient
#     class MockOSClient:
#         def __init__(self):
#             self.index_name = "mock_index"
#             logger.info("MockOSClient initialized")
#         def create_index(self):
#             logger.info("Mock index creation called")
#         def search(self, query):
#             logger.info(f"Mock search called for query: {query}")
#             return []
#         def BulkIndexAsync(self, documents): # Note: Bulk in Nest is async
#             logger.info(f"Mock bulk index called with {len(documents)} documents")
#             # Simulate success
#             return (len(documents), []) # Return success count and empty errors

#     class MockPDFProcessor:
#          def __init__(self, os_client):
#              self.os_client = os_client # Holds the mock OSClient
#              logger.info("MockPDFProcessor initialized")

#          def extract_pdf_content_pypdf(self, pdf_path):
#              """模拟提取文本"""
#              logger.info(f"Mock extracting text from {pdf_path}")
#              if "empty" in pdf_path:
#                  return [] # Simulate empty extraction
#              return [{'页号': 1, '页内容': f"这是来自文件 {pdf_path} 页号 1 的模拟文本"},
#                      {'页号': 2, '页内容': f"这是来自文件 {pdf_path} 页号 2 的模拟文本"}]

#          def index_pdf(self, pdf_path):
#              """模拟索引PDF文件"""
#              logger.info(f"Mock indexing PDF: {pdf_path}")
#              extracted_pages = self.extract_pdf_content_pypdf(pdf_path)
#              if not extracted_pages:
#                  logger.warning(f"Mock extraction failed for {pdf_path}")
#                  return False

#              documents_for_bulk = []
#              # 模拟构建 SearchDocument 结构
#              for page in extracted_pages:
#                  doc_id = f"{pdf_path}:{page['页号']}"
#                  source_data = {
#                      '患者名': '模拟患者',
#                      '住院号': 12345,
#                      '文件名称': os.path.basename(pdf_path),
#                      '页号': page['页号'],
#                      '页内容': page['页内容']
#                  }
#                  documents_for_bulk.append({"_index": self.os_client.index_name, "_id": doc_id, "_source": source_data})

#              # 调用模拟的 BulkIndexAsync 方法 (注意，实际应 await)
#              # 在这个同步测试中，我们直接调用
#              success_count, errors = self.os_client.BulkIndexAsync(documents_for_bulk)

#              if errors:
#                  logger.error(f"Mock bulk indexing failed for {pdf_path}")
#                  return False
#              else:
#                  logger.info(f"Mock bulk indexed {success_count} docs from {pdf_path}")
#                  return True


#     # 设置一个测试目录
#     test_dir = "test_pdf_dir"
#     os.makedirs(test_dir, exist_ok=True)
#     # 创建一些空的 pdf 文件用于测试扫描逻辑
#     open(os.path.join(test_dir, "file_A.pdf"), "w").close()
#     time.sleep(0.1) # 确保文件时间戳不同
#     open(os.path.join(test_dir, "file_B.pdf"), "w").close()
#     time.sleep(0.1)
#     open(os.path.join(test_dir, "file_C_empty.pdf"), "w").close() # Simulate empty content file

#     # 模拟修改 file_A
#     time.sleep(0.5)
#     open(os.path.join(test_dir, "file_A.pdf"), "w").close()

#     # 初始化数据库和扫描器
#     mock_os_client = MockOSClient()
#     mock_pdf_processor = MockPDFProcessor(mock_os_client)
#     db_manager = IndexedFileManager("test_scan.db")
#     scanner = FileScanner(db_manager, mock_pdf_processor)

#     print("\n--- First Scan ---")
#     scanner.scan_and_index_directory(test_dir)
#     print("\n--- Second Scan (should skip most) ---")
#     scanner.scan_and_index_directory(test_dir)

#     # 模拟修改 file_B
#     time.sleep(0.5)
#     open(os.path.join(test_dir, "file_B.pdf"), "w").close()
#     print("\n--- Third Scan (should re-index file_B) ---")
#     scanner.scan_and_index_directory(test_dir)

#     # 清理测试文件和目录
#     # import shutil
#     # shutil.rmtree(test_dir)
#     # os.remove("test_scan.db")