from flask import Flask, request, send_file, jsonify, send_from_directory,Response
from flask_cors import CORS
import os
import opensearchpy
import logging
import datetime
import json # Keep json for logging/debugging
from webdav_client import WebDavClient,OperationFailed
import io # For handling byte streams

# Import the new SettingsManager class
from config import SettingsManager # Assuming the file is config.py

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app)

# --- Logging Configuration (Keep as is for handlers, but get logger after config) ---
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))

log_filename = f"app_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
file_handler = logging.FileHandler(log_filename, mode='w', encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
# Prevent adding handlers multiple times if app is reloaded
if not root_logger.handlers:
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__) # Get logger after configuring handlers

# --- Settings Management ---
# Create an instance of the SettingsManager
settings_manager = SettingsManager()

# Get settings for initialization
app_settings = settings_manager.get_all_settings()

# --- Apply loaded settings to application variables ---
PDF_DIR = app_settings.get('localfile', {}).get('pdf_directory', '/default/pdf/path') # Use .get for safety
INDEX_NAME = app_settings.get('opensearch', {}).get('index_name', 'medical_records') # Use .get for safety

# Initialize OpenSearch client with loaded settings
opensearch_client = None # Initialize as None
opensearch_config = app_settings.get('opensearch', {})# Get WebDAV settings

try:
    opensearch_client = opensearchpy.OpenSearch(
        hosts=opensearch_config['host'],
        http_auth=(opensearch_config['user'], opensearch_config['password']),
        use_ssl=False,
        verify_certs=False, # Consider setting this to True in production with proper CA certs
        ssl_assert_hostname=False,
        ssl_show_warn=False, # Set to True in production for warnings
        timeout=60,  # Increase timeout to 30 seconds
        max_retries=3,  # Retry failed requests
        retry_on_timeout=True
    )
    # Test connection
    if not opensearch_client.ping():
            logger.error("Failed to connect to OpenSearch. Check settings.json and OpenSearch status.")
            opensearch_client = None # Set to None if connection fails
    else:
            logger.info("Successfully connected to OpenSearch.")
except Exception as e:
        logger.error(f"Error initializing OpenSearch client: {e}")
        opensearch_client = None # Set to None on initialization error


# --- Routes ---

# Render frontend page
@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

# Search API
@app.route('/api/search', methods=['GET'])
def search():
    # Check if opensearch client is initialized and connected
    if opensearch_client is None:
         return jsonify({'error': 'OpenSearch client not initialized or connected. Check server logs and settings.json.'}), 500

    query = request.args.get('query', '')
    file_type = request.args.get('file_type', '')
    patient_name = request.args.get('patient_name', '')
    hospital_id = request.args.get('hospital_id', '')
    admission_date_start = request.args.get('admission_date_start', '')
    admission_date_end = request.args.get('admission_date_end', '')
    discharge_date_start = request.args.get('discharge_date_start', '')
    discharge_date_end = request.args.get('discharge_date_end', '')

    page = int(request.args.get('page', 1))
    size = int(request.args.get('size', 5))

    if not query and not hospital_id and not patient_name:
        return jsonify({'results': [], 'total': 0, 'page': page, 'size': size, 'total_pages': 0})
    
    from_idx = (page - 1) * size
    search_query = {
        "query": { 
            "bool": {
                "must": [],
                "filter": []
            }
        },
        'from': from_idx,
        'size': size,
        'highlight': {
            'fields': {
                '页内容': {}
            },
            'pre_tags': ['<strong>'],
            'post_tags': ['</strong>']
        }
    } 

    if query:
        logger.info(f"text_query: {search_query}")
        search_query['query']['bool']['must'].append({
            "query_string": {
                "default_field": "页内容",
                "query": query
            }
        })
    else: # 如果搜索文本为空，则搜索所有文件
        search_query['query']['bool']['must'].append({
            "match_all": {}
        })

    if file_type:
        search_query['query']['bool']['filter'].append(
            {
                'term': {
                    '文件类型.keyword': file_type
                }
            }
        )

    if patient_name:
        search_query['query']['bool']['filter'].append(
            {
                'term': {
                    '患者名.keyword': patient_name
                }
            }
        )
    if hospital_id:
            search_query['query']['bool']['filter'].append(
                {
                    'term': {
                        '住院号.keyword': hospital_id
                    }
                }
            )

    if admission_date_start or admission_date_end:
        date_range_filter = {'range': {'入院时间': {}}}
        if admission_date_start:
            date_range_filter['range']['入院时间']['gte'] = admission_date_start
        if admission_date_end:
            date_range_filter['range']['入院时间']['lte'] = admission_date_end
        search_query['query']['bool']['filter'].append(date_range_filter)

    if discharge_date_start or discharge_date_end:
        date_range_filter = {'range': {'出院时间': {}}}
        if discharge_date_start:
            date_range_filter['range']['出院时间']['gte'] = discharge_date_start
        if discharge_date_end:
            date_range_filter['range']['出院时间']['lte'] = discharge_date_end
        search_query['query']['bool']['filter'].append(date_range_filter)


    logger.info(f"Searching with query: {json.dumps(search_query, indent=2, ensure_ascii=False)}")

    try:
        response = opensearch_client.search(
            index=INDEX_NAME,
            body=search_query
        )

        results = []
        for hit in response['hits']['hits']:
            highlight_text = hit.get('highlight', {}).get('页内容')
            display_text = highlight_text[0] if highlight_text else hit['_source'].get('页内容', '')

            results.append({
                'id': hit['_id'],
                'patient': hit['_source'].get('患者名', ''),
                'hospital_id': hit['_source'].get('住院号', ''),
                'admission_date': hit['_source'].get('入院时间', ''),
                'discharge_date': hit['_source'].get('出院时间', ''),
                'doc_type': hit['_source'].get('文件类型', ''),
                'filename': hit['_source'].get('文件名称', ''),
                'page': hit['_source'].get('页号', 0),
                'text': display_text
            })

        total = response['hits']['total']['value'] if isinstance(response['hits']['total'], dict) else response['hits']['total']
        total_pages = (total + size - 1) // size

        logger.info(f"Search successful. Found {total} results.")
        return jsonify({
            'results': results,
            'total': total,
            'page': page,
            'size': size,
            'total_pages': total_pages
        })
    except opensearchpy.exceptions.NotFoundError:
         logger.error(f"Index '{INDEX_NAME}' not found.")
         return jsonify({'error': f"Index '{INDEX_NAME}' not found. Please check settings.json and OpenSearch status."}), 404
    except Exception as e:
        logger.error(f"Error during OpenSearch search: {str(e)}", exc_info=True)
        return jsonify({'error': f'搜索失败: {str(e)}'}), 500



# Get PDF API
@app.route('/api/pdf', methods=['GET'])
def get_pdf():
    filename = request.args.get('filename')
    logger.info(f"Requested PDF: {filename}")
    if not filename:
         logger.warning("PDF request received without filename.")
         return jsonify({'error': '文件名不能为空'}), 400

    #webdav有可能在运行中被修改,所以每次都要加载一下设置
    webdav_settings = settings_manager.get_webdav_settings()
    webdav_enabled = webdav_settings.get('enabled', False)
    webdav_ip = webdav_settings.get('ip')
    webdav_user = webdav_settings.get('user')
    webdav_port = webdav_settings.get('port')
    webdav_password = webdav_settings.get('password')
    webdav_directory = webdav_settings.get('directory', '/') # Default to root if not specified

    # --- WebDAV 有效性判断 ---
    if webdav_enabled and webdav_ip and webdav_user and webdav_password:

        #确保路径以斜杠开头
        path_for_client = f"{webdav_directory.rstrip('/')}/{filename.lstrip('/')}"
        if webdav_directory == '/':
             path_for_client = filename.lstrip('/')
        else:
             path_for_client = f"{webdav_directory.rstrip('/')}/{filename.lstrip('/')}"

        logger.debug(f"Attempting WebDAV download from {webdav_ip} path {path_for_client}")
        try:
            # Connect to the WebDAV server
            webdav = WebDavClient(host=webdav_ip, username=webdav_user, password=webdav_password,protocol='http', port=webdav_port)
            byte_stream = io.BytesIO()
            webdav.download(path_for_client, byte_stream)
            byte_stream.seek(0)  # Rewind the stream for reading

            logger.info(f"Successfully fetched '{filename}' from WebDAV.")
            return send_file(byte_stream, mimetype='application/pdf',download_name=filename)

        except OperationFailed as e:
            logger.error(f" webdav 失败'{e.reason}'  {webdav_ip} path {path_for_client}")
            return jsonify({'error': f'WebDAV: {filename}'}), 404
        except Exception as e:
            logger.error(f"Unexpected error during WebDAV fetch of '{filename}': {e}", exc_info=True)
            return jsonify({'error': f'从WebDAV获取文件时发生未知错误: {str(e)}'}), 500

    logger.info(f"WebDAV没有设置, 下载文件'{filename}', 使用本地的路径: {PDF_DIR}")
    
    if not PDF_DIR or not os.path.isdir(PDF_DIR):
         logger.error("没有发现本地路径")
         return jsonify({'error': 'PDF目录未配置或不存在。请检查settings.json'}), 500

    # 预防路径遍历攻击
    filename = os.path.basename(filename)
    file_path = os.path.join(PDF_DIR, filename)

    if os.path.exists(file_path) and os.path.isfile(file_path):
        logger.info(f"Serving local PDF file: {file_path}")
        return send_file(file_path, mimetype='application/pdf')

    # If neither WebDAV (if enabled) nor local file is found
    logger.warning(f"Local PDF file not found: {file_path}")
    return jsonify({'error': 'PDF not found'}), 404


# Get File Types API
@app.route('/api/file_types', methods=['GET'])
def get_file_types():
    logger.info("Fetching file types from OpenSearch aggregations.")
    try:
        response = opensearch_client.search(index=INDEX_NAME, body={
            'size': 0,
            'aggs': {
                'by_type': {
                    'terms': {
                        'field': '文件类型.keyword',
                        'size': 100
                    }
                }
            }
        })
        file_types = [bucket['key'] for bucket in response.get('aggregations', {}).get('by_type', {}).get('buckets', [])]
        logger.info(f"Found file types: {file_types}")
        return jsonify(file_types)
    except opensearchpy.exceptions.NotFoundError:
         logger.error(f"Index '{INDEX_NAME}' not found when fetching file types.")
         return jsonify({'error': f"Index '{INDEX_NAME}' not found. Cannot fetch file types. Check settings.json"}), 404
    except Exception as e:
        logger.error(f"Error fetching file types: {str(e)}", exc_info=True)
        return jsonify({'error': f'获取文件类型失败: {str(e)}'}), 500

# --- WebDAV Settings APIs ---

@app.route('/api/save_webdav_settings', methods=['POST'])
def save_webdav_settings():
    """Receives and saves WebDAV settings."""
    try:
        data = request.get_json()
        if not data:
            logger.warning("Received empty JSON data for saving settings.")
            return jsonify({'error': 'Invalid JSON data'}), 400

        # Use the settings manager to update and save
        success = settings_manager.update_webdav_settings(data)

        if success:
             current_webdav_settings = settings_manager.get_webdav_settings()
             if current_webdav_settings.get('enabled'):
                  if not current_webdav_settings.get('ip') or \
                     not current_webdav_settings.get('user') or \
                     not current_webdav_settings.get('port') or \
                     not current_webdav_settings.get('password'):
                      logger.warning("Attempted to enable WebDAV with missing required fields AFTER update.")
                      return jsonify({'error': '启用 WebDAV 时，IP地址、用户名和密码不能为空。设置已保存，但可能不完整。'}), 400

             logger.info("WebDAV settings saved successfully via manager.")
             return jsonify({'status': 'success', 'message': 'WebDAV settings saved'})
        else:
             # update_webdav_settings can return False if data format is wrong
             return jsonify({'error': '保存设置失败: 数据格式错误。'}), 400


    except Exception as e:
        logger.error(f"Error saving WebDAV settings via API: {str(e)}", exc_info=True)
        return jsonify({'error': f'保存设置失败: {str(e)}'}), 500

@app.route('/api/get_webdav_settings', methods=['GET'])
def get_webdav_settings():
    """Returns the current WebDAV settings."""
    logger.info("Returning current WebDAV settings via manager.")
    # Use the settings manager to get the webdav section
    return jsonify(settings_manager.get_webdav_settings())


# --- Main execution ---
if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False, port=5001)