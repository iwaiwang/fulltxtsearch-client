from flask import Flask, request, send_file, jsonify, send_from_directory
from flask_cors import CORS
import os
import opensearchpy  # 假设已安装 opensearch-py
import logging
import datetime

app = Flask(__name__)
CORS(app)  # 允许跨域请求

# OpenSearch 客户端配置
opensearch_client = opensearchpy.OpenSearch(
    hosts='https://localhost:9200',
    http_auth=('admin', '123@QWE#asd'),  # 根据实际配置调整
    use_ssl=False,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False,
)

# PDF 文件存储路径
PDF_DIR = '/Users/john/Data/projects/es_test/test/pdf_files'  # 请替换为实际PDF存储路径
INDEX_NAME='medical_records'

# 日志配置

# 控制台的log
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
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)


# 渲染前端页面
@app.route('/')
def index():
    return send_from_directory('static', 'index.html')
@app.route('/api/search', methods=['GET'])
def search():
    query = request.args.get('query', '')
    file_type = request.args.get('file_type', '')  # 新增：文件类型参数
    patient_name = request.args.get('patient_name', '')  # 新增：患者姓名参数
    hospital_id = request.args.get('hospital_id', '')
    page = int(request.args.get('page', 1))  # 当前页，从1开始
    size = int(request.args.get('size', 10))  # 每页条数，默认10

    if not query:
        return jsonify({'results': [], 'total': 0, 'page': page, 'size': size, 'total_pages': 0})

    # 计算 OpenSearch 的 from 参数
    from_idx = (page - 1) * size

    # OpenSearch 查询
    # 构建 OpenSearch 查询
    search_query = {
        'query': {
            'bool': {
                'must': [
                    {
                        'match_phrase': {
                            '页内容': {
                                'query': query,
                                #'analyzer': 'ik_smart'
                            }
                        }
                    }
                ],
                'filter': [] # 初始化 filter 列表
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
    
    # 如果指定了文件类型，添加 filter
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
                        '住院号': hospital_id
                    }
                }
            )

    logger.info(f"Searching for query: {search_query}")
    try:
        response = opensearch_client.search(
            index=INDEX_NAME,
            body=search_query
        )

        results = []
        for hit in response['hits']['hits']:
            highlight_text = hit.get('highlight', {}).get('页内容', [''])[0] if hit.get('highlight') else hit['_source']['页内容']
            results.append({
                'id': hit['_id'],
                'patient': hit['_source']['患者名'],
                'hospital_id': hit['_source']['住院号'],
                'admission_date': hit['_source']['入院时间'],
                'discharge_date': hit['_source']['出院时间'],
                'doc_type': hit['_source']['文件类型'],
                'filename': hit['_source']['文件名称'],
                'page': hit['_source']['页号'],
                'text': highlight_text
            })

        # 获取总数和计算总页数
        total = response['hits']['total']['value']
        total_pages = (total + size - 1) // size  # 向上取整

        #logger.info(f"Total results: {results}")
        return jsonify({
            'results': results,
            'total': total,
            'page': page,
            'size': size,
            'total_pages': total_pages
        })
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/pdf', methods=['GET'])
def get_pdf():
    # 获取文件名
    filename = request.args.get('filename')
    logger.info(f"Requested PDF: {filename}")
    file_path = os.path.join(PDF_DIR, filename)
    if os.path.exists(file_path):
        return send_file(file_path, mimetype='application/pdf')
    return jsonify({'error': 'PDF not found'}), 404


@app.route('/api/file_types', methods=['GET'])
def get_file_types():
    #获取opensearch中所有文件类型
    response = opensearch_client.search(index=INDEX_NAME, body={
        'aggs': {
            'by_type': {
                'terms': {
                    'field': '文件类型.keyword',
                    'size': 100
                }
            }
        }
    })
    file_types = [bucket['key'] for bucket in response['aggregations']['by_type']['buckets']]
    return jsonify(file_types)

if __name__ == '__main__':
    app.run(debug=True,port=5001)