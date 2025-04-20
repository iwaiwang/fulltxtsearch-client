from flask import Flask, request, render_template, send_file
from pdf_elasticsearch_indexer_with_pages import PDFElasticsearchIndexer
import os

app = Flask(__name__)

# 初始化索引器
INDEXER = PDFElasticsearchIndexer(es_host='localhost', es_port=9200, index_name='pdf_documents')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        query = request.form.get('query', '')
        results = INDEXER.search(query, size=10)
        return render_template('results.html', query=query, results=results)
    return render_template('index.html')

@app.route('/pdf/<path:pdf_path>')
def serve_pdf(pdf_path):
    return send_file(pdf_path)

if __name__ == '__main__':
    # 可选：在启动时索引PDF目录
    PDF_DIRECTORY = './pdf_files'
    INDEXER.index_directory(PDF_DIRECTORY)
    app.run(debug=True)