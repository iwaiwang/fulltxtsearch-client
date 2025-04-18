from elasticsearch import ESClient
from file_watcher import start_watching
import os


def main():
    es_client = ESClient()
    es_client.create_index()
    pdf_directory = "./pdf_files"
    es_client.index_directory(pdf_directory)
    start_watching(pdf_directory, es_client)

if __name__ == "__main__":
    main()
