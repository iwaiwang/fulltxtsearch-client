from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import os

class PDFHandler(FileSystemEventHandler):
    def __init__(self, es_client, pdf_processor):
        self.es_client = es_client

    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".pdf"):
            return
        self._process_file(event.src_path)

    def _process_file(self, file_path):
        # Check if file is already indexed
        res = self.es_client.es.search(
            index=self.es_client.index_name,
            body={"query": {"term": {"filename": file_path}}}
        )
        if res["hits"]["total"]["value"] > 0:
            return

        # Index new file
        self.es_client.index_pdf(file_path)

def start_watching(directory, es_client):
    event_handler = PDFHandler(es_client)
    observer = Observer()
    observer.schedule(event_handler, directory, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
