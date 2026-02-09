import time
import argparse
import signal
import sys
from pathlib import Path
from typing import NoReturn

# Add project root to path
import sys
sys.path.append(str(Path(__file__).parent.parent))

from document_processor.queue_manager import QueueManager
from document_processor.downloader import Downloader
from document_processor.parsers import ParserFactory
from document_processor.matcher import KeywordMatcher
from utils.logger_config import get_logger

logger = get_logger()

class DocumentProcessorDaemon:
    def __init__(self, worker_id: int, batch_size: int = 10, pdf_page_limit: int = 1):
        self.worker_id = worker_id
        self.batch_size = batch_size
        self.running = True
        
        # Components
        self.queue_manager = QueueManager(worker_id)
        self.downloader = Downloader(download_dir=f"temp_downloads_worker_{worker_id}")
        self.parser_factory = ParserFactory(pdf_page_limit=pdf_page_limit)
        self.matcher = KeywordMatcher()
        
        # Handle signals
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum, frame):
        logger.info("Shutdown signal received. Stopping...")
        self.running = False

    def run(self):
        logger.info(f"Starting Document Processor Daemon (Worker {self.worker_id})")
        
        # Initial population of queue (only if worker 1/server?)
        # Or let's say both can try, but it's better if one does it or it's a separate process.
        # User said: "воркер 1 это срервер". Let's assume worker 1 populates.
        if self.worker_id == 1:
            logger.info("Worker 1: Checking for new contracts to add to queue...")
            self.queue_manager.populate_queue()

        while self.running:
            try:
                # 1. Get batch
                tasks = self.queue_manager.get_next_batch(self.batch_size)
                
                if not tasks:
                    logger.info("No tasks available. Sleeping for 60 seconds...")
                    # Periodically repopulate queue if worker 1
                    if self.worker_id == 1:
                        self.queue_manager.populate_queue()
                    
                    # Sleep in small chunks to allow quick shutdown
                    for _ in range(12): # 12 * 5s = 60s
                        if not self.running: break
                        time.sleep(5)
                    continue

                # 2. Process batch
                for task in tasks:
                    if not self.running: break
                    
                    task_id = task['id']
                    contract_reg_number = task['contract_reg_number']
                    table_source = task['table_source']
                    
                    logger.info(f"Processing Task {task_id}: {contract_reg_number} from {table_source}")
                    
                    try:
                        # a. Get links
                        links = self.downloader.get_links(contract_reg_number, table_source)
                        
                        if not links:
                            logger.warning(f"No documentation links found for {contract_reg_number}")
                            self.queue_manager.mark_error(task_id, "No links found")
                            continue
                            
                        # b. Download and extract
                        files = self.downloader.download_and_extract(task_id, links)
                        
                        if not files:
                            logger.warning(f"No files downloaded/extracted for {contract_reg_number}")
                            self.queue_manager.mark_error(task_id, "No files downloaded")
                            continue
                            
                        # c. Parse and Match
                        matches_found = 0
                        for file_path in files:
                            parser = self.parser_factory.get_parser(file_path)
                            if not parser:
                                continue
                                
                            logger.info(f"Parsing {file_path.name}...")
                            text = parser.parse(file_path)
                            
                            if text:
                                matches = self.matcher.find_matches(text)
                                if matches:
                                    self.matcher.save_matches(task_id, file_path.name, matches)
                                    matches_found += len(matches)
                        
                        # d. Mark complete
                        self.queue_manager.mark_completed(task_id)
                        logger.info(f"Task {task_id} completed. Matches found: {matches_found}")
                        
                    except Exception as e:
                        logger.error(f"Error processing task {task_id}: {e}")
                        self.queue_manager.mark_error(task_id, str(e))
                    finally:
                        # e. Cleanup
                        self.downloader.cleanup(task_id)

            except Exception as e:
                logger.error(f"Unexpected error in daemon loop: {e}")
                time.sleep(10)

        logger.info("Daemon stopped.")
        self.cleanup()

    def cleanup(self):
        self.queue_manager.close()
        self.downloader.close()
        self.matcher.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Document Processor Daemon")
    parser.add_argument("--worker-id", type=int, required=True, help="Worker ID (1=Server, 2=Local)")
    parser.add_argument("--batch-size", type=int, default=10, help="Number of tasks to process per batch")
    parser.add_argument("--pdf-page-limit", type=int, default=1, help="Limit pages for PDF parsing")
    
    args = parser.parse_args()
    
    daemon = DocumentProcessorDaemon(
        worker_id=args.worker_id,
        batch_size=args.batch_size,
        pdf_page_limit=args.pdf_page_limit
    )
    daemon.run()
