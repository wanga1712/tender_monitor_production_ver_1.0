import os
import requests
import zipfile
import shutil
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse
from database_work.database_connection import DatabaseManager
from utils.logger_config import get_logger

logger = get_logger()

class Downloader:
    def __init__(self, download_dir: str = "temp_downloads"):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.db_manager = DatabaseManager()

    def get_links(self, contract_reg_number: str, table_source: str) -> List[str]:
        """
        Retrieves documentation links for a contract from the database.
        """
        links = []
        is_44 = "44_fz" in table_source
        
        # Determine the correct tables
        if is_44:
            contract_table = table_source # e.g. reestr_contract_44_fz
            links_table = "links_documentation_44_fz"
        else:
            contract_table = table_source # e.g. reestr_contract_223_fz
            links_table = "links_documentation_223_fz"

        try:
            with self.db_manager.connection.cursor() as cursor:
                # 1. Get contract ID
                # Note: table_source might be 'reestr_contract_44_fz_awarded', so we query that table.
                # Assuming contract_number is unique enough or we trust the source.
                query_id = f"SELECT id FROM {contract_table} WHERE contract_number = %s"
                cursor.execute(query_id, (contract_reg_number,))
                result = cursor.fetchone()
                
                if not result:
                    logger.warning(f"Contract {contract_reg_number} not found in {contract_table}")
                    return []
                
                contract_id = result[0]

                # 2. Get links
                query_links = f"SELECT link FROM {links_table} WHERE contract_id = %s"
                cursor.execute(query_links, (contract_id,))
                rows = cursor.fetchall()
                
                links = [row[0] for row in rows if row[0]]
                
        except Exception as e:
            logger.error(f"Error fetching links for {contract_reg_number}: {e}")
            # Don't raise here, just return empty list so we can handle it gracefully
            return []

        return links

    def download_and_extract(self, task_id: int, links: List[str]) -> List[Path]:
        """
        Downloads files from links and extracts archives.
        Returns a list of paths to processable files.
        """
        task_dir = self.download_dir / str(task_id)
        if task_dir.exists():
            shutil.rmtree(task_dir)
        task_dir.mkdir(parents=True)

        processed_files = []

        for link in links:
            try:
                # Handle empty links or invalid formats
                if not link or not link.startswith('http'):
                    continue

                filename = self._get_filename_from_url(link)
                file_path = task_dir / filename
                
                logger.info(f"Downloading {link} to {file_path}")
                
                # Download
                # Add headers to mimic browser if needed, or use existing helpers
                response = requests.get(link, stream=True, verify=False, timeout=30)
                response.raise_for_status()
                
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                # Check if it's an archive
                if zipfile.is_zipfile(file_path):
                    logger.info(f"Extracting {file_path}")
                    extract_dir = task_dir / f"extracted_{file_path.stem}"
                    extract_dir.mkdir(exist_ok=True)
                    try:
                        with zipfile.ZipFile(file_path, 'r') as zip_ref:
                            zip_ref.extractall(extract_dir)
                        
                        # Add extracted files
                        for root, _, files in os.walk(extract_dir):
                            for file in files:
                                processed_files.append(Path(root) / file)
                        
                        # Remove the original zip to save space/confusion? 
                        # Or keep it? Let's keep for now or delete if successful.
                        file_path.unlink() 
                    except zipfile.BadZipFile:
                        logger.error(f"Bad zip file: {link}")
                        # Treat as regular file if extraction fails? No, it's corrupted zip.
                else:
                    processed_files.append(file_path)

            except Exception as e:
                logger.error(f"Failed to download/process {link}: {e}")
                continue

        return processed_files

    def _get_filename_from_url(self, url: str) -> str:
        parsed = urlparse(url)
        path = parsed.path
        filename = os.path.basename(path)
        if not filename:
            filename = "document.bin"
        return filename

    def cleanup(self, task_id: int):
        """Removes the temporary directory for a task."""
        task_dir = self.download_dir / str(task_id)
        if task_dir.exists():
            try:
                shutil.rmtree(task_dir)
            except Exception as e:
                logger.error(f"Error cleaning up {task_dir}: {e}")

    def close(self):
        self.db_manager.close()
