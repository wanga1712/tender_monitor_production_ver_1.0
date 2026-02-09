import os
from datetime import datetime
from typing import List, Dict, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from database_work.database_connection import DatabaseManager
from utils.logger_config import get_logger

logger = get_logger()

class QueueManager:
    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.db_manager = DatabaseManager()

    def populate_queue(self):
        """
        Populates the processing queue with new contracts from source tables.
        Only adds contracts that are not already in the queue.
        """
        source_tables = [
            "reestr_contract_44_fz",
            "reestr_contract_44_fz_awarded",
            "reestr_contract_223_fz",
            "reestr_contract_223_fz_awarded"
        ]

        try:
            with self.db_manager.connection.cursor() as cursor:
                for table in source_tables:
                    # Select contracts that are not in the queue
                    # Assuming 'contract_number' is the unique identifier in source tables
                    # and 'contract_reg_number' in queue table.
                    
                    # We need to be careful about column names. 
                    # Based on existing code, source tables have 'contract_number'.
                    
                    query = f"""
                        INSERT INTO document_processing_queue (contract_reg_number, table_source, status)
                        SELECT t.contract_number, '{table}', 'pending'
                        FROM {table} t
                        LEFT JOIN document_processing_queue q 
                        ON t.contract_number = q.contract_reg_number AND q.table_source = '{table}'
                        WHERE q.id IS NULL
                        LIMIT 1000;  -- Process in chunks to avoid massive transactions
                    """
                    cursor.execute(query)
                    if cursor.rowcount > 0:
                        logger.info(f"Added {cursor.rowcount} tasks from {table}")
                
                self.db_manager.connection.commit()
                
        except Exception as e:
            self.db_manager.connection.rollback()
            logger.error(f"Error populating queue: {e}")
            raise

    def get_next_batch(self, batch_size: int = 10) -> List[Dict]:
        """
        Retrieves and locks the next batch of tasks for this worker.
        Uses FOR UPDATE SKIP LOCKED to ensure concurrency safety.
        """
        try:
            with self.db_manager.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Select and lock rows
                cursor.execute(f"""
                    UPDATE document_processing_queue
                    SET status = 'processing',
                        worker_id = %s,
                        started_at = NOW()
                    WHERE id IN (
                        SELECT id
                        FROM document_processing_queue
                        WHERE status = 'pending'
                        ORDER BY id ASC
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, contract_reg_number, table_source
                """, (self.worker_id, batch_size))
                
                tasks = cursor.fetchall()
                self.db_manager.connection.commit()
                
                if tasks:
                    logger.info(f"Worker {self.worker_id} picked up {len(tasks)} tasks")
                
                return [dict(task) for task in tasks]
                
        except Exception as e:
            self.db_manager.connection.rollback()
            logger.error(f"Error getting batch: {e}")
            raise

    def mark_completed(self, task_id: int):
        """Marks a task as successfully completed."""
        try:
            with self.db_manager.connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE document_processing_queue
                    SET status = 'completed',
                        finished_at = NOW()
                    WHERE id = %s
                """, (task_id,))
                self.db_manager.connection.commit()
        except Exception as e:
            self.db_manager.connection.rollback()
            logger.error(f"Error marking task {task_id} completed: {e}")

    def mark_error(self, task_id: int, error_message: str):
        """Marks a task as failed with an error message."""
        try:
            with self.db_manager.connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE document_processing_queue
                    SET status = 'error',
                        error_message = %s,
                        finished_at = NOW()
                    WHERE id = %s
                """, (error_message, task_id))
                self.db_manager.connection.commit()
        except Exception as e:
            self.db_manager.connection.rollback()
            logger.error(f"Error marking task {task_id} error: {e}")

    def close(self):
        self.db_manager.close()
