# Document Processing Daemon

## Overview
This daemon processes tender documentation (PDF, Word, Excel, etc.) to find keywords from the Product Catalog and User Filters.

## Features
- Fetches tasks from `reestr_contract_44_fz`, `reestr_contract_223_fz` and their awarded counterparts.
- Downloads and unpacks archives.
- Parses various formats: PDF (incl. scanned), DOCX, XLSX, TXT.
- **Concurrency:** Supports multiple workers with locking (Worker 1 = Server, Worker 2 = Local).
- **Resource Management:** Configurable page limits for PDF parsing on server.
- **Output:** Saves matches to `match_repository` and updates task status.

## Architecture
- `daemon.py`: Main entry point.
- `queue_manager.py`: Handles database locking and status updates.
- `downloader.py`: Handles file download and unpacking.
- `parser_factory.py`: Returns appropriate parser for file type.
- `parsers/`: Individual parsers.

## Database Requirements
- `document_processing_queue`: Queue for tasks.
- `product_catalog`: Keywords database.
- `match_repository`: Results storage.

## Configuration
- `WORKER_ID`: 1 (Server) or 2 (Local).
- `BATCH_SIZE`: 10.
- `PDF_PAGE_LIMIT`: 1 (Server) or All (Local).
