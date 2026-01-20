#!/bin/bash
# Скрипт для применения изменений на сервере

cd /opt/tendermonitor
source venv/bin/activate

# Применяем SQL скрипт для добавления timestamp
echo "Применяю SQL скрипт для добавления timestamp в file_names_xml..."
psql -h 100.122.104.106 -U postgres -d Tender_Monitor -f /tmp/add_timestamp_to_file_names.sql

# Копируем обновленные файлы
echo "Копирую обновленные файлы..."
cp /tmp/database_operations.py database_work/database_operations.py
cp /tmp/xml_parser.py parsing_xml/xml_parser.py
cp /tmp/logger_config.py utils/logger_config.py

echo "✅ Изменения применены!"
