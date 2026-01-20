-- Добавление поля processed_at (timestamp) в таблицу file_names_xml
-- для отслеживания времени обработки файлов

BEGIN;

-- Добавляем поле processed_at с дефолтным значением CURRENT_TIMESTAMP
ALTER TABLE file_names_xml 
ADD COLUMN IF NOT EXISTS processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Создаем индекс для быстрого поиска по времени обработки
CREATE INDEX IF NOT EXISTS idx_file_names_xml_processed_at 
ON file_names_xml (processed_at DESC);

-- Обновляем существующие записи (если они есть) текущим временем
-- Это нужно только если в таблице уже есть данные
UPDATE file_names_xml 
SET processed_at = CURRENT_TIMESTAMP 
WHERE processed_at IS NULL;

COMMIT;
