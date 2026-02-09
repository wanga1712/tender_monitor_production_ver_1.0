-- Таблица очереди обработки документов
CREATE TABLE IF NOT EXISTS document_processing_queue (
    id SERIAL PRIMARY KEY,
    contract_reg_number TEXT NOT NULL, -- Реестровый номер контракта
    table_source TEXT NOT NULL, -- Источник: 'reestr_contract_44_fz', 'reestr_contract_223_fz' и т.д.
    status TEXT DEFAULT 'pending', -- pending, processing, processed, error
    worker_id INT, -- ID воркера, который взял задачу (1 - сервер, 2 - локальный)
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(contract_reg_number, table_source)
);

-- Таблица каталога продуктов (ключевые слова для поиска)
CREATE TABLE IF NOT EXISTS product_catalog (
    id SERIAL PRIMARY KEY,
    keyword TEXT UNIQUE NOT NULL,
    category TEXT
);

-- Таблица результатов поиска (найденные совпадения)
CREATE TABLE IF NOT EXISTS match_repository (
    id SERIAL PRIMARY KEY,
    queue_id INT REFERENCES document_processing_queue(id) ON DELETE CASCADE,
    file_name TEXT NOT NULL,
    keyword_found TEXT NOT NULL,
    context TEXT, -- Контекст (окружающий текст)
    page_number INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для ускорения поиска очереди
CREATE INDEX IF NOT EXISTS idx_queue_status ON document_processing_queue(status);
CREATE INDEX IF NOT EXISTS idx_queue_worker ON document_processing_queue(worker_id);
