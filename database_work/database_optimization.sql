-- SQL скрипт для оптимизации базы данных TenderMonitor
-- Сгенерировано автоматически

BEGIN;

-- ========================================
-- ИНДЕКСЫ (высокий приоритет)
-- ========================================
CREATE INDEX IF NOT EXISTS idx_file_names_xml_file_name ON file_names_xml (file_name);
CREATE UNIQUE INDEX IF NOT EXISTS idx_file_names_xml_file_name_unique ON file_names_xml (file_name);
CREATE INDEX IF NOT EXISTS idx_reestr_contract_223_fz_contract_number ON reestr_contract_223_fz (contract_number);
CREATE UNIQUE INDEX IF NOT EXISTS idx_reestr_contract_223_fz_contract_number_unique ON reestr_contract_223_fz (contract_number);
CREATE INDEX IF NOT EXISTS idx_reestr_contract_44_fz_contract_number ON reestr_contract_44_fz (contract_number);
CREATE UNIQUE INDEX IF NOT EXISTS idx_reestr_contract_44_fz_contract_number_unique ON reestr_contract_44_fz (contract_number);
CREATE INDEX IF NOT EXISTS idx_trading_platform_trading_platform_name ON trading_platform (trading_platform_name);

-- ========================================
-- ВНЕШНИЕ КЛЮЧИ (средний приоритет)
-- ========================================
ALTER TABLE reestr_contract_223_fz ADD CONSTRAINT fk_reestr_contract_223_fz_region_id FOREIGN KEY (region_id) REFERENCES region(id) ON DELETE CASCADE;
ALTER TABLE reestr_contract_44_fz ADD CONSTRAINT fk_reestr_contract_44_fz_region_id FOREIGN KEY (region_id) REFERENCES region(id) ON DELETE CASCADE;

COMMIT;