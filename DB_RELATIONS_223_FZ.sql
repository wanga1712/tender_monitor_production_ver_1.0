-- Связи (Foreign Keys) для таблицы reestr_contract_223_fz
-- Эти связи нужно восстановить после восстановления дампа БД

-- ВАЖНО: Эти связи должны быть восстановлены после восстановления данных

-- 1. Связь с contractor
ALTER TABLE reestr_contract_223_fz
ADD CONSTRAINT reestr_contract_223_fz_contractor_id_fkey
FOREIGN KEY (contractor_id) REFERENCES contractor(id)
ON UPDATE NO ACTION
ON DELETE NO ACTION;

-- 2. Связь с customer
ALTER TABLE reestr_contract_223_fz
ADD CONSTRAINT reestr_contract_223_fz_customer_id_fkey
FOREIGN KEY (customer_id) REFERENCES customer(id)
ON UPDATE NO ACTION
ON DELETE NO ACTION;

-- 3. Связь с collection_codes_okpd
ALTER TABLE reestr_contract_223_fz
ADD CONSTRAINT reestr_contract_223_fz_okpd_id_fkey
FOREIGN KEY (okpd_id) REFERENCES collection_codes_okpd(id)
ON UPDATE NO ACTION
ON DELETE NO ACTION;

-- 4. Связь с trading_platform
ALTER TABLE reestr_contract_223_fz
ADD CONSTRAINT reestr_contract_223_fz_trading_platform_id_fkey
FOREIGN KEY (trading_platform_id) REFERENCES trading_platform(id)
ON UPDATE NO ACTION
ON DELETE NO ACTION;

-- Индексы (если нужно восстановить отдельно)
CREATE INDEX IF NOT EXISTS idx_okpd_startdate_223fz 
ON reestr_contract_223_fz USING btree (okpd_id, start_date DESC);

CREATE INDEX IF NOT EXISTS idx_reestr_contract_223_fz_status_id 
ON reestr_contract_223_fz USING btree (status_id) 
WHERE (status_id IS NOT NULL);
