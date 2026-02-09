-- Детальная проверка миграции

-- 1. Сколько всего записей в каждой таблице 44-ФЗ
SELECT 'Основная таблица' as table_name, COUNT(*) as total FROM reestr_contract_44_fz
UNION ALL
SELECT 'Комиссия', COUNT(*) FROM reestr_contract_44_fz_commission_work
UNION ALL
SELECT 'Неясный', COUNT(*) FROM reestr_contract_44_fz_unclear
UNION ALL
SELECT 'Разыгранные', COUNT(*) FROM reestr_contract_44_fz_awarded
UNION ALL
SELECT 'Завершенные', COUNT(*) FROM reestr_contract_44_fz_completed;

-- 2. Записи в основной, которые ДОЛЖНЫ быть в commission_work
SELECT COUNT(*) as should_migrate_to_commission
FROM reestr_contract_44_fz
WHERE end_date IS NOT NULL 
  AND end_date <= CURRENT_DATE + INTERVAL '1 day';

-- 3. Записи в commission_work, которые ДОЛЖНЫ быть в unclear
SELECT COUNT(*) as should_migrate_to_unclear
FROM reestr_contract_44_fz_commission_work
WHERE end_date IS NOT NULL 
  AND end_date < CURRENT_DATE - INTERVAL '60 days'
  AND delivery_start_date IS NULL;

-- 4. Записи в commission_work, которые ДОЛЖНЫ быть в awarded
SELECT COUNT(*) as should_migrate_to_awarded
FROM reestr_contract_44_fz_commission_work
WHERE delivery_start_date IS NOT NULL;

-- 5. Примеры дубликатов (контракты в нескольких таблицах)
SELECT 
    contract_number,
    COUNT(*) as table_count,
    string_agg(DISTINCT table_name, ', ' ORDER BY table_name) as tables
FROM (
    SELECT contract_number, 'main' as table_name FROM reestr_contract_44_fz
    UNION ALL SELECT contract_number, 'commission' FROM reestr_contract_44_fz_commission_work
    UNION ALL SELECT contract_number, 'unclear' FROM reestr_contract_44_fz_unclear
    UNION ALL SELECT contract_number, 'awarded' FROM reestr_contract_44_fz_awarded
    UNION ALL SELECT contract_number, 'completed' FROM reestr_contract_44_fz_completed
) all_contracts
GROUP BY contract_number
HAVING COUNT(*) > 1
LIMIT 20;

-- 6. Проверка: есть ли в commission_work записи, которые уже есть в unclear
SELECT COUNT(*) as duplicates_commission_unclear
FROM reestr_contract_44_fz_commission_work c
WHERE EXISTS (
    SELECT 1 FROM reestr_contract_44_fz_unclear u 
    WHERE u.contract_number = c.contract_number
);

-- 7. Проверка: есть ли в commission_work записи, которые уже есть в completed
SELECT COUNT(*) as duplicates_commission_completed
FROM reestr_contract_44_fz_commission_work c
WHERE EXISTS (
    SELECT 1 FROM reestr_contract_44_fz_completed comp 
    WHERE comp.contract_number = c.contract_number
);
