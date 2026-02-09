-- Быстрая проверка миграции статусов контрактов

\echo '========================================'
\echo 'ОБЩАЯ СТАТИСТИКА ПО ТАБЛИЦАМ'
\echo '========================================'

SELECT 
    '44-ФЗ: Основная' as table_name,
    COUNT(*) as records
FROM reestr_contract_44_fz
UNION ALL
SELECT '44-ФЗ: Комиссия', COUNT(*) FROM reestr_contract_44_fz_commission_work
UNION ALL
SELECT '44-ФЗ: Неясный', COUNT(*) FROM reestr_contract_44_fz_unclear
UNION ALL
SELECT '44-ФЗ: Разыгранные', COUNT(*) FROM reestr_contract_44_fz_awarded
UNION ALL
SELECT '44-ФЗ: Завершенные', COUNT(*) FROM reestr_contract_44_fz_completed
UNION ALL
SELECT '223-ФЗ: Основная', COUNT(*) FROM reestr_contract_223_fz
UNION ALL
SELECT '223-ФЗ: Комиссия', COUNT(*) FROM reestr_contract_223_fz_commission_work
UNION ALL
SELECT '223-ФЗ: Неясный', COUNT(*) FROM reestr_contract_223_fz_unclear
UNION ALL
SELECT '223-ФЗ: Разыгранные', COUNT(*) FROM reestr_contract_223_fz_awarded
UNION ALL
SELECT '223-ФЗ: Завершенные', COUNT(*) FROM reestr_contract_223_fz_completed
ORDER BY table_name;

\echo ''
\echo '========================================'
\echo 'ПРОВЕРКА: Записи, которые ДОЛЖНЫ быть мигрированы'
\echo '========================================'

SELECT 
    '44-ФЗ: Основная → Комиссия (end_date <= завтра)' as check_type,
    COUNT(*) as should_migrate
FROM reestr_contract_44_fz
WHERE end_date IS NOT NULL AND end_date <= CURRENT_DATE + INTERVAL '1 day'
UNION ALL
SELECT 
    '44-ФЗ: Комиссия → Разыгранные (есть delivery_start_date)',
    COUNT(*)
FROM reestr_contract_44_fz_commission_work
WHERE delivery_start_date IS NOT NULL
UNION ALL
SELECT 
    '44-ФЗ: Комиссия → Неясный (end_date < -60 дней, нет delivery_start_date)',
    COUNT(*)
FROM reestr_contract_44_fz_commission_work
WHERE end_date IS NOT NULL 
  AND end_date < CURRENT_DATE - INTERVAL '60 days'
  AND delivery_start_date IS NULL
UNION ALL
SELECT 
    '44-ФЗ: Все → Завершенные (delivery_end_date < -90 дней)',
    (
        SELECT COUNT(*) FROM reestr_contract_44_fz WHERE delivery_end_date < CURRENT_DATE - INTERVAL '90 days'
    ) + (
        SELECT COUNT(*) FROM reestr_contract_44_fz_commission_work WHERE delivery_end_date < CURRENT_DATE - INTERVAL '90 days'
    ) + (
        SELECT COUNT(*) FROM reestr_contract_44_fz_unclear WHERE delivery_end_date < CURRENT_DATE - INTERVAL '90 days'
    ) + (
        SELECT COUNT(*) FROM reestr_contract_44_fz_awarded WHERE delivery_end_date < CURRENT_DATE - INTERVAL '90 days'
    );

\echo ''
\echo '========================================'
\echo 'ПРОВЕРКА ДУБЛИКАТОВ (контракты в нескольких таблицах)'
\echo '========================================'

SELECT 
    contract_number,
    COUNT(*) as table_count,
    string_agg(DISTINCT table_name, ', ') as tables
FROM (
    SELECT contract_number, 'main' as table_name FROM reestr_contract_44_fz
    UNION ALL SELECT contract_number, 'commission' FROM reestr_contract_44_fz_commission_work
    UNION ALL SELECT contract_number, 'unclear' FROM reestr_contract_44_fz_unclear
    UNION ALL SELECT contract_number, 'awarded' FROM reestr_contract_44_fz_awarded
    UNION ALL SELECT contract_number, 'completed' FROM reestr_contract_44_fz_completed
) all_contracts
GROUP BY contract_number
HAVING COUNT(*) > 1
LIMIT 10;
