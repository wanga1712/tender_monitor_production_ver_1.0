-- ========================================
-- ПРОВЕРКА СТАТУСА МИГРАЦИИ КОНТРАКТОВ
-- ========================================

\echo '========================================'
\echo '1. ОБЩАЯ СТАТИСТИКА ПО ТАБЛИЦАМ'
\echo '========================================'

-- 44-ФЗ
SELECT 
    '44-ФЗ: Основная таблица' as table_name,
    COUNT(*) as total_records
FROM reestr_contract_44_fz
UNION ALL
SELECT 
    '44-ФЗ: Работа комиссии',
    COUNT(*)
FROM reestr_contract_44_fz_commission_work
UNION ALL
SELECT 
    '44-ФЗ: Неясный',
    COUNT(*)
FROM reestr_contract_44_fz_unclear
UNION ALL
SELECT 
    '44-ФЗ: Разыгранные',
    COUNT(*)
FROM reestr_contract_44_fz_awarded
UNION ALL
SELECT 
    '44-ФЗ: Завершенные',
    COUNT(*)
FROM reestr_contract_44_fz_completed
UNION ALL
-- 223-ФЗ
SELECT 
    '223-ФЗ: Основная таблица',
    COUNT(*)
FROM reestr_contract_223_fz
UNION ALL
SELECT 
    '223-ФЗ: Работа комиссии',
    COUNT(*)
FROM reestr_contract_223_fz_commission_work
UNION ALL
SELECT 
    '223-ФЗ: Неясный',
    COUNT(*)
FROM reestr_contract_223_fz_unclear
UNION ALL
SELECT 
    '223-ФЗ: Разыгранные',
    COUNT(*)
FROM reestr_contract_223_fz_awarded
UNION ALL
SELECT 
    '223-ФЗ: Завершенные',
    COUNT(*)
FROM reestr_contract_223_fz_completed
ORDER BY table_name;

\echo ''
\echo '========================================'
\echo '2. ПРОВЕРКА: Записи в основной таблице 44-ФЗ, которые ДОЛЖНЫ быть в commission_work'
\echo '   (end_date <= CURRENT_DATE + 1 день)'
\echo '========================================'

SELECT 
    COUNT(*) as should_be_migrated_to_commission,
    MIN(end_date) as min_end_date,
    MAX(end_date) as max_end_date
FROM reestr_contract_44_fz
WHERE end_date IS NOT NULL 
  AND end_date <= CURRENT_DATE + INTERVAL '1 day';

\echo ''
\echo 'Примеры таких записей (первые 5):'
SELECT 
    id,
    contract_number,
    end_date,
    CURRENT_DATE + INTERVAL '1 day' as migration_threshold
FROM reestr_contract_44_fz
WHERE end_date IS NOT NULL 
  AND end_date <= CURRENT_DATE + INTERVAL '1 day'
LIMIT 5;

\echo ''
\echo '========================================'
\echo '3. ПРОВЕРКА: Записи в commission_work 44-ФЗ, которые ДОЛЖНЫ быть в awarded'
\echo '   (delivery_start_date IS NOT NULL)'
\echo '========================================'

SELECT 
    COUNT(*) as should_be_migrated_to_awarded
FROM reestr_contract_44_fz_commission_work
WHERE delivery_start_date IS NOT NULL;

\echo ''
\echo 'Примеры таких записей (первые 5):'
SELECT 
    id,
    contract_number,
    end_date,
    delivery_start_date
FROM reestr_contract_44_fz_commission_work
WHERE delivery_start_date IS NOT NULL
LIMIT 5;

\echo ''
\echo '========================================'
\echo '4. ПРОВЕРКА: Записи в commission_work 44-ФЗ, которые ДОЛЖНЫ быть в unclear'
\echo '   (end_date < CURRENT_DATE - 60 дней AND delivery_start_date IS NULL)'
\echo '========================================'

SELECT 
    COUNT(*) as should_be_migrated_to_unclear
FROM reestr_contract_44_fz_commission_work
WHERE end_date IS NOT NULL
  AND end_date < CURRENT_DATE - INTERVAL '60 days'
  AND delivery_start_date IS NULL;

\echo ''
\echo 'Примеры таких записей (первые 5):'
SELECT 
    id,
    contract_number,
    end_date,
    delivery_start_date,
    CURRENT_DATE - INTERVAL '60 days' as unclear_threshold
FROM reestr_contract_44_fz_commission_work
WHERE end_date IS NOT NULL
  AND end_date < CURRENT_DATE - INTERVAL '60 days'
  AND delivery_start_date IS NULL
LIMIT 5;

\echo ''
\echo '========================================'
\echo '5. ПРОВЕРКА: Записи, которые ДОЛЖНЫ быть в completed 44-ФЗ'
\echo '   (delivery_end_date < CURRENT_DATE - 90 дней)'
\echo '========================================'

-- Проверяем в основной таблице
SELECT 
    'В основной таблице' as source_table,
    COUNT(*) as should_be_completed
FROM reestr_contract_44_fz
WHERE delivery_end_date IS NOT NULL
  AND delivery_end_date < CURRENT_DATE - INTERVAL '90 days'
UNION ALL
-- Проверяем в commission_work
SELECT 
    'В commission_work',
    COUNT(*)
FROM reestr_contract_44_fz_commission_work
WHERE delivery_end_date IS NOT NULL
  AND delivery_end_date < CURRENT_DATE - INTERVAL '90 days'
UNION ALL
-- Проверяем в unclear
SELECT 
    'В unclear',
    COUNT(*)
FROM reestr_contract_44_fz_unclear
WHERE delivery_end_date IS NOT NULL
  AND delivery_end_date < CURRENT_DATE - INTERVAL '90 days'
UNION ALL
-- Проверяем в awarded
SELECT 
    'В awarded',
    COUNT(*)
FROM reestr_contract_44_fz_awarded
WHERE delivery_end_date IS NOT NULL
  AND delivery_end_date < CURRENT_DATE - INTERVAL '90 days';

\echo ''
\echo 'Примеры таких записей из основной таблицы (первые 5):'
SELECT 
    id,
    contract_number,
    delivery_end_date,
    CURRENT_DATE - INTERVAL '90 days' as completed_threshold
FROM reestr_contract_44_fz
WHERE delivery_end_date IS NOT NULL
  AND delivery_end_date < CURRENT_DATE - INTERVAL '90 days'
LIMIT 5;

\echo ''
\echo '========================================'
\echo '6. ПРОВЕРКА ДУБЛИКАТОВ: Контракты с одинаковым contract_number в разных таблицах'
\echo '========================================'

-- 44-ФЗ
SELECT 
    contract_number,
    COUNT(*) as table_count,
    string_agg(DISTINCT table_name, ', ' ORDER BY table_name) as tables
FROM (
    SELECT contract_number, 'main' as table_name FROM reestr_contract_44_fz
    UNION ALL
    SELECT contract_number, 'commission_work' FROM reestr_contract_44_fz_commission_work
    UNION ALL
    SELECT contract_number, 'unclear' FROM reestr_contract_44_fz_unclear
    UNION ALL
    SELECT contract_number, 'awarded' FROM reestr_contract_44_fz_awarded
    UNION ALL
    SELECT contract_number, 'completed' FROM reestr_contract_44_fz_completed
) all_contracts
GROUP BY contract_number
HAVING COUNT(*) > 1
LIMIT 10;

\echo ''
\echo '========================================'
\echo '7. СТАТИСТИКА ПО ДАТАМ (44-ФЗ)'
\echo '========================================'

SELECT 
    'Основная таблица' as table_name,
    COUNT(*) FILTER (WHERE end_date IS NOT NULL) as with_end_date,
    COUNT(*) FILTER (WHERE delivery_start_date IS NOT NULL) as with_delivery_start,
    COUNT(*) FILTER (WHERE delivery_end_date IS NOT NULL) as with_delivery_end,
    COUNT(*) FILTER (WHERE delivery_end_date < CURRENT_DATE - INTERVAL '90 days') as should_be_completed
FROM reestr_contract_44_fz
UNION ALL
SELECT 
    'commission_work',
    COUNT(*) FILTER (WHERE end_date IS NOT NULL),
    COUNT(*) FILTER (WHERE delivery_start_date IS NOT NULL),
    COUNT(*) FILTER (WHERE delivery_end_date IS NOT NULL),
    COUNT(*) FILTER (WHERE delivery_end_date < CURRENT_DATE - INTERVAL '90 days')
FROM reestr_contract_44_fz_commission_work
UNION ALL
SELECT 
    'unclear',
    COUNT(*) FILTER (WHERE end_date IS NOT NULL),
    COUNT(*) FILTER (WHERE delivery_start_date IS NOT NULL),
    COUNT(*) FILTER (WHERE delivery_end_date IS NOT NULL),
    COUNT(*) FILTER (WHERE delivery_end_date < CURRENT_DATE - INTERVAL '90 days')
FROM reestr_contract_44_fz_unclear
UNION ALL
SELECT 
    'awarded',
    COUNT(*) FILTER (WHERE end_date IS NOT NULL),
    COUNT(*) FILTER (WHERE delivery_start_date IS NOT NULL),
    COUNT(*) FILTER (WHERE delivery_end_date IS NOT NULL),
    COUNT(*) FILTER (WHERE delivery_end_date < CURRENT_DATE - INTERVAL '90 days')
FROM reestr_contract_44_fz_awarded
UNION ALL
SELECT 
    'completed',
    COUNT(*) FILTER (WHERE end_date IS NOT NULL),
    COUNT(*) FILTER (WHERE delivery_start_date IS NOT NULL),
    COUNT(*) FILTER (WHERE delivery_end_date IS NOT NULL),
    COUNT(*) FILTER (WHERE delivery_end_date < CURRENT_DATE - INTERVAL '90 days')
FROM reestr_contract_44_fz_completed;

\echo ''
\echo '========================================'
\echo '8. ПРОВЕРКА 223-ФЗ (аналогично)'
\echo '========================================'

SELECT 
    '223-ФЗ: Основная таблица - должны быть в commission' as check_type,
    COUNT(*) as count
FROM reestr_contract_223_fz
WHERE end_date IS NOT NULL 
  AND end_date <= CURRENT_DATE + INTERVAL '1 day'
UNION ALL
SELECT 
    '223-ФЗ: commission - должны быть в awarded',
    COUNT(*)
FROM reestr_contract_223_fz_commission_work
WHERE delivery_start_date IS NOT NULL
UNION ALL
SELECT 
    '223-ФЗ: commission - должны быть в unclear',
    COUNT(*)
FROM reestr_contract_223_fz_commission_work
WHERE end_date IS NOT NULL
  AND end_date < CURRENT_DATE - INTERVAL '60 days'
  AND delivery_start_date IS NULL
UNION ALL
SELECT 
    '223-ФЗ: должны быть в completed (из всех таблиц)',
    (
        SELECT COUNT(*) FROM reestr_contract_223_fz WHERE delivery_end_date < CURRENT_DATE - INTERVAL '90 days'
    ) + (
        SELECT COUNT(*) FROM reestr_contract_223_fz_commission_work WHERE delivery_end_date < CURRENT_DATE - INTERVAL '90 days'
    ) + (
        SELECT COUNT(*) FROM reestr_contract_223_fz_unclear WHERE delivery_end_date < CURRENT_DATE - INTERVAL '90 days'
    ) + (
        SELECT COUNT(*) FROM reestr_contract_223_fz_awarded WHERE delivery_end_date < CURRENT_DATE - INTERVAL '90 days'
    );

\echo ''
\echo '========================================'
\echo 'ПРОВЕРКА ЗАВЕРШЕНА'
\echo '========================================'
