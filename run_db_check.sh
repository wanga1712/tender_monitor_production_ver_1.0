#!/bin/bash
# Скрипт для проверки миграции в БД

cd /opt/tendermonitor

# Загружаем credentials
source database_work/db_credintials.env

echo "=========================================="
echo "ПРОВЕРКА МИГРАЦИИ В БД"
echo "=========================================="
echo "База данных: $DB_DATABASE"
echo "Пользователь: $DB_USER"
echo ""

# Быстрая проверка
echo "1. Быстрая проверка миграции:"
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_DATABASE -f quick_check_migration.sql

echo ""
echo "=========================================="
echo "Для полной проверки выполните:"
echo "PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_DATABASE -f check_migration_status.sql"
echo "=========================================="
