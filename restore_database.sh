#!/bin/bash
# Скрипт для восстановления базы данных Tender_Monitor из дампа

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Восстановление базы данных Tender_Monitor"
echo "=========================================="

# Проверка аргументов
if [ -z "$1" ]; then
    echo -e "${RED}Ошибка: Укажите путь к дампу${NC}"
    echo "Использование: $0 <путь_к_дампу>"
    exit 1
fi

DUMP_FILE="$1"
DB_NAME="Tender_Monitor"

# Проверка существования файла
if [ ! -f "$DUMP_FILE" ]; then
    echo -e "${RED}Ошибка: Файл дампа не найден: $DUMP_FILE${NC}"
    exit 1
fi

echo -e "${YELLOW}Файл дампа: $DUMP_FILE${NC}"
echo -e "${YELLOW}База данных: $DB_NAME${NC}"
echo ""

# Определяем формат дампа
FILE_EXT="${DUMP_FILE##*.}"
FILE_BASE="${DUMP_FILE%.*}"

echo -e "${YELLOW}Определение формата дампа...${NC}"

# Проверка формата
if [ "$FILE_EXT" = "gz" ]; then
    # SQL.gz формат
    echo -e "${GREEN}✓ Формат: SQL (gzip compressed)${NC}"
    FORMAT="sql_gz"
elif [ "$FILE_EXT" = "sql" ]; then
    # SQL формат
    echo -e "${GREEN}✓ Формат: SQL${NC}"
    FORMAT="sql"
elif [ "$FILE_EXT" = "dump" ] || [ "$FILE_EXT" = "backup" ]; then
    # Custom формат
    echo -e "${GREEN}✓ Формат: PostgreSQL custom${NC}"
    FORMAT="custom"
else
    # Пробуем определить по содержимому
    if file "$DUMP_FILE" | grep -q "PostgreSQL"; then
        echo -e "${GREEN}✓ Формат: PostgreSQL custom${NC}"
        FORMAT="custom"
    else
        echo -e "${YELLOW}⚠ Неизвестный формат, пытаемся как SQL${NC}"
        FORMAT="sql"
    fi
fi

# Запрашиваем подтверждение
echo ""
echo -e "${RED}⚠ ВНИМАНИЕ: Будет выполнено восстановление базы данных!${NC}"
echo -e "${YELLOW}Текущие данные будут перезаписаны!${NC}"
read -p "Продолжить? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo -e "${YELLOW}Операция отменена${NC}"
    exit 0
fi

# Создаем резервную копию текущей БД (опционально)
echo ""
echo -e "${YELLOW}Создание резервной копии текущей БД...${NC}"
BACKUP_FILE="backup_before_restore_$(date +%Y%m%d_%H%M%S).dump"
pg_dump -Fc "$DB_NAME" > "$BACKUP_FILE" 2>/dev/null || echo -e "${YELLOW}⚠ Не удалось создать резервную копию${NC}"
if [ -f "$BACKUP_FILE" ]; then
    echo -e "${GREEN}✓ Резервная копия создана: $BACKUP_FILE${NC}"
fi

# Восстановление в зависимости от формата
echo ""
echo -e "${YELLOW}Восстановление базы данных...${NC}"

if [ "$FORMAT" = "sql_gz" ]; then
    # SQL.gz формат
    gunzip -c "$DUMP_FILE" | psql -d "$DB_NAME" -v ON_ERROR_STOP=1
elif [ "$FORMAT" = "sql" ]; then
    # SQL формат
    psql -d "$DB_NAME" -f "$DUMP_FILE" -v ON_ERROR_STOP=1
elif [ "$FORMAT" = "custom" ]; then
    # Custom формат
    pg_restore -d "$DB_NAME" -v -c "$DUMP_FILE"
fi

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ База данных восстановлена успешно${NC}"
else
    echo -e "${RED}❌ Ошибка при восстановлении${NC}"
    exit 1
fi

# Восстановление связей
echo ""
echo -e "${YELLOW}Проверка и восстановление связей...${NC}"

if [ -f "DB_RELATIONS_223_FZ.sql" ]; then
    psql -d "$DB_NAME" -f "DB_RELATIONS_223_FZ.sql" 2>&1 | grep -v "already exists" || true
    echo -e "${GREEN}✓ Связи проверены/восстановлены${NC}"
else
    echo -e "${YELLOW}⚠ Файл DB_RELATIONS_223_FZ.sql не найден, восстановите связи вручную${NC}"
fi

# Проверка восстановленных данных
echo ""
echo -e "${YELLOW}Проверка восстановленных данных...${NC}"

psql -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM reestr_contract_223_fz;" | xargs | read count
echo -e "${GREEN}✓ Записей в reestr_contract_223_fz: $count${NC}"

psql -d "$DB_NAME" -t -c "SELECT MAX(start_date) FROM reestr_contract_223_fz;" | xargs | read max_date
echo -e "${GREEN}✓ Последняя дата start_date: $max_date${NC}"

echo ""
echo -e "${GREEN}=========================================="
echo "Восстановление завершено!"
echo "==========================================${NC}"
echo ""
echo -e "${YELLOW}Следующие шаги:${NC}"
echo "1. Проверьте данные в БД"
echo "2. Обновите config.ini с датой: $max_date"
echo "3. Перезапустите приложение"
