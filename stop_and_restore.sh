#!/bin/bash
# Скрипт для остановки приложения и восстановления БД

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=========================================="
echo "Остановка приложения и подготовка к восстановлению БД"
echo "=========================================="

# Проверка прав
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Ошибка: Запустите скрипт с правами root (sudo)${NC}"
    exit 1
fi

echo -e "${YELLOW}Шаг 1: Остановка приложения...${NC}"

# Останавливаем service
if systemctl is-active --quiet tendermonitor; then
    systemctl stop tendermonitor
    echo -e "${GREEN}✅ Service tendermonitor остановлен${NC}"
else
    echo -e "${YELLOW}⚠ Service tendermonitor не был запущен${NC}"
fi

# Проверяем процессы
PROCESSES=$(ps aux | grep '[p]ython.*main.py' | wc -l)
if [ "$PROCESSES" -gt 0 ]; then
    echo -e "${YELLOW}⚠ Найдены процессы Python, завершаем...${NC}"
    pkill -f 'python.*main.py' || true
    sleep 2
    PROCESSES=$(ps aux | grep '[p]ython.*main.py' | wc -l)
    if [ "$PROCESSES" -gt 0 ]; then
        echo -e "${RED}❌ Не удалось остановить процессы${NC}"
        exit 1
    else
        echo -e "${GREEN}✅ Все процессы остановлены${NC}"
    fi
else
    echo -e "${GREEN}✅ Активных процессов не найдено${NC}"
fi

echo -e "\n${YELLOW}Шаг 2: Проверка подключений к БД...${NC}"

# Загружаем данные подключения
source /opt/tendermonitor/database_work/db_credintials.env

DB_HOST=${DB_HOST:-localhost}
DB_USER=${DB_USER:-postgres}
DB_NAME=${DB_DATABASE:-tender_monitor}

echo "   Хост: $DB_HOST"
echo "   Пользователь: $DB_USER"
echo "   База данных: $DB_NAME"

# Проверяем подключения
CONNECTIONS=$(PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = '$DB_NAME' AND state != 'idle';" 2>/dev/null || echo "0")

echo -e "\n${YELLOW}Активных подключений (не idle): $CONNECTIONS${NC}"

if [ "$CONNECTIONS" -gt 1 ]; then
    echo -e "${YELLOW}⚠ Есть активные подключения к БД${NC}"
    echo "   Показываю подключения:"
    PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "SELECT pid, usename, application_name, client_addr, state FROM pg_stat_activity WHERE datname = '$DB_NAME' AND state != 'idle';" 2>/dev/null || true
fi

echo -e "\n${GREEN}=========================================="
echo "Готово к восстановлению БД"
echo "==========================================${NC}"
echo ""
echo -e "${YELLOW}Для восстановления выполните:${NC}"
echo "  cd /opt/tendermonitor"
echo "  source venv/bin/activate"
echo "  python3 restore_from_dump.py"
