#!/bin/bash
# Скрипт настройки nginx для работы с ЕИС на Linux

set -e

echo "=========================================="
echo "Настройка nginx для TenderMonitor"
echo "=========================================="

# Цвета
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Проверка прав root
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Ошибка: Запустите скрипт с правами root (sudo)${NC}"
    exit 1
fi

echo -e "${YELLOW}Шаг 1: Проверка установки nginx...${NC}"

# Проверяем наличие nginx
if ! command -v nginx &> /dev/null; then
    echo -e "${YELLOW}Nginx не установлен. Устанавливаем...${NC}"
    
    # Определяем дистрибутив
    if [ -f /etc/redhat-release ]; then
        # RedOS, CentOS, RHEL
        yum install -y nginx
    elif [ -f /etc/debian_version ]; then
        # Debian, Ubuntu
        apt-get update
        apt-get install -y nginx
    else
        echo -e "${RED}Не удалось определить дистрибутив. Установите nginx вручную.${NC}"
        exit 1
    fi
fi

echo -e "${GREEN}✓ Nginx установлен${NC}"

echo -e "${YELLOW}Шаг 2: Копирование конфигурации...${NC}"

# Копируем конфигурацию
if [ -f "nginx_eis.conf" ]; then
    cp nginx_eis.conf /etc/nginx/conf.d/eis.conf
    echo -e "${GREEN}✓ Конфигурация скопирована в /etc/nginx/conf.d/eis.conf${NC}"
else
    echo -e "${RED}Ошибка: Файл nginx_eis.conf не найден${NC}"
    exit 1
fi

echo -e "${YELLOW}Шаг 3: Проверка конфигурации nginx...${NC}"

# Проверяем конфигурацию
if nginx -t; then
    echo -e "${GREEN}✓ Конфигурация nginx корректна${NC}"
else
    echo -e "${RED}Ошибка в конфигурации nginx!${NC}"
    exit 1
fi

echo -e "${YELLOW}Шаг 4: Запуск nginx...${NC}"

# Включаем и запускаем nginx
systemctl enable nginx
systemctl restart nginx

# Проверяем статус
if systemctl is-active --quiet nginx; then
    echo -e "${GREEN}✓ Nginx запущен и работает${NC}"
else
    echo -e "${RED}Ошибка: Nginx не запустился${NC}"
    systemctl status nginx
    exit 1
fi

echo -e "${YELLOW}Шаг 5: Проверка доступности порта 8080...${NC}"

sleep 2

if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080 | grep -q "200\|404\|502"; then
    echo -e "${GREEN}✓ Порт 8080 доступен${NC}"
else
    echo -e "${YELLOW}⚠ Порт 8080 может быть недоступен. Проверьте логи:${NC}"
    echo "  tail -f /var/log/nginx/eis_error.log"
fi

echo ""
echo -e "${GREEN}=========================================="
echo "Настройка завершена!"
echo "==========================================${NC}"
echo ""
echo -e "${YELLOW}Полезные команды:${NC}"
echo "  Статус nginx: systemctl status nginx"
echo "  Перезапуск: systemctl restart nginx"
echo "  Логи ошибок: tail -f /var/log/nginx/eis_error.log"
echo "  Логи доступа: tail -f /var/log/nginx/eis_access.log"
echo "  Проверка порта: curl http://localhost:8080"
echo ""

