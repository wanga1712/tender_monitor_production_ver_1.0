#!/bin/bash
# Скрипт для исправления конфигурации прокси: отключение nginx, настройка stunnel
# Использование: sudo ./fix_proxy_config.sh

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Исправление конфигурации прокси"
echo "=========================================="
echo ""

# Проверка прав root
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Ошибка: Запустите скрипт с правами root (sudo)${NC}"
    exit 1
fi

# Шаг 1: Остановка и отключение nginx
echo -e "${YELLOW}Шаг 1: Остановка nginx...${NC}"

if systemctl is-active --quiet nginx; then
    echo "Останавливаю nginx..."
    systemctl stop nginx
    echo -e "${GREEN}✓ Nginx остановлен${NC}"
else
    echo "Nginx уже остановлен"
fi

if systemctl is-enabled --quiet nginx 2>/dev/null; then
    echo "Отключаю автозапуск nginx..."
    systemctl disable nginx
    echo -e "${GREEN}✓ Автозапуск nginx отключен${NC}"
else
    echo "Автозапуск nginx уже отключен"
fi

echo ""

# Шаг 2: Проверка КриптоПро CSP
echo -e "${YELLOW}Шаг 2: Проверка КриптоПро CSP...${NC}"

STUNNEL_THREAD="/opt/cprocsp/sbin/amd64/stunnel_thread"
STUNNEL_FORK="/opt/cprocsp/sbin/amd64/stunnel_fork"

if [ ! -f "$STUNNEL_THREAD" ] && [ ! -f "$STUNNEL_FORK" ]; then
    echo -e "${RED}❌ КриптоПро CSP не найден!${NC}"
    echo ""
    echo "Необходимо установить КриптоПро CSP 5.0.12000"
    echo "Ссылка: https://cryptopro.ru/products/csp/downloads"
    echo ""
    echo "После установки запустите этот скрипт снова."
    exit 1
fi

if [ -f "$STUNNEL_THREAD" ]; then
    STUNNEL_EXE="$STUNNEL_THREAD"
    echo -e "${GREEN}✓ Найден stunnel_thread${NC}"
else
    STUNNEL_EXE="$STUNNEL_FORK"
    echo -e "${GREEN}✓ Найден stunnel_fork${NC}"
fi

echo ""

# Шаг 3: Проверка конфигурации stunnel
echo -e "${YELLOW}Шаг 3: Проверка конфигурации stunnel...${NC}"

STUNNEL_CONF="/etc/opt/cprocsp/stunnel/stunnel.conf"

if [ ! -f "$STUNNEL_CONF" ]; then
    echo -e "${YELLOW}⚠ Конфигурация stunnel не найдена${NC}"
    echo ""
    echo "Запустите скрипт настройки stunnel:"
    echo "  sudo ./setup_stunnel_linux.sh"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓ Конфигурация stunnel найдена${NC}"
echo ""

# Шаг 4: Проверка сертификата
echo -e "${YELLOW}Шаг 4: Проверка сертификата...${NC}"

CERT_PATH="/etc/opt/cprocsp/stunnel/1.cer"

if [ ! -f "$CERT_PATH" ]; then
    echo -e "${YELLOW}⚠ Сертификат не найден в $CERT_PATH${NC}"
    echo ""
    echo "Скопируйте сертификат с Windows на сервер:"
    echo "  scp certificate.cer user@server:/tmp/"
    echo "  sudo cp /tmp/certificate.cer $CERT_PATH"
    echo "  sudo chmod 644 $CERT_PATH"
    echo ""
    read -p "Продолжить без проверки сертификата? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    echo -e "${GREEN}✓ Сертификат найден${NC}"
fi

echo ""

# Шаг 5: Остановка старых процессов stunnel
echo -e "${YELLOW}Шаг 5: Остановка старых процессов stunnel...${NC}"

if [ -f "/var/opt/cprocsp/tmp/stunnel_cli.pid" ]; then
    OLD_PID=$(cat /var/opt/cprocsp/tmp/stunnel_cli.pid)
    if ps -p $OLD_PID > /dev/null 2>&1; then
        echo "Останавливаю старый процесс stunnel (PID: $OLD_PID)..."
        kill $OLD_PID || true
        sleep 1
        echo -e "${GREEN}✓ Старый процесс остановлен${NC}"
    fi
fi

# Убиваем все процессы stunnel на всякий случай
pkill -f "stunnel" || true
sleep 1

echo ""

# Шаг 6: Запуск stunnel
echo -e "${YELLOW}Шаг 6: Запуск stunnel...${NC}"

# Запускаем stunnel в фоне
nohup $STUNNEL_EXE $STUNNEL_CONF > /var/opt/cprocsp/tmp/stunnel_startup.log 2>&1 &
STUNNEL_PID=$!

echo "Stunnel запущен (PID: $STUNNEL_PID)"
echo "Ожидание инициализации..."
sleep 3

# Проверяем, что процесс жив
if ! ps -p $STUNNEL_PID > /dev/null 2>&1; then
    echo -e "${RED}❌ Stunnel не запустился!${NC}"
    echo ""
    echo "Проверьте логи:"
    echo "  tail -f /var/opt/cprocsp/tmp/stunnel.log"
    echo "  cat /var/opt/cprocsp/tmp/stunnel_startup.log"
    exit 1
fi

echo -e "${GREEN}✓ Stunnel запущен${NC}"
echo ""

# Шаг 7: Проверка порта 8080
echo -e "${YELLOW}Шаг 7: Проверка порта 8080...${NC}"

sleep 2

if netstat -tuln | grep -q ":8080 "; then
    echo -e "${GREEN}✓ Порт 8080 слушается${NC}"
else
    echo -e "${RED}❌ Порт 8080 не доступен!${NC}"
    echo ""
    echo "Проверьте логи stunnel:"
    echo "  tail -f /var/opt/cprocsp/tmp/stunnel.log"
    exit 1
fi

echo ""

# Шаг 8: Тестовый запрос
echo -e "${YELLOW}Шаг 8: Тестовый запрос...${NC}"

if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080 | grep -q "200\|400\|404"; then
    echo -e "${GREEN}✓ Stunnel отвечает на запросы${NC}"
else
    echo -e "${YELLOW}⚠ Stunnel запущен, но тестовый запрос не прошел${NC}"
    echo "Это может быть нормально, если сервер требует специальные заголовки"
fi

echo ""
echo -e "${GREEN}=========================================="
echo "✅ Конфигурация исправлена!"
echo "==========================================${NC}"
echo ""
echo -e "${YELLOW}Статус:${NC}"
echo "  ✓ Nginx остановлен и отключен"
echo "  ✓ Stunnel запущен и работает"
echo "  ✓ Порт 8080 доступен"
echo ""
echo -e "${YELLOW}Следующие шаги:${NC}"
echo "  1. Перезапустите сервис tendermonitor:"
echo "     sudo systemctl restart tendermonitor.service"
echo ""
echo "  2. Проверьте логи:"
echo "     sudo journalctl -u tendermonitor.service -f"
echo ""
echo -e "${YELLOW}Полезные команды:${NC}"
echo "  Логи stunnel:  tail -f /var/opt/cprocsp/tmp/stunnel.log"
echo "  Статус порта:  netstat -tuln | grep 8080"
echo "  Остановка:     kill \$(cat /var/opt/cprocsp/tmp/stunnel_cli.pid)"
echo ""
