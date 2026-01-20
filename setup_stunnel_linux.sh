#!/bin/bash
# Скрипт настройки Stunnel для Linux согласно технической инструкции
# Операционная система: RedOS 7.3.3
# КриптоПро CSP 5.0.12000 с поддержкой Stunnel и ГОСТ2012

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Настройка Stunnel для Linux"
echo "=========================================="

# Проверка прав root
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Ошибка: Запустите скрипт с правами root (sudo)${NC}"
    exit 1
fi

# Проверка наличия КриптоПро CSP
STUNNEL_THREAD="/opt/cprocsp/sbin/amd64/stunnel_thread"
STUNNEL_FORK="/opt/cprocsp/sbin/amd64/stunnel_fork"

if [ ! -f "$STUNNEL_THREAD" ] && [ ! -f "$STUNNEL_FORK" ]; then
    echo -e "${RED}Ошибка: КриптоПро CSP не найден!${NC}"
    echo "Установите КриптоПро CSP 5.0.12000 с поддержкой Stunnel и ГОСТ2012"
    echo "Ссылка: https://cryptopro.ru/products/csp/downloads"
    exit 1
fi

# Определяем, какой stunnel использовать
if [ -f "$STUNNEL_THREAD" ]; then
    STUNNEL_EXE="$STUNNEL_THREAD"
    echo -e "${GREEN}✓ Найден stunnel_thread${NC}"
else
    STUNNEL_EXE="$STUNNEL_FORK"
    echo -e "${GREEN}✓ Найден stunnel_fork${NC}"
fi

# Создаем необходимые директории
echo -e "${YELLOW}Шаг 1: Создание директорий...${NC}"

mkdir -p /etc/opt/cprocsp/stunnel
mkdir -p /var/opt/cprocsp/tmp

chmod 755 /etc/opt/cprocsp/stunnel
chmod 755 /var/opt/cprocsp/tmp

echo -e "${GREEN}✓ Директории созданы${NC}"

# Проверка наличия сертификата
CERT_PATH="/etc/opt/cprocsp/stunnel/1.cer"
CAFILE_PATH="/etc/opt/cprocsp/stunnel/server_cert.cer"

if [ ! -f "$CERT_PATH" ]; then
    echo -e "${YELLOW}⚠ Внимание: Сертификат не найден в $CERT_PATH${NC}"
    echo "Скопируйте сертификат с Windows на сервер:"
    echo "  scp certificate.cer user@nyx:/tmp/"
    echo "Затем переместите его:"
    echo "  sudo cp /tmp/certificate.cer $CERT_PATH"
    echo "  sudo chmod 644 $CERT_PATH"
    read -p "Продолжить настройку без сертификата? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    echo -e "${GREEN}✓ Сертификат найден: $CERT_PATH${NC}"
    chmod 644 "$CERT_PATH"
fi

# Проверка наличия CAfile (сертификат сервера)
if [ ! -f "$CAFILE_PATH" ]; then
    echo -e "${YELLOW}⚠ CAfile (сертификат сервера) не найден в $CAFILE_PATH${NC}"
    echo "Если у вас есть server_cert.cer, скопируйте его:"
    echo "  sudo cp /tmp/server_cert.cer $CAFILE_PATH"
    echo "  sudo chmod 644 $CAFILE_PATH"
    read -p "Продолжить без CAfile? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
    USE_CAFILE=0
else
    echo -e "${GREEN}✓ CAfile найден: $CAFILE_PATH${NC}"
    chmod 644 "$CAFILE_PATH"
    USE_CAFILE=1
fi

# Запрашиваем PIN-код
echo -e "${YELLOW}Введите PIN-код для сертификата (или нажмите Enter, если не требуется):${NC}"
read -s PIN_CODE
echo ""

# Создаем конфигурационный файл stunnel.conf
echo -e "${YELLOW}Шаг 2: Создание конфигурации stunnel.conf...${NC}"

STUNNEL_CONF="/etc/opt/cprocsp/stunnel/stunnel.conf"

# Создаем конфигурацию с учетом всех параметров из рабочей версии Windows
cat > "$STUNNEL_CONF" << EOF
cert = $CERT_PATH
pid = /var/opt/cprocsp/tmp/stunnel_cli.pid
output = /var/opt/cprocsp/tmp/stunnel.log
socket = l:TCP_NODELAY=1
socket = r:TCP_NODELAY=1
debug = 7
client = yes

[https]
accept = localhost:8080
connect = int44.zakupki.gov.ru:443
EOF

# Добавляем PIN-код, если указан
if [ -n "$PIN_CODE" ]; then
    echo "pin = $PIN_CODE" >> "$STUNNEL_CONF"
fi

# Добавляем CAfile, если существует
if [ "$USE_CAFILE" -eq 1 ]; then
    echo "CAfile = $CAFILE_PATH" >> "$STUNNEL_CONF"
fi

# Добавляем verify = 0 (согласно рабочей конфигурации)
echo "verify = 0" >> "$STUNNEL_CONF"

chmod 644 "$STUNNEL_CONF"
echo -e "${GREEN}✓ Конфигурация создана: $STUNNEL_CONF${NC}"

# Показываем содержимое конфигурации
echo -e "${YELLOW}Содержимое конфигурации:${NC}"
cat "$STUNNEL_CONF"

echo ""
echo -e "${GREEN}=========================================="
echo "Настройка завершена!"
echo "==========================================${NC}"
echo ""
echo -e "${YELLOW}Следующие шаги:${NC}"
echo "1. Убедитесь, что сертификат находится в $CERT_PATH"
echo "2. Убедитесь, что закрытый ключ доступен на Linux сервере"
echo "3. Если требуется изменить PIN-код, отредактируйте $STUNNEL_CONF"
echo ""
echo -e "${YELLOW}Запуск Stunnel:${NC}"
echo "  $STUNNEL_EXE $STUNNEL_CONF"
echo ""
echo -e "${YELLOW}Проверка работы:${NC}"
echo "  curl http://localhost:8080"
echo "  (должен вернуть 200)"
echo ""
echo -e "${YELLOW}Проверка логов:${NC}"
echo "  tail -f /var/opt/cprocsp/tmp/stunnel.log"
echo ""
echo -e "${YELLOW}Остановка Stunnel:${NC}"
echo "  kill \$(cat /var/opt/cprocsp/tmp/stunnel_cli.pid)"
echo ""
