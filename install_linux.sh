#!/bin/bash
# Скрипт установки TenderMonitor на Linux сервер
# Требуется: RedOS 7.3.3 или совместимый дистрибутив

set -e  # Прерывать выполнение при ошибке

echo "=========================================="
echo "Установка TenderMonitor на Linux"
echo "=========================================="

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Проверка прав root
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Ошибка: Запустите скрипт с правами root (sudo)${NC}"
    exit 1
fi

# Определяем базовую директорию
INSTALL_DIR="/opt/tendermonitor"
SERVICE_USER="tendermonitor"

echo -e "${YELLOW}Шаг 1: Создание пользователя и директорий...${NC}"

# Создаем пользователя для сервиса (если не существует)
if ! id "$SERVICE_USER" &>/dev/null; then
    useradd -r -s /bin/bash -d "$INSTALL_DIR" "$SERVICE_USER"
    echo -e "${GREEN}✓ Пользователь $SERVICE_USER создан${NC}"
else
    echo -e "${YELLOW}✓ Пользователь $SERVICE_USER уже существует${NC}"
fi

# Создаем директории
mkdir -p "$INSTALL_DIR"
mkdir -p "$INSTALL_DIR/data/44_FZ/xml_reestr_44_fz_new_contracts"
mkdir -p "$INSTALL_DIR/data/44_FZ/xml_reestr_44_new_contracts_recouped"
mkdir -p "$INSTALL_DIR/data/223_FZ/xml_reestr_223_fz_new_contracts"
mkdir -p "$INSTALL_DIR/data/223_FZ/xml_reestr_new_223_contracts_recouped"
mkdir -p "$INSTALL_DIR/data/unziped_xml_files"
mkdir -p "$INSTALL_DIR/required_tags"
mkdir -p /var/opt/cprocsр/tmp

# Устанавливаем права доступа
chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR"
chmod 755 "$INSTALL_DIR"
chmod -R 755 "$INSTALL_DIR/data"

echo -e "${GREEN}✓ Директории созданы${NC}"

echo -e "${YELLOW}Шаг 2: Проверка установки КриптоПро CSP...${NC}"

# Проверяем наличие КриптоПро CSP
if [ ! -f "/opt/cprocsр/sbin/amd64/stunnel_thread" ] && [ ! -f "/opt/cprocsр/sbin/amd64/stunnel_fork" ]; then
    echo -e "${RED}⚠ Внимание: КриптоПро CSP не найден в стандартном месте${NC}"
    echo -e "${YELLOW}Установите КриптоПро CSP 5.0.12000 с поддержкой Stunnel и ГОСТ2012${NC}"
    echo "Ссылка: https://cryptopro.ru/products/csp/downloads"
    read -p "Продолжить установку? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    echo -e "${GREEN}✓ КриптоПро CSP найден${NC}"
fi

echo -e "${YELLOW}Шаг 3: Проверка Python и pip...${NC}"

# Проверяем Python 3
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Ошибка: Python 3 не установлен${NC}"
    echo "Установите Python 3: yum install python3 python3-pip"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
echo -e "${GREEN}✓ Python $PYTHON_VERSION найден${NC}"

# Проверяем pip
if ! command -v pip3 &> /dev/null; then
    echo -e "${YELLOW}Установка pip3...${NC}"
    yum install -y python3-pip || apt-get install -y python3-pip
fi

echo -e "${GREEN}✓ pip3 найден${NC}"

echo -e "${YELLOW}Шаг 4: Создание виртуального окружения Python...${NC}"

cd "$INSTALL_DIR"
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}✓ Виртуальное окружение создано${NC}"
else
    echo -e "${YELLOW}✓ Виртуальное окружение уже существует${NC}"
fi

echo -e "${YELLOW}Шаг 5: Установка зависимостей Python...${NC}"

source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo -e "${GREEN}✓ Зависимости установлены${NC}"

echo -e "${YELLOW}Шаг 6: Настройка nginx для проксирования к ЕИС...${NC}"

# Проверяем наличие nginx
if ! command -v nginx &> /dev/null; then
    echo -e "${YELLOW}Nginx не установлен. Устанавливаем...${NC}"
    
    # Определяем дистрибутив
    if [ -f /etc/redhat-release ]; then
        yum install -y nginx
    elif [ -f /etc/debian_version ]; then
        apt-get update
        apt-get install -y nginx
    else
        echo -e "${YELLOW}⚠ Установите nginx вручную${NC}"
    fi
fi

# Копируем конфигурацию nginx, если файл существует
if [ -f "$INSTALL_DIR/nginx_eis.conf" ]; then
    cp "$INSTALL_DIR/nginx_eis.conf" /etc/nginx/conf.d/eis.conf
    echo -e "${GREEN}✓ Конфигурация nginx скопирована${NC}"
    
    # Проверяем конфигурацию
    if nginx -t 2>/dev/null; then
        systemctl enable nginx
        systemctl restart nginx
        echo -e "${GREEN}✓ Nginx настроен и запущен${NC}"
    else
        echo -e "${YELLOW}⚠ Ошибка в конфигурации nginx. Проверьте вручную: nginx -t${NC}"
    fi
else
    echo -e "${YELLOW}⚠ Файл nginx_eis.conf не найден. Настройте nginx вручную${NC}"
    echo "  Или запустите: sudo ./setup_nginx.sh"
fi

echo -e "${YELLOW}Шаг 7: Настройка конфигурации приложения...${NC}"

# Копируем config_linux.ini в config.ini, если его нет
if [ ! -f "$INSTALL_DIR/config.ini" ]; then
    if [ -f "$INSTALL_DIR/config_linux.ini" ]; then
        cp "$INSTALL_DIR/config_linux.ini" "$INSTALL_DIR/config.ini"
        echo -e "${GREEN}✓ config.ini создан из config_linux.ini${NC}"
    else
        echo -e "${YELLOW}⚠ config.ini не найден, создайте его вручную${NC}"
    fi
fi

# Создаем .env файл для токена, если его нет
if [ ! -f "$INSTALL_DIR/brum.env" ]; then
    echo "TOKEN=your_token_here" > "$INSTALL_DIR/brum.env"
    chmod 600 "$INSTALL_DIR/brum.env"
    chown "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR/brum.env"
    echo -e "${YELLOW}⚠ Создан шаблон brum.env, укажите ваш токен${NC}"
fi

echo -e "${YELLOW}Шаг 8: Создание systemd service...${NC}"

# Создаем systemd service файл
cat > /etc/systemd/system/tendermonitor.service << EOF
[Unit]
Description=TenderMonitor - Мониторинг тендеров ЕИС
After=network.target postgresql.service

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$INSTALL_DIR
Environment="PATH=$INSTALL_DIR/venv/bin"
ExecStart=$INSTALL_DIR/venv/bin/python $INSTALL_DIR/main.py
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
echo -e "${GREEN}✓ Systemd service создан${NC}"

echo -e "${YELLOW}Шаг 9: Установка прав доступа...${NC}"

chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR"
chmod +x "$INSTALL_DIR/main.py" 2>/dev/null || true

echo -e "${GREEN}✓ Права доступа установлены${NC}"

echo ""
echo -e "${GREEN}=========================================="
echo "Установка завершена!"
echo "==========================================${NC}"
echo ""
echo -e "${YELLOW}Следующие шаги:${NC}"
echo "1. Скопируйте файлы проекта в $INSTALL_DIR"
echo "2. Убедитесь, что сертификат находится в /etc/opt/cprocsр/stunnel/1.cer"
echo "3. Настройте config.ini и brum.env"
echo "4. Настройте подключение к БД в database_work/db_credintials.env"
echo "5. Протестируйте запуск: sudo -u $SERVICE_USER $INSTALL_DIR/venv/bin/python $INSTALL_DIR/main.py"
echo "6. Запустите сервис: systemctl start tendermonitor"
echo "7. Включите автозапуск: systemctl enable tendermonitor"
echo ""
echo -e "${YELLOW}Полезные команды:${NC}"
echo "  Просмотр логов: journalctl -u tendermonitor -f"
echo "  Статус сервиса: systemctl status tendermonitor"
echo "  Остановка: systemctl stop tendermonitor"
echo "  Перезапуск: systemctl restart tendermonitor"
echo ""

