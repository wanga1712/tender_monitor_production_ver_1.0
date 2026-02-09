#!/bin/bash
# Скрипт развертывания обновлений TenderMonitor на сервере Linux
# Использование: ./deploy_updates.sh

set -e  # Остановить выполнение при ошибке

echo "=========================================="
echo "Развертывание обновлений TenderMonitor"
echo "=========================================="
echo ""

# Переменные
PROJECT_DIR="/opt/tendermonitor"  # Путь к проекту на сервере (проверено)
BACKUP_DIR="$PROJECT_DIR/backups/$(date +%Y%m%d_%H%M%S)"
ARCHIVE_PATH="/tmp/updates.tar.gz"

# Проверка, что архив существует
if [ ! -f "$ARCHIVE_PATH" ]; then
    echo "❌ Ошибка: Архив $ARCHIVE_PATH не найден!"
    echo "   Сначала перенесите архив на сервер с помощью scp"
    exit 1
fi

# Проверка, что директория проекта существует
if [ ! -d "$PROJECT_DIR" ]; then
    echo "❌ Ошибка: Директория проекта $PROJECT_DIR не найдена!"
    echo "   Проверьте путь к проекту"
    exit 1
fi

echo "📁 Директория проекта: $PROJECT_DIR"
echo "💾 Директория резервных копий: $BACKUP_DIR"
echo ""

# Создать директорию для резервных копий
echo "📦 Создание резервных копий..."
mkdir -p "$BACKUP_DIR"

# Создать резервные копии изменяемых файлов
if [ -f "$PROJECT_DIR/orchestration/monitoring_service.py" ]; then
    cp "$PROJECT_DIR/orchestration/monitoring_service.py" "$BACKUP_DIR/"
    echo "   ✅ Сохранена копия: orchestration/monitoring_service.py"
fi

if [ -f "$PROJECT_DIR/parsing_xml/okpd_parser.py" ]; then
    cp "$PROJECT_DIR/parsing_xml/okpd_parser.py" "$BACKUP_DIR/"
    echo "   ✅ Сохранена копия: parsing_xml/okpd_parser.py"
fi

if [ -f "$PROJECT_DIR/file_downloader.py" ]; then
    cp "$PROJECT_DIR/file_downloader.py" "$BACKUP_DIR/"
    echo "   ✅ Сохранена копия: file_downloader.py"
fi

if [ -f "$PROJECT_DIR/eis_requester.py" ]; then
    cp "$PROJECT_DIR/eis_requester.py" "$BACKUP_DIR/"
    echo "   ✅ Сохранена копия: eis_requester.py"
fi

if [ -f "$PROJECT_DIR/config.ini" ]; then
    cp "$PROJECT_DIR/config.ini" "$BACKUP_DIR/"
    echo "   ✅ Сохранена копия: config.ini"
fi

echo ""

# Остановить сервис
echo "🛑 Остановка сервиса tendermonitor..."
sudo systemctl stop tendermonitor.service
echo "   ✅ Сервис остановлен"
echo ""

# Распаковать обновления
echo "📦 Распаковка обновлений..."
cd "$PROJECT_DIR"
tar -xzf "$ARCHIVE_PATH"
echo "   ✅ Обновления распакованы"
echo ""

# Проверить, что файлы обновлены
echo "🔍 Проверка обновленных файлов..."
if [ -f "$PROJECT_DIR/orchestration/monitoring_service.py" ]; then
    echo "   ✅ orchestration/monitoring_service.py - $(stat -c%s "$PROJECT_DIR/orchestration/monitoring_service.py") bytes"
fi

if [ -f "$PROJECT_DIR/parsing_xml/okpd_parser.py" ]; then
    echo "   ✅ parsing_xml/okpd_parser.py - $(stat -c%s "$PROJECT_DIR/parsing_xml/okpd_parser.py") bytes"
fi

if [ -f "$PROJECT_DIR/file_downloader.py" ]; then
    echo "   ✅ file_downloader.py - $(stat -c%s "$PROJECT_DIR/file_downloader.py") bytes"
fi

if [ -f "$PROJECT_DIR/eis_requester.py" ]; then
    echo "   ✅ eis_requester.py - $(stat -c%s "$PROJECT_DIR/eis_requester.py") bytes"
fi
echo ""

# Удалить временный архив
echo "🗑️  Удаление временного архива..."
rm -f "$ARCHIVE_PATH"
echo "   ✅ Архив удален"
echo ""

# Запустить сервис
echo "🚀 Запуск сервиса tendermonitor..."
sudo systemctl start tendermonitor.service
sleep 2
echo "   ✅ Сервис запущен"
echo ""

# Проверить статус сервиса
echo "📊 Проверка статуса сервиса..."
if sudo systemctl is-active --quiet tendermonitor.service; then
    echo "   ✅ Сервис работает корректно"
else
    echo "   ❌ ВНИМАНИЕ: Сервис не запустился!"
    echo "   Проверьте логи: sudo journalctl -u tendermonitor.service -n 50"
    exit 1
fi
echo ""

# Показать последние логи
echo "📋 Последние логи сервиса (последние 20 строк):"
echo "=========================================="
sudo journalctl -u tendermonitor.service -n 20 --no-pager
echo "=========================================="
echo ""

echo "✅ Развертывание завершено успешно!"
echo ""
echo "📝 Полезные команды:"
echo "   - Статус сервиса:    sudo systemctl status tendermonitor.service"
echo "   - Логи в реальном времени: sudo journalctl -u tendermonitor.service -f"
echo "   - Откат изменений:   cp $BACKUP_DIR/* $PROJECT_DIR/"
echo ""
