#!/bin/bash
# Скрипт для проверки статуса сервера и миграции

echo "=========================================="
echo "ПРОВЕРКА СТАТУСА СЕРВИСА TENDERMONITOR"
echo "=========================================="
echo ""

echo "1. Статус systemd сервиса:"
systemctl status tendermonitor.service --no-pager -l | head -20
echo ""

echo "2. Статус systemd таймера миграции:"
systemctl status tendermonitor-status-migration.timer --no-pager -l 2>/dev/null || echo "Таймер не найден"
echo ""

echo "3. Последние логи сервиса (последние 30 строк):"
journalctl -u tendermonitor.service -n 30 --no-pager
echo ""

echo "4. Последние ошибки из errors.log (последние 20 строк):"
if [ -f /opt/tendermonitor/errors.log ]; then
    tail -20 /opt/tendermonitor/errors.log
else
    echo "Файл errors.log не найден"
fi
echo ""

echo "5. Использование памяти:"
free -h
echo ""

echo "6. Использование диска:"
df -h /opt/tendermonitor 2>/dev/null || df -h /
echo ""

echo "7. Процессы Python (tendermonitor):"
ps aux | grep -E "python.*tendermonitor|python.*main.py" | grep -v grep
echo ""

echo "8. Проверка последнего запуска миграции:"
if [ -f /opt/tendermonitor/database_work/daily_status_migration.py ]; then
    echo "Файл миграции найден"
    # Попробуем найти логи миграции
    journalctl -u tendermonitor-status-migration.service -n 50 --no-pager 2>/dev/null || echo "Логи миграции не найдены"
else
    echo "Файл миграции не найден"
fi
echo ""

echo "=========================================="
echo "ПРОВЕРКА ЗАВЕРШЕНА"
echo "=========================================="
