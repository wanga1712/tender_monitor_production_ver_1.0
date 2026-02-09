#!/bin/bash
# Скрипт для установки и активации таймера миграции статусов

echo "=========================================="
echo "УСТАНОВКА ТАЙМЕРА МИГРАЦИИ СТАТУСОВ"
echo "=========================================="
echo ""

# Копируем файлы systemd
echo "1. Копирование файлов systemd..."
sudo cp /tmp/tendermonitor-status-migration.service /etc/systemd/system/
sudo cp /tmp/tendermonitor-status-migration.timer /etc/systemd/system/
echo "✅ Файлы скопированы"
echo ""

# Перезагружаем systemd
echo "2. Перезагрузка systemd daemon..."
sudo systemctl daemon-reload
echo "✅ Daemon перезагружен"
echo ""

# Включаем и запускаем таймер
echo "3. Включение и запуск таймера..."
sudo systemctl enable tendermonitor-status-migration.timer
sudo systemctl start tendermonitor-status-migration.timer
echo "✅ Таймер включен и запущен"
echo ""

# Проверяем статус
echo "4. Статус таймера:"
sudo systemctl status tendermonitor-status-migration.timer --no-pager -l
echo ""

echo "5. Статус сервиса:"
sudo systemctl status tendermonitor-status-migration.service --no-pager -l
echo ""

echo "6. Следующий запуск таймера:"
sudo systemctl list-timers tendermonitor-status-migration.timer --no-pager
echo ""

echo "=========================================="
echo "УСТАНОВКА ЗАВЕРШЕНА"
echo "=========================================="
echo ""
echo "Для ручного запуска миграции:"
echo "  sudo systemctl start tendermonitor-status-migration.service"
echo ""
echo "Для просмотра логов:"
echo "  journalctl -u tendermonitor-status-migration.service -f"
