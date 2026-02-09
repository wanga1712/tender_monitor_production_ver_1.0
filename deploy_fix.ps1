#!/usr/bin/env pwsh
# Скрипт для быстрого деплоя исправлений на сервер
# Использование: .\deploy_fix.ps1

$ErrorActionPreference = "Stop"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Деплой исправлений на сервер nyx" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# Параметры подключения
$SERVER = "nyx"
$USER = "wanga"

# Файлы для деплоя
$FILES_TO_DEPLOY = @(
    "database_work/contracts_migration.py",
    "main.py",
    "fix_proxy_config.sh",
    "setup_stunnel_linux.sh",
    "FIX_NGINX_ISSUE.md"
)

Write-Host "Шаг 1: Создание архива с исправлениями..." -ForegroundColor Yellow

# Создаем временный архив
$ARCHIVE = "emergency_fix.tar.gz"

# Удаляем старый архив если есть
if (Test-Path $ARCHIVE) {
    Remove-Item $ARCHIVE
}

# Создаем архив
tar -czf $ARCHIVE $FILES_TO_DEPLOY

if (-not (Test-Path $ARCHIVE)) {
    Write-Host "❌ Ошибка создания архива!" -ForegroundColor Red
    exit 1
}

Write-Host "✓ Архив создан: $ARCHIVE" -ForegroundColor Green
Write-Host ""

Write-Host "Шаг 2: Копирование на сервер..." -ForegroundColor Yellow

# Копируем архив на сервер
scp $ARCHIVE "${USER}@${SERVER}:/tmp/"

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Ошибка копирования на сервер!" -ForegroundColor Red
    exit 1
}

Write-Host "✓ Архив скопирован на сервер" -ForegroundColor Green
Write-Host ""

Write-Host "Шаг 3: Применение исправлений на сервере..." -ForegroundColor Yellow
Write-Host "Подключение к серверу..." -ForegroundColor Gray

# Создаем временный bash скрипт для выполнения на сервере
$DEPLOY_SCRIPT = @'
#!/bin/bash
set -e

PROJECT_DIR="/opt/tendermonitor"
ARCHIVE="emergency_fix.tar.gz"

echo "🛑 Остановка сервиса..."
sudo systemctl stop tendermonitor.service

echo "💾 Создание бэкапа..."
BACKUP_DIR="$PROJECT_DIR/backups/emergency_$(date +%Y%m%d_%H%M%S)"
sudo mkdir -p "$BACKUP_DIR"
sudo cp "$PROJECT_DIR/database_work/contracts_migration.py" "$BACKUP_DIR/" 2>/dev/null || true
sudo cp "$PROJECT_DIR/main.py" "$BACKUP_DIR/" 2>/dev/null || true
echo "✓ Бэкап создан: $BACKUP_DIR"

echo "📦 Распаковка обновлений..."
cd "$PROJECT_DIR"
sudo tar -xzf "/tmp/$ARCHIVE"
echo "✓ Обновления распакованы"

echo "🔧 Исправление прав доступа..."
sudo chmod +x "$PROJECT_DIR/fix_proxy_config.sh"
sudo chmod +x "$PROJECT_DIR/setup_stunnel_linux.sh"
echo "✓ Права исправлены"

echo "🔄 Исправление конфигурации прокси..."
sudo "$PROJECT_DIR/fix_proxy_config.sh"

echo "🚀 Запуск сервиса..."
sudo systemctl start tendermonitor.service
sleep 3

echo "📊 Проверка статуса..."
if sudo systemctl is-active --quiet tendermonitor.service; then
    echo "✅ Сервис запущен успешно!"
    sudo journalctl -u tendermonitor.service -n 20 --no-pager
else
    echo "❌ Сервис не запустился!"
    sudo journalctl -u tendermonitor.service -n 50 --no-pager
    exit 1
fi
'@

# Сохраняем скрипт во временный файл с LF (Linux) переносами строк
$DEPLOY_SCRIPT = $DEPLOY_SCRIPT -replace "`r`n", "`n"
[System.IO.File]::WriteAllText("$PWD/deploy_remote.sh", $DEPLOY_SCRIPT)

# Копируем скрипт на сервер
scp deploy_remote.sh "${USER}@${SERVER}:/tmp/"

# Выполняем скрипт на сервере (с -t для ввода пароля sudo)
ssh -t "${USER}@${SERVER}" "bash /tmp/deploy_remote.sh"

# Удаляем временный скрипт
Remove-Item deploy_remote.sh

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "❌ Ошибка при применении исправлений!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Для отката изменений выполните на сервере:" -ForegroundColor Yellow
    Write-Host "  ssh $SERVER" -ForegroundColor White
    Write-Host "  sudo systemctl stop tendermonitor.service" -ForegroundColor White
    Write-Host "  sudo cp /opt/tendermonitor/backups/emergency_*/contracts_migration.py /opt/tendermonitor/database_work/" -ForegroundColor White
    Write-Host "  sudo cp /opt/tendermonitor/backups/emergency_*/main.py /opt/tendermonitor/" -ForegroundColor White
    Write-Host "  sudo systemctl start tendermonitor.service" -ForegroundColor White
    exit 1
}

Write-Host ""
Write-Host "==========================================" -ForegroundColor Green
Write-Host "✅ Исправления применены успешно!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host ""

Write-Host "Проверьте логи на сервере:" -ForegroundColor Yellow
Write-Host "  ssh $SERVER" -ForegroundColor White
Write-Host "  sudo journalctl -u tendermonitor.service -f" -ForegroundColor White
Write-Host ""

# Удаляем временный архив
Remove-Item $ARCHIVE
Write-Host "✓ Временный архив удален" -ForegroundColor Green
