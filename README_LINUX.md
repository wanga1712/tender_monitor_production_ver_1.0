# TenderMonitor - Linux версия

## Быстрый старт

### 1. Экспорт сертификата с Windows

На Windows машине запустите:
```powershell
.\export_certificate_windows.ps1
```

Скопируйте полученный `certificate.cer` на Linux сервер.

### 2. Установка на Linux

```bash
# Скопируйте проект на сервер
scp -r TenderMonitor user@server:/tmp/

# Подключитесь к серверу
ssh user@server

# Запустите установку
cd /tmp/TenderMonitor
sudo chmod +x install_linux.sh
sudo ./install_linux.sh
```

### 3. Настройка

1. **Сертификат:**
   ```bash
   sudo cp certificate.cer /etc/opt/cprocsр/stunnel/1.cer
   ```

2. **Конфигурация:**
   ```bash
   cd /opt/tendermonitor
   sudo cp config_linux.ini config.ini
   sudo nano config.ini  # Отредактируйте пути при необходимости
   sudo nano brum.env    # Укажите токен
   ```

3. **База данных:**
   ```bash
   sudo nano database_work/db_credintials.env
   ```

### 4. Запуск

```bash
# Тестовый запуск
sudo -u tendermonitor /opt/tendermonitor/venv/bin/python /opt/tendermonitor/main.py

# Запуск как сервис
sudo systemctl start tendermonitor
sudo systemctl enable tendermonitor

# Просмотр логов
sudo journalctl -u tendermonitor -f
```

## Структура файлов

- `stunnel_runner.py` - Кроссплатформенный модуль запуска Stunnel
- `config_linux.ini` - Конфигурация для Linux
- `install_linux.sh` - Скрипт автоматической установки
- `export_certificate_windows.ps1` - Экспорт сертификата с Windows
- `MIGRATION_GUIDE.md` - Подробное руководство по миграции

## Основные изменения для Linux

1. **Stunnel:** Используется `stunnel_thread`/`stunnel_fork` вместо `stunnel_msspi.exe`
2. **Пути:** Все пути адаптированы для Linux (`/opt/tendermonitor/...`)
3. **Конфигурация:** Stunnel конфиг в `/etc/opt/cprocsр/stunnel/stunnel.conf`
4. **Сертификат:** Хранится в `/etc/opt/cprocsр/stunnel/1.cer`
5. **Логи:** Stunnel логи в `/var/opt/cprocsр/tmp/stunnel.log`

## Требования

- RedOS 7.3.3 или совместимый дистрибутив
- КриптоПро CSP 5.0.12000 с поддержкой Stunnel и ГОСТ2012
- Python 3.8+
- PostgreSQL (если используется БД)

## Подробная документация

См. `MIGRATION_GUIDE.md` для детальной информации.

