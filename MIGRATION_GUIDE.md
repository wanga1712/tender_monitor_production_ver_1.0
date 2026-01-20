# Руководство по миграции TenderMonitor с Windows на Linux

## Обзор

Это руководство описывает процесс переноса проекта TenderMonitor с Windows на Linux сервер.

## Предварительные требования

### На Windows машине:
- Установленный КриптоПро CSP
- Сертификат в реестре Windows
- Доступ к исходному проекту

### На Linux сервере:
- RedOS 7.3.3 или совместимый дистрибутив
- Python 3.8+
- КриптоПро CSP 5.0.12000 с поддержкой Stunnel и ГОСТ2012
- PostgreSQL (если используется БД)

## Шаг 1: Экспорт сертификата с Windows

1. Откройте PowerShell от имени администратора
2. Перейдите в директорию проекта
3. Запустите скрипт экспорта:
   ```powershell
   .\export_certificate_windows.ps1
   ```
4. Выберите нужный сертификат из списка
5. Сертификат будет сохранен как `certificate.cer`

## Шаг 2: Копирование проекта на Linux сервер

1. Скопируйте все файлы проекта на Linux сервер:
   ```bash
   scp -r /path/to/TenderMonitor user@linux-server:/tmp/
   ```

2. Или используйте git (если проект в репозитории):
   ```bash
   git clone <repository-url>
   ```

## Шаг 3: Установка на Linux сервере

1. Подключитесь к Linux серверу:
   ```bash
   ssh user@linux-server
   ```

2. Перейдите в директорию проекта:
   ```bash
   cd /tmp/TenderMonitor
   ```

3. Запустите скрипт установки (требуются права root):
   ```bash
   sudo chmod +x install_linux.sh
   sudo ./install_linux.sh
   ```

Скрипт выполнит:
- Создание пользователя `tendermonitor`
- Создание необходимых директорий
- Проверку установки КриптоПро CSP
- Установку зависимостей Python
- Настройку конфигурации Stunnel
- Создание systemd service

## Шаг 4: Настройка сертификата

1. Скопируйте экспортированный сертификат на Linux сервер:
   ```bash
   scp certificate.cer user@linux-server:/tmp/
   ```

2. На Linux сервере переместите сертификат:
   ```bash
   sudo mkdir -p /etc/opt/cprocsр/stunnel
   sudo cp /tmp/certificate.cer /etc/opt/cprocsр/stunnel/1.cer
   sudo chmod 644 /etc/opt/cprocsр/stunnel/1.cer
   ```

3. Убедитесь, что закрытый ключ доступен на Linux сервере. Если ключ хранится в контейнере, установите его через КриптоПро CSP.

## Шаг 5: Настройка конфигурации Stunnel

Отредактируйте файл `/etc/opt/cprocsр/stunnel/stunnel.conf`:

```ini
cert = /etc/opt/cprocsр/stunnel/1.cer
pid = /var/opt/cprocsр/tmp/stunnel_cli.pid
output = /var/opt/cprocsр/tmp/stunnel.log
socket=l:TCP_NODELAY=1
socket=r:TCP_NODELAY=1
client = yes

[https]
accept = localhost:8080
connect = int44.zakupki.gov.ru:443
# Если требуется PIN-код, раскомментируйте:
# pin = <ваш_пинкод>
```

## Шаг 6: Настройка конфигурации приложения

1. Скопируйте `config_linux.ini` в `config.ini`:
   ```bash
   cd /opt/tendermonitor
   sudo cp config_linux.ini config.ini
   ```

2. Отредактируйте `config.ini` при необходимости (пути, даты и т.д.)

3. Настройте токен в `brum.env`:
   ```bash
   sudo nano brum.env
   # Укажите ваш токен: TOKEN=your_actual_token
   ```

4. Настройте подключение к БД:
   ```bash
   sudo nano database_work/db_credintials.env
   ```

## Шаг 7: Копирование дополнительных файлов

Скопируйте JSON файлы с тегами:
```bash
sudo cp -r required_tags /opt/tendermonitor/
```

## Шаг 8: Тестирование

1. Проверьте работу Stunnel:
   ```bash
   sudo /opt/cprocsр/sbin/amd64/stunnel_thread /etc/opt/cprocsр/stunnel/stunnel.conf
   ```

2. Проверьте доступность порта:
   ```bash
   curl http://localhost:8080
   # Должен вернуть 200 OK
   ```

3. Протестируйте запуск приложения:
   ```bash
   sudo -u tendermonitor /opt/tendermonitor/venv/bin/python /opt/tendermonitor/main.py
   ```

## Шаг 9: Запуск как сервис

1. Запустите сервис:
   ```bash
   sudo systemctl start tendermonitor
   ```

2. Включите автозапуск:
   ```bash
   sudo systemctl enable tendermonitor
   ```

3. Проверьте статус:
   ```bash
   sudo systemctl status tendermonitor
   ```

4. Просмотр логов:
   ```bash
   sudo journalctl -u tendermonitor -f
   ```

## Решение проблем

### Ошибка: "Error 0x80092004 returned by CertFindCertificateInStore"
**Решение**: Установите сертификат повторно в хранилище Личное со ссылкой на контейнер ключа.

### Ошибка: "Error 0x8009030e returned by VerifyCertChain"
**Решение**: Добавьте в `stunnel.conf` в секцию `[https]`:
```ini
verify = 0
```

### Ошибка: "msspi_set_mycert_options failed"
**Решение**: Добавьте в `stunnel.conf` в секцию `[https]`:
```ini
pin = <ваш_пинкод>
```

### Stunnel не запускается
Проверьте:
- Существует ли файл сертификата: `ls -la /etc/opt/cprocsр/stunnel/1.cer`
- Правильность путей в `stunnel.conf`
- Логи: `cat /var/opt/cprocsр/tmp/stunnel.log`
- Права доступа к файлам

### Порт 8080 недоступен
Проверьте:
- Запущен ли Stunnel: `ps aux | grep stunnel`
- Не занят ли порт: `netstat -tuln | grep 8080`
- Firewall правила

## Основные отличия Windows/Linux

| Компонент | Windows | Linux |
|-----------|---------|-------|
| Stunnel | `stunnel_msspi.exe` | `stunnel_thread` или `stunnel_fork` |
| Конфиг Stunnel | `C:\Stunnel\stunnel.conf` | `/etc/opt/cprocsр/stunnel/stunnel.conf` |
| Сертификат | Реестр Windows | `/etc/opt/cprocsр/stunnel/1.cer` |
| Логи Stunnel | `C:\Stunnel\stunnel.log` | `/var/opt/cprocsр/tmp/stunnel.log` |
| Пути данных | `F:\...` | `/opt/tendermonitor/data/...` |

## Полезные ссылки

- [КриптоПро CSP для Linux](https://cryptopro.ru/products/csp/downloads)
- [Инструкция по настройке Stunnel для Linux](https://www.cryptopro.ru/sites/default/files/products/stunnel/userguidestunnel_linux.pdf)
- [Документация КриптоПро](https://cryptopro.ru/support/documentation)

## Поддержка

При возникновении проблем проверьте:
1. Логи приложения: `journalctl -u tendermonitor -n 100`
2. Логи Stunnel: `cat /var/opt/cprocsр/tmp/stunnel.log`
3. Логи ошибок приложения: `/opt/tendermonitor/errors.log`

