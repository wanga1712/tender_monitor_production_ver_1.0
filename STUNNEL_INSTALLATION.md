# Инструкция по установке и настройке Stunnel для Linux

## Установка КриптоПро CSP

Stunnel входит в состав КриптоПро CSP. Для установки:

1. **Скачайте КриптоПро CSP для Linux:**
   - Официальный сайт: https://cryptopro.ru/products/csp/downloads
   - Выберите версию: **КриптоПро CSP 5.0.12000** с поддержкой Stunnel и ГОСТ2012
   - Для RedOS 7.3.3 выберите соответствующий пакет (RPM)

2. **Установите КриптоПро CSP:**
   ```bash
   # Для RedOS/CentOS/RHEL
   sudo rpm -ivh cprocsp-*.rpm
   
   # Или через yum (если репозиторий настроен)
   sudo yum install cprocsp-*
   ```

3. **Проверьте установку:**
   ```bash
   # Проверка наличия stunnel
   ls -la /opt/cprocsp/sbin/amd64/stunnel*
   
   # Должны быть файлы:
   # - stunnel_thread
   # - stunnel_fork
   ```

## Настройка Stunnel

### Автоматическая настройка (рекомендуется)

1. Скопируйте скрипт на сервер:
   ```bash
   scp setup_stunnel_linux.sh wanga@nyx:/tmp/
   ```

2. На сервере выполните:
   ```bash
   ssh nyx
   sudo chmod +x /tmp/setup_stunnel_linux.sh
   sudo /tmp/setup_stunnel_linux.sh
   ```

Скрипт автоматически:
- Создаст необходимые директории
- Проверит наличие сертификата
- Создаст конфигурацию stunnel.conf с параметрами из рабочей версии Windows
- Запросит PIN-код для сертификата

### Ручная настройка

Если предпочитаете настроить вручную:

1. **Создайте директории:**
   ```bash
   sudo mkdir -p /etc/opt/cprocsp/stunnel
   sudo mkdir -p /var/opt/cprocsp/tmp
   sudo chmod 755 /etc/opt/cprocsp/stunnel
   sudo chmod 755 /var/opt/cprocsp/tmp
   ```

2. **Скопируйте сертификаты:**
   ```bash
   # Пользовательский сертификат (экспортированный с Windows)
   sudo cp /tmp/certificate.cer /etc/opt/cprocsp/stunnel/1.cer
   sudo chmod 644 /etc/opt/cprocsp/stunnel/1.cer
   
   # Сертификат сервера (если есть)
   sudo cp /tmp/server_cert.cer /etc/opt/cprocsp/stunnel/server_cert.cer
   sudo chmod 644 /etc/opt/cprocsp/stunnel/server_cert.cer
   ```

3. **Создайте конфигурацию stunnel.conf:**
   ```bash
   sudo nano /etc/opt/cprocsp/stunnel/stunnel.conf
   ```

   Содержимое (на основе рабочей конфигурации Windows):
   ```ini
   cert = /etc/opt/cprocsp/stunnel/1.cer
   pid = /var/opt/cprocsp/tmp/stunnel_cli.pid
   output = /var/opt/cprocsp/tmp/stunnel.log
   socket = l:TCP_NODELAY=1
   socket = r:TCP_NODELAY=1
   debug = 7
   client = yes
   
   [https]
   accept = localhost:8080
   connect = int44.zakupki.gov.ru:443
   pin = 0532
   CAfile = /etc/opt/cprocsp/stunnel/server_cert.cer
   verify = 0
   ```

   **Примечания:**
   - Замените `pin = 0532` на ваш PIN-код
   - Если нет CAfile, удалите строку `CAfile = ...`
   - `verify = 0` отключает проверку сертификата сервера (как в рабочей версии)

## Запуск и проверка

1. **Запустите Stunnel:**
   ```bash
   # Используйте stunnel_thread или stunnel_fork
   sudo /opt/cprocsp/sbin/amd64/stunnel_thread /etc/opt/cprocsp/stunnel/stunnel.conf
   
   # Или в фоновом режиме
   sudo /opt/cprocsp/sbin/amd64/stunnel_fork /etc/opt/cprocsp/stunnel/stunnel.conf
   ```

2. **Проверьте, что процесс запущен:**
   ```bash
   ps aux | grep stunnel
   ```

3. **Проверьте, что порт 8080 слушается:**
   ```bash
   netstat -tuln | grep 8080
   # или
   ss -tuln | grep 8080
   ```

4. **Проверьте доступность:**
   ```bash
   curl http://localhost:8080
   # Должен вернуть 200 OK
   ```

5. **Проверьте логи:**
   ```bash
   tail -f /var/opt/cprocsp/tmp/stunnel.log
   ```

## Автозапуск Stunnel через systemd

Для автоматического запуска Stunnel при загрузке системы:

1. **Создайте systemd service:**
   ```bash
   sudo nano /etc/systemd/system/stunnel-eis.service
   ```

2. **Содержимое файла:**
   ```ini
   [Unit]
   Description=Stunnel для подключения к ЕИС
   After=network.target

   [Service]
   Type=forking
   ExecStart=/opt/cprocsp/sbin/amd64/stunnel_fork /etc/opt/cprocsp/stunnel/stunnel.conf
   ExecStop=/bin/kill -TERM $MAINPID
   PIDFile=/var/opt/cprocsp/tmp/stunnel_cli.pid
   Restart=on-failure
   RestartSec=10

   [Install]
   WantedBy=multi-user.target
   ```

3. **Активируйте и запустите:**
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable stunnel-eis
   sudo systemctl start stunnel-eis
   sudo systemctl status stunnel-eis
   ```

## Сравнение конфигураций Windows/Linux

| Параметр | Windows | Linux |
|----------|---------|-------|
| Путь к stunnel | `F:\sertification\st\stunnel_msspi.exe` | `/opt/cprocsp/sbin/amd64/stunnel_thread` |
| Конфиг | `F:\sertification\st\stunnel.conf` | `/etc/opt/cprocsp/stunnel/stunnel.conf` |
| Сертификат | `F:\sertification\user_cert.pfx` | `/etc/opt/cprocsp/stunnel/1.cer` |
| CAfile | `F:\sertification\server_cert.cer` | `/etc/opt/cprocsp/stunnel/server_cert.cer` |
| Логи | `stunnel.log` (в директории stunnel) | `/var/opt/cprocsp/tmp/stunnel.log` |
| PID файл | Не указан | `/var/opt/cprocsp/tmp/stunnel_cli.pid` |

## Устранение проблем

### Ошибка: "certificate file not found"
- Проверьте путь к сертификату: `ls -la /etc/opt/cprocsp/stunnel/1.cer`
- Убедитесь, что файл существует и имеет правильные права (644)

### Ошибка: "msspi_set_mycert_options failed"
- Проверьте, что PIN-код указан правильно в stunnel.conf
- Убедитесь, что закрытый ключ доступен (сертификат установлен в КриптоПро CSP)

### Ошибка: "Error 0x8009030e returned by VerifyCertChain"
- Убедитесь, что в конфигурации указано `verify = 0`
- Проверьте наличие CAfile, если он указан

### Порт 8080 недоступен
- Проверьте, запущен ли Stunnel: `ps aux | grep stunnel`
- Проверьте, не занят ли порт другим процессом: `netstat -tuln | grep 8080`
- Проверьте firewall правила: `sudo firewall-cmd --list-all`

### Stunnel не запускается
- Проверьте логи: `cat /var/opt/cprocsp/tmp/stunnel.log`
- Проверьте права доступа к файлам и директориям
- Убедитесь, что КриптоПро CSP установлен правильно

## Полезные ссылки

- [КриптоПро CSP для Linux](https://cryptopro.ru/products/csp/downloads)
- [Инструкция по настройке Stunnel для Linux](https://www.cryptopro.ru/sites/default/files/products/stunnel/userguidestunnel_linux.pdf)
- [Документация КриптоПро](https://cryptopro.ru/support/documentation)
