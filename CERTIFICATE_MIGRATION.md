# Инструкция по переносу сертификата с Windows на Linux

## Шаг 1: Экспорт сертификата на Windows

На Windows используется файл `.pfx` (например, `F:\sertification\user_cert.pfx`), но для Linux нужен формат `.cer`.

### Вариант 1: Экспорт через PowerShell (если сертификат в реестре)

1. Откройте PowerShell от имени администратора
2. Перейдите в директорию проекта:
   ```powershell
   cd C:\Users\wangr\PycharmProjects\pythonProject97
   ```
3. Запустите скрипт экспорта:
   ```powershell
   .\export_certificate_windows.ps1
   ```
4. Выберите нужный сертификат из списка (если их несколько)
5. Сертификат будет сохранен в файл `certificate.cer` в текущей директории

### Вариант 2: Конвертация из .pfx (если есть .pfx файл)

Если у вас есть `.pfx` файл (например, `F:\sertification\user_cert.pfx`), конвертируйте его в `.cer`:

```powershell
# Экспорт сертификата из .pfx в .cer (без закрытого ключа)
$pfxPath = "F:\sertification\user_cert.pfx"
$cerPath = "certificate.cer"
$password = ConvertTo-SecureString -String "0532" -Force -AsPlainText

$pfx = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2($pfxPath, $password)
$certBytes = $pfx.Export([System.Security.Cryptography.X509Certificates.X509ContentType]::Cert)
[System.IO.File]::WriteAllBytes($cerPath, $certBytes)

Write-Host "Сертификат экспортирован в: $cerPath"
```

**Важно:** На Linux сервере также должен быть доступен закрытый ключ. Убедитесь, что сертификат установлен в КриптоПро CSP на Linux с доступом к закрытому ключу.

## Шаг 2: Копирование сертификатов на Linux сервер

### Копирование пользовательского сертификата

**Вариант 1: Через SCP (рекомендуется)**

На Windows машине выполните:
```powershell
scp certificate.cer wanga@nyx:/tmp/certificate.cer
```

**Вариант 2: Через WinSCP или другой SFTP клиент**

1. Подключитесь к серверу nyx
2. Скопируйте файл `certificate.cer` во временную директорию `/tmp/`

### Копирование сертификата сервера (CAfile)

Если у вас есть `server_cert.cer` (например, `F:\sertification\server_cert.cer`), также скопируйте его:

```powershell
scp F:\sertification\server_cert.cer wanga@nyx:/tmp/server_cert.cer
```

## Шаг 3: Установка сертификатов на Linux сервере

Подключитесь к серверу nyx по SSH и выполните:

```bash
# Создаем директорию для сертификатов (если не создана)
sudo mkdir -p /etc/opt/cprocsp/stunnel

# Копируем пользовательский сертификат
sudo cp /tmp/certificate.cer /etc/opt/cprocsp/stunnel/1.cer
sudo chmod 644 /etc/opt/cprocsp/stunnel/1.cer

# Копируем сертификат сервера (если есть)
if [ -f /tmp/server_cert.cer ]; then
    sudo cp /tmp/server_cert.cer /etc/opt/cprocsp/stunnel/server_cert.cer
    sudo chmod 644 /etc/opt/cprocsp/stunnel/server_cert.cer
fi

# Проверяем, что файлы скопированы
ls -la /etc/opt/cprocsp/stunnel/
```

## Шаг 4: Установка закрытого ключа на Linux

**ВАЖНО:** Сертификат должен быть установлен в КриптоПро CSP на Linux сервере с доступом к закрытому ключу.

### Если ключ в контейнере:

1. Установите сертификат через КриптоПро CSP:
   ```bash
   /opt/cprocsp/bin/amd64/certmgr -inst -store uMy -file /etc/opt/cprocsp/stunnel/1.cer
   ```

2. Проверьте установку:
   ```bash
   /opt/cprocsp/bin/amd64/certmgr -list -store uMy
   ```

### Если требуется PIN-код:

Добавьте в конфигурацию stunnel.conf в секцию `[https]`:
```ini
pin = <ваш_пинкод>
```

## Шаг 5: Настройка Stunnel

Запустите скрипт настройки на сервере:

```bash
# Скопируйте скрипт на сервер (если еще не скопирован)
scp setup_stunnel_linux.sh wanga@nyx:/tmp/

# На сервере выполните:
ssh nyx
sudo chmod +x /tmp/setup_stunnel_linux.sh
sudo /tmp/setup_stunnel_linux.sh
```

Или настройте вручную согласно инструкции ниже.

## Шаг 6: Проверка работы

1. Запустите Stunnel:
   ```bash
   sudo /opt/cprocsp/sbin/amd64/stunnel_thread /etc/opt/cprocsp/stunnel/stunnel.conf
   # или
   sudo /opt/cprocsp/sbin/amd64/stunnel_fork /etc/opt/cprocsp/stunnel/stunnel.conf
   ```

2. Проверьте, что порт 8080 слушается:
   ```bash
   netstat -tuln | grep 8080
   # или
   ss -tuln | grep 8080
   ```

3. Проверьте доступность:
   ```bash
   curl http://localhost:8080
   # Должен вернуть 200 OK
   ```

4. Проверьте логи:
   ```bash
   tail -f /var/opt/cprocsp/tmp/stunnel.log
   ```

## Возможные проблемы

### Ошибка: "certificate file not found"
- Проверьте путь к сертификату в stunnel.conf
- Убедитесь, что файл существует: `ls -la /etc/opt/cprocsp/stunnel/1.cer`

### Ошибка: "msspi_set_mycert_options failed"
- Добавьте PIN-код в stunnel.conf:
  ```ini
  pin = <ваш_пинкод>
  ```

### Ошибка: "Error 0x8009030e returned by VerifyCertChain"
- Добавьте в секцию `[https]`:
  ```ini
  verify = 0
  ```

### Порт 8080 недоступен
- Проверьте, запущен ли Stunnel: `ps aux | grep stunnel`
- Проверьте, не занят ли порт: `netstat -tuln | grep 8080`
- Проверьте firewall правила

## Автозапуск Stunnel (опционально)

Для автоматического запуска Stunnel при загрузке системы создайте systemd service:

```bash
sudo nano /etc/systemd/system/stunnel-eis.service
```

Содержимое:
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

Затем:
```bash
sudo systemctl daemon-reload
sudo systemctl enable stunnel-eis
sudo systemctl start stunnel-eis
sudo systemctl status stunnel-eis
```
