# Исправление проблемы с nginx

## Проблема

Кто-то из AI моделей настроил **nginx** вместо **stunnel** на production сервере. Это привело к двум критическим ошибкам:

1. **TabError** в `database_work/contracts_migration.py` (строка 449)
2. **Nginx не запущен** - программа ищет nginx, но должна использовать stunnel

## Почему это проблема?

- Программа рассчитана на работу со **stunnel** через КриптоПро CSP
- Требуется поддержка **ГОСТ сертификатов**
- Nginx **НЕ поддерживает** ГОСТ криптографию

## Быстрое исправление

### На локальной машине (Windows)

1. **Исправить TabError** (уже исправлено в локальной копии):
   ```powershell
   # Файл database_work/contracts_migration.py исправлен
   # Теперь нужно залить на сервер
   ```

2. **Создать архив с исправлениями**:
   ```powershell
   # Из директории проекта
   tar -czf updates.tar.gz `
       database_work/contracts_migration.py `
       main.py `
       fix_proxy_config.sh `
       setup_stunnel_linux.sh
   ```

3. **Скопировать на сервер**:
   ```powershell
   scp updates.tar.gz wanga@nyx:/tmp/
   scp fix_proxy_config.sh wanga@nyx:/tmp/
   ```

### На сервере (Linux)

1. **Остановить сервис**:
   ```bash
   ssh nyx
   sudo systemctl stop tendermonitor.service
   ```

2. **Применить исправления**:
   ```bash
   cd /opt/tendermonitor
   sudo tar -xzf /tmp/updates.tar.gz
   ```

3. **Исправить конфигурацию прокси**:
   ```bash
   sudo chmod +x /tmp/fix_proxy_config.sh
   sudo /tmp/fix_proxy_config.sh
   ```

   Этот скрипт:
   - Остановит и отключит nginx
   - Проверит наличие КриптоПро CSP
   - Запустит stunnel
   - Проверит порт 8080

4. **Запустить сервис**:
   ```bash
   sudo systemctl start tendermonitor.service
   ```

5. **Проверить логи**:
   ```bash
   sudo journalctl -u tendermonitor.service -f
   ```

## Если КриптоПро CSP не установлен

Если скрипт сообщает, что КриптоПро CSP не найден:

1. **Скачайте КриптоПро CSP** для Linux (RedOS 7.3.3):
   - https://cryptopro.ru/products/csp/downloads

2. **Установите на сервере**:
   ```bash
   sudo rpm -ivh cprocsp-*.rpm
   ```

3. **Настройте stunnel**:
   ```bash
   sudo chmod +x /opt/tendermonitor/setup_stunnel_linux.sh
   sudo /opt/tendermonitor/setup_stunnel_linux.sh
   ```

4. **Скопируйте сертификат** с Windows:
   ```powershell
   scp certificate.cer wanga@nyx:/tmp/
   ```

   На сервере:
   ```bash
   sudo cp /tmp/certificate.cer /etc/opt/cprocsp/stunnel/1.cer
   sudo chmod 644 /etc/opt/cprocsp/stunnel/1.cer
   ```

5. **Запустите fix_proxy_config.sh** снова

## Проверка результата

После исправления должно быть:

```bash
# Nginx остановлен
sudo systemctl status nginx
# Должно показать: inactive (dead)

# Stunnel запущен
netstat -tuln | grep 8080
# Должно показать: tcp  0  0 127.0.0.1:8080  0.0.0.0:*  LISTEN

# Сервис работает
sudo systemctl status tendermonitor.service
# Должно показать: active (running)

# Нет ошибок в логах
sudo journalctl -u tendermonitor.service -n 50
# Должно показать: ✅ Stunnel успешно настроен
```

## Что было исправлено

### 1. TabError в contracts_migration.py
**Строка 449**: Неправильный отступ (табы вместо пробелов)

**Было**:
```python
                    debug_log("C", "contracts_migration.py:433", "Батч удален", {
```

**Стало**:
```python
                        debug_log("C", "contracts_migration.py:433", "Батч удален", {
```

### 2. Ссылки на nginx в main.py
**Строки 269-280**: Сообщения об ошибках ссылались на nginx

**Было**:
```python
print("⚠️  Программа завершена из-за ошибки Nginx.")
print("   Проверьте статус: systemctl status nginx")
```

**Стало**:
```python
print("⚠️  Программа завершена из-за ошибки прокси.")
print("   Проверьте конфигурацию stunnel на сервере.")
```

## Автоматизация (опционально)

Для автоматического запуска stunnel при загрузке системы:

1. Создайте systemd service:
   ```bash
   sudo nano /etc/systemd/system/cprocsp-stunnel.service
   ```

2. Добавьте:
   ```ini
   [Unit]
   Description=CryptoPro Stunnel
   After=network.target

   [Service]
   Type=forking
   ExecStart=/opt/cprocsp/sbin/amd64/stunnel_thread /etc/opt/cprocsp/stunnel/stunnel.conf
   PIDFile=/var/opt/cprocsp/tmp/stunnel_cli.pid
   Restart=always

   [Install]
   WantedBy=multi-user.target
   ```

3. Активируйте:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable cprocsp-stunnel.service
   sudo systemctl start cprocsp-stunnel.service
   ```

## Полезные ссылки

- [STUNNEL_INSTALLATION.md](file:///c:/Users/wangr/PycharmProjects/pythonProject97/STUNNEL_INSTALLATION.md) - Полная инструкция по установке stunnel
- [CERTIFICATE_MIGRATION.md](file:///c:/Users/wangr/PycharmProjects/pythonProject97/CERTIFICATE_MIGRATION.md) - Перенос сертификатов
- [QUICK_START_STUNNEL.md](file:///c:/Users/wangr/PycharmProjects/pythonProject97/QUICK_START_STUNNEL.md) - Быстрый старт
