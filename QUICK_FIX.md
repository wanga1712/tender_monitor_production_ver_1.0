# СРОЧНОЕ ИСПРАВЛЕНИЕ - БЫСТРЫЙ СТАРТ

## Проблема
- ❌ TabError в contracts_migration.py (строка 449)
- ❌ Nginx настроен вместо stunnel
- ❌ Сервис постоянно перезапускается

## Решение (1 команда!)

### На Windows:

```powershell
.\deploy_fix.ps1
```

Скрипт автоматически:
1. Создаст архив с исправлениями
2. Скопирует на сервер
3. Остановит сервис
4. Создаст бэкап
5. Применит исправления
6. Настроит stunnel
7. Запустит сервис

## Что было исправлено

✅ TabError в `database_work/contracts_migration.py`  
✅ Ссылки на nginx в `main.py`  
✅ Создан скрипт `fix_proxy_config.sh` для настройки stunnel  
✅ Создана документация `FIX_NGINX_ISSUE.md`

## Проверка после деплоя

```bash
ssh nyx
sudo journalctl -u tendermonitor.service -f
```

Должно показать:
```
✅ Stunnel успешно настроен
🚀 Запуск программы TenderMonitor...
✅ Подключение к БД успешно
```

## Если что-то не так

См. подробную инструкцию: [FIX_NGINX_ISSUE.md](FIX_NGINX_ISSUE.md)

## Откат

```bash
ssh nyx
sudo systemctl stop tendermonitor.service
sudo cp /opt/tendermonitor/backups/emergency_*/contracts_migration.py /opt/tendermonitor/database_work/
sudo cp /opt/tendermonitor/backups/emergency_*/main.py /opt/tendermonitor/
sudo systemctl start tendermonitor.service
```
