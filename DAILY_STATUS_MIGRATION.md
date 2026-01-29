# Ежедневная миграция контрактов по статусам

## Описание

Модуль `database_work/daily_status_migration.py` автоматически переносит контракты между таблицами на основе их статусов и дат.

## Логика миграции

### 1. Из основной таблицы в "Работа комиссии"

**Условие:** `end_date <= 1 день от текущей даты`

Контракты из `reestr_contract_44_fz` и `reestr_contract_223_fz` переносятся в:
- `reestr_contract_44_fz_commission_work`
- `reestr_contract_223_fz_commission_work`

### 2. Из "Работа комиссии" в другие статусы

**Условие 1:** `end_date > 60 дней от текущей даты` → перенос в "неясный"
- `reestr_contract_44_fz_unclear`
- `reestr_contract_223_fz_unclear`

**Условие 2:** `delivery_start_date IS NOT NULL` → перенос в "Разыгранные"
- `reestr_contract_44_fz_awarded`
- `reestr_contract_223_fz_awarded`

## Структура таблиц

Все статусные таблицы создаются автоматически при первом запуске с той же структурой, что и основные таблицы, включая все связи (foreign keys) и индексы.

## Автоматический запуск

Миграция запускается автоматически каждый день в **00:00:00** (12:00 ночи) через systemd timer.

### Установка systemd сервиса и таймера

```bash
# На сервере nyx
cd /opt/tendermonitor

# Копируем файлы сервиса и таймера
sudo cp tendermonitor-status-migration.service /etc/systemd/system/
sudo cp tendermonitor-status-migration.timer /etc/systemd/system/

# Перезагружаем systemd
sudo systemctl daemon-reload

# Включаем таймер
sudo systemctl enable tendermonitor-status-migration.timer

# Запускаем таймер
sudo systemctl start tendermonitor-status-migration.timer

# Проверяем статус
sudo systemctl status tendermonitor-status-migration.timer
```

### Ручной запуск

```bash
# На сервере nyx
cd /opt/tendermonitor
source venv/bin/activate
python3 database_work/daily_status_migration.py
```

### Просмотр логов

```bash
# Логи systemd
sudo journalctl -u tendermonitor-status-migration.service -n 50

# Логи приложения
tail -f /opt/tendermonitor/errors.log
```

## Бэкап базы данных

Бэкап БД создаётся **не перед каждой миграцией**, а по более щадящим правилам:

- при **первом запуске** модуля (когда ещё нет ни одного бэкапа);
- далее **1 раз в неделю по воскресеньям** (при ночном запуске таймера).

Все бэкапы хранятся в директории `/opt/tendermonitor/backups/`.

Формат имени файла: `tendermonitor_backup_YYYYMMDD_HHMMSS.sql`

Хранится **только один** последний бэкап, все более старые файлы автоматически удаляются.

## Обновление логики вставки/обновления

Модули `database_id_fetcher.py` и `database_operations.py` обновлены для работы со всеми статусными таблицами:

- При поиске контракта по `contract_number` проверяются все статусные таблицы
- При обновлении контракта автоматически определяется, в какой таблице он находится

## Важные замечания

1. **Основной модуль обновления** (`main.py`) продолжает работать только с основными таблицами (`reestr_contract_44_fz` и `reestr_contract_223_fz`)

2. **После переноса** контракты удаляются из исходной таблицы, чтобы избежать дублирования

3. **Связи (foreign keys)** сохраняются при переносе, так как используются те же ID

4. **Миграция выполняется батчами** по 50 контрактов для оптимизации производительности

## Результаты миграции

Модуль возвращает словарь с результатами:

```python
{
    'success': True,
    'backup_file': '/opt/tendermonitor/backups/tendermonitor_backup_...',
    '44_fz': {
        'main_to_commission': {'migrated': 10, 'deleted': 10},
        'commission_migration': {
            'unclear_migrated': 5,
            'unclear_deleted': 5,
            'awarded_migrated': 3,
            'awarded_deleted': 3
        }
    },
    '223_fz': {
        'main_to_commission': {'migrated': 8, 'deleted': 8},
        'commission_migration': {
            'unclear_migrated': 4,
            'unclear_deleted': 4,
            'awarded_migrated': 2,
            'awarded_deleted': 2
        }
    }
}
```

## Устранение неполадок

### Ошибка "table does not exist"

Убедитесь, что статусные таблицы созданы:
```bash
python3 -c "from database_work.daily_status_migration import check_and_create_status_tables; check_and_create_status_tables()"
```

### Ошибка при создании бэкапа

Проверьте права доступа к директории `/opt/tendermonitor/backups/`:
```bash
sudo mkdir -p /opt/tendermonitor/backups
sudo chown wanga:wanga /opt/tendermonitor/backups
```

### Таймер не запускается

Проверьте статус таймера:
```bash
sudo systemctl status tendermonitor-status-migration.timer
sudo systemctl list-timers tendermonitor-status-migration.timer
```
