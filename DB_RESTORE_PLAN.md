# План восстановления базы данных

## ✅ Информация о текущей БД

### Связи таблицы reestr_contract_223_fz:

**Найдено 4 Foreign Key связи:**

1. **reestr_contract_223_fz_contractor_id_fkey**
   - Колонка: `contractor_id`
   - Ссылается на: `contractor.id`
   - ON UPDATE: NO ACTION
   - ON DELETE: NO ACTION

2. **reestr_contract_223_fz_customer_id_fkey**
   - Колонка: `customer_id`
   - Ссылается на: `customer.id`
   - ON UPDATE: NO ACTION
   - ON DELETE: NO ACTION

3. **reestr_contract_223_fz_okpd_id_fkey**
   - Колонка: `okpd_id`
   - Ссылается на: `collection_codes_okpd.id`
   - ON UPDATE: NO ACTION
   - ON DELETE: NO ACTION

4. **reestr_contract_223_fz_trading_platform_id_fkey**
   - Колонка: `trading_platform_id`
   - Ссылается на: `trading_platform.id`
   - ON UPDATE: NO ACTION
   - ON DELETE: NO ACTION

### Индексы:
- `reestr_contract_223_fz_pkey` (PRIMARY KEY на id)
- `idx_okpd_startdate_223fz` (btree на okpd_id, start_date DESC)
- `idx_reestr_contract_223_fz_status_id` (btree на status_id WHERE status_id IS NOT NULL)

### Текущие данные:
- Всего записей: **324**
- Последняя дата start_date: **2025-12-30**
- Последняя дата end_date: **2026-01-14**

## 📋 План действий

### Шаг 1: Поиск дампа БД

Ищем файлы дампа в стандартных местах:
- `/var/backups/`
- `/var/lib/postgresql/`
- `/home/wanga/`
- `/opt/tendermonitor/`

### Шаг 2: Проверка дампа

После нахождения дампа:
- Проверить формат (SQL, custom, tar)
- Проверить размер
- Проверить структуру таблиц

### Шаг 3: Сохранение информации о связях

**Важно:** Запомнить все 4 связи для восстановления после восстановления дампа.

### Шаг 4: Восстановление дампа

**Варианты восстановления:**

#### Вариант A: pg_restore (для custom/tar формата)
```bash
pg_restore -d Tender_Monitor -c -v dump_file.dump
```

#### Вариант B: psql (для SQL формата)
```bash
psql -d Tender_Monitor -f dump_file.sql
```

#### Вариант C: С восстановлением только данных
```bash
# Сначала структура (если нужно)
psql -d Tender_Monitor -f schema.sql

# Затем данные
pg_restore -d Tender_Monitor --data-only -t reestr_contract_223_fz dump_file.dump
```

### Шаг 5: Восстановление связей

Если связи не восстановились автоматически, восстановить вручную:

```sql
-- Связь с contractor
ALTER TABLE reestr_contract_223_fz
ADD CONSTRAINT reestr_contract_223_fz_contractor_id_fkey
FOREIGN KEY (contractor_id) REFERENCES contractor(id);

-- Связь с customer
ALTER TABLE reestr_contract_223_fz
ADD CONSTRAINT reestr_contract_223_fz_customer_id_fkey
FOREIGN KEY (customer_id) REFERENCES customer(id);

-- Связь с okpd
ALTER TABLE reestr_contract_223_fz
ADD CONSTRAINT reestr_contract_223_fz_okpd_id_fkey
FOREIGN KEY (okpd_id) REFERENCES collection_codes_okpd(id);

-- Связь с trading_platform
ALTER TABLE reestr_contract_223_fz
ADD CONSTRAINT reestr_contract_223_fz_trading_platform_id_fkey
FOREIGN KEY (trading_platform_id) REFERENCES trading_platform(id);
```

### Шаг 6: Проверка последней даты в восстановленной БД

После восстановления проверить:
- Какая последняя дата в таблице
- Количество записей

### Шаг 7: Обновление config.ini

Установить дату в config.ini на день после последней даты в восстановленной БД.
