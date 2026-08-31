# S7 EIS ingestion — TenderMonitor

Этот каталог содержит production-контур получения и первичной нормализации данных ЕИС на сервере S7.

Его задача — не AI-классификация и не CRM. Контур:

1. запрашивает данные ЕИС по датам и регионам;
2. скачивает и распаковывает архивы;
3. маршрутизирует XML по 44-ФЗ, 223-ФЗ и опционально 615-ПП;
4. фильтрует/нормализует записи;
5. сохраняет закупки и контракты в PostgreSQL;
6. синхронизирует жизненный цикл контрактов, включая перевод в `*_awarded`;
7. умеет работать в двух режимах: **forward** и **backward**.

> **Production path на S7:** `/opt/tendermonitor`
> **Основной entrypoint:** `/opt/tendermonitor/main.py`

---

## 1. Общая архитектура

```text
systemd
  │
  ├─ tendermonitor-eis-parser.service
  │     └─ forward
  │
  └─ tendermonitor-eis-parser-backward.service
        └─ backward
             │
             ▼
          main.py
             │
             ├─ ProxyRunner / stunnel
             ├─ проверка PostgreSQL
             └─ TenderMonitorService
                    │
                    └─ EISRequester(date)
                           │
                           ├─ SOAP getDocsByOrgRegionRequest
                           ├─ 44-ФЗ: PRIZ / RGK
                           ├─ 223-ФЗ: RI223 / RD223
                           └─ 615-ПП: optional
                                   │
                                   ▼
                            archive URLs
                                   │
                                   ▼
                            FileDownloader
                                   │
                         download → unzip → delete ZIP
                                   │
                                   ▼
                           process_okpd_files()
                                   │
              ┌────────────────────┼─────────────────────┐
              │                    │                     │
              ▼                    ▼                     ▼
        PRIZ / RI223           44-ФЗ RGK              223-ФЗ RD223
        normal XML             batch path             recouped path
              │                    │                     │
              ▼                    ▼                     ▼
          XMLParser          rgk_batch.py       AdvancedXMLParser
                                  │                     │
                                  ▼                     ▼
                             rgk_record.py       RecoupedContractSync
                                  │                     │
                                  ▼              locator → updater
                             rgk_plan.py                 │
                                  │                     ▼
                                  ▼               awarded promoter
                           rgk_batch_store.py
                                  │
                                  ▼
                              PostgreSQL
```

---

## 2. Production-сервисы

Оба systemd-сервиса запускают **один и тот же `main.py`**. Режим работы задаётся окружением.

### Forward

Unit:

```text
tendermonitor-eis-parser.service
```

Основные свойства:

- `WorkingDirectory=/opt/tendermonitor`
- Python из `/opt/tendermonitor/venv`
- `ExecStart=/opt/tendermonitor/venv/bin/python /opt/tendermonitor/main.py`
- `Restart=on-failure`

Forward — основной контур текущих дат.

Он:

- идёт от даты в `config.ini` вперёд;
- сохраняет прогресс по регионам;
- после догоняющей обработки переходит к режиму ожидания свежих данных ЕИС;
- при недоступности данных повторяет проверку;
- после успешной даты переходит к следующей.

### Backward

Unit:

```text
tendermonitor-eis-parser-backward.service
```

Он запускает тот же `main.py`, но с отдельным runtime state:

```text
TENDERMONITOR_CONFIG
TENDERMONITOR_PROCESSED_DATES
TENDERMONITOR_REGION_PROGRESS
TENDERMONITOR_LOG_DIR
TENDERMONITOR_DIRECTION=backward
TENDERMONITOR_STOP_BEFORE=<YYYY-MM-DD>
```

Backward:

- идёт по датам назад;
- не должен использовать progress/config forward-контура;
- не ждёт появления свежих данных ЕИС;
- заканчивает работу при достижении заданной нижней границы;
- имеет более низкий runtime priority, чтобы не мешать основному parser.

### Критический инвариант

**Forward и backward могут использовать общий source code, но не общий mutable runtime state.**

Нельзя направлять оба сервиса на один:

- `config.ini`;
- `region_progress.json`;
- каталог runtime-логов;
- другой изменяемый progress/state-файл.

---

## 3. Управление датами и прогрессом

Оркестрация вынесена из `main.py` в:

```text
orchestration/monitoring_service.py
```

`TenderMonitorService` отвечает за:

- выбор следующей даты;
- forward/backward направление;
- обработку регионов;
- восстановление прогресса после restart;
- очистку region progress после успешно законченной даты;
- переход между датами;
- статистику;
- вызов memory guard между датами.

`main.py` оставляет у себя runtime concerns:

- запуск proxy/stunnel;
- проверку БД;
- чтение конфигурации;
- environment overrides;
- создание `EISRequester`;
- верхнеуровневую обработку критических ошибок.

### Runtime paths

Базовые пути задаются в `config/__init__.py` и могут быть переопределены:

```text
TENDERMONITOR_CONFIG
TENDERMONITOR_PROCESSED_DATES
TENDERMONITOR_REGION_PROGRESS
```

`region_progress.json` — фактический checkpoint обработки регионов конкретной даты.

Не следует считать `processed_dates.json` единственным источником истины о прогрессе: основной цикл опирается также на текущую дату конфигурации и region progress.

---

## 4. Получение данных из ЕИС

Основной клиент:

```text
eis_requester.py
```

EISRequester:

1. получает список регионов из БД;
2. формирует SOAP-запрос на конкретную дату;
3. обходит подсистемы и document types;
4. получает XML-ответ со ссылками на архивы;
5. передаёт архивы в `FileDownloader`;
6. отмечает регион обработанным только после завершения его прохода.

Локальный SOAP endpoint:

```text
http://localhost:8080/eis-integration/services/getDocsIP
```

Доступ к ЕИС идёт через локальный proxy/stunnel-контур.

При `ConnectionError`/`Timeout` клиент повторяет запросы с увеличивающейся паузой. Это означает, что временная недоступность ЕИС сама по себе не должна превращаться в потерю даты.

---

## 5. Источники и маршрутизация

Типовой `config.ini` задаёт следующие подсистемы.

| Контур | Subsystem | Назначение | Основной путь |
|---|---|---|---|
| 44-ФЗ | `PRIZ` | новые извещения/закупки | normal XML → OKPD → `XMLParser` |
| 44-ФЗ | `RGK` | реестр контрактов | bounded batch RGK |
| 223-ФЗ | `RI223` | новые извещения | normal XML → OKPD → `XMLParser` |
| 223-ФЗ | `RD223` | данные реестра/исполнения контрактов | recouped → `AdvancedXMLParser` |
| 615-ПП | `RD615`/configured | закупки 615-ПП | отдельный путь, OKPD filter bypass |

Document types не должны хардкодиться в README или systemd unit — их authoritative runtime contract находится в `config.ini`.

Шаблон конфигурации:

```text
config.ini.example
```

---

## 6. Download → unpack → parse

`file_downloader.py` связывает subsystem с каталогом:

```text
PRIZ   → reest_new_contract_archive_44_fz_xml
RGK    → recouped_contract_archive_44_fz_xml
RI223  → reest_new_contract_archive_223_fz_xml
RD223  → recouped_contract_archive_223_fz_xml
```

Для каждого архива:

```text
URL
 ↓
rewrite_eis_url_via_stunnel()
 ↓
download
 ↓
ArchiveExtractor
 ↓
XML files
 ↓
ZIP deleted
 ↓
process_okpd_files()
```

После распаковки маршрутизация определяется фактическим source directory.

---

## 7. Normal XML: новые закупки

Для новых 44-ФЗ/223-ФЗ используется обычный путь:

```text
process_okpd_files_normal()
        ↓
     XMLParser
        ↓
DatabaseOperations
```

Здесь выполняются:

- чтение XML;
- выделение ОКПД;
- сопоставление с интересующими кодами;
- извлечение canonical полей;
- сохранение закупки и связанных сущностей;
- deduplication по уже известным файлам/записям.

Этот путь не следует смешивать с RGK awarded-логикой: это разные стадии жизненного цикла закупки.

---

## 8. 44-ФЗ RGK: batch-контур

Production path 44-ФЗ RGK отделён от старой per-row схемы:

```text
okpd_parser.py
  ↓
process_44_rgk_folder()
  ↓
rgk_batch.py
  ↓
rgk_record.py
  ↓
rgk_plan.py
  ↓
rgk_batch_store.py
  ↓
PostgreSQL
```

### Зачем он существует

RGK может содержать большое число XML. Выполнять lifecycle lookup и commit отдельно для каждого файла слишком дорого.

Batch-контур:

- разбивает XML на ограниченные партии;
- bulk-проверяет известные filenames;
- bulk-разрешает OKPD;
- bulk-разрешает contractors;
- ищет контракты в lifecycle tables;
- сравнивает новое состояние с существующим;
- пропускает unchanged writes;
- планирует insert/update/promote/unresolved до записи;
- применяет изменения пачкой;
- делает один `COMMIT` на `persist()`.

### Batch size

Environment:

```text
TENDERMONITOR_RGK_BATCH_SIZE
```

Значение по умолчанию:

```text
500
```

Код ограничивает диапазон:

```text
100 .. 2000
```

Изменять размер следует только после измерения:

- elapsed per date;
- DB round-trips;
- memory;
- deadlocks/lock contention;
- число commits;
- количество unchanged records.

### Planner отдельно от persistence

`rgk_plan.py` не делает DB I/O.

Он определяет:

- duplicate;
- insert;
- update;
- unchanged;
- promote;
- unresolved.

`rgk_batch_store.py` отвечает только за lookup/persistence.

Это важная архитектурная граница: **decision logic не должна постепенно возвращаться в SQL/write слой.**

---

## 9. RGK record integrity

Новый контракт нельзя создавать только потому, что RGK XML существует.

Canonical insert разрешён только при наличии достаточных данных.

Минимально требуются:

- валидный `okpd_id`;
- реальный `auction_name`/contract subject.

Запрещено создавать placeholder rows вида:

```text
okpd_id = NULL
auction_name = "Контракт <номер>"
```

Если canonical insert невозможен, запись должна попасть в:

```text
rgk_contract_unresolved
```

с причиной, например:

```text
MISSING_OKPD_ID
MISSING_REAL_TITLE
MISSING_OKPD_AND_TITLE
PLACEHOLDER_TITLE_BLOCKED
```

Идея `unresolved` — сохранить факт и payload для reconciliation, не загрязняя основные реестры выдуманными карточками.

---

## 10. Contract lifecycle

Имена и порядок lifecycle tables централизованы в:

```text
database_work/registry_tables.py
```

### 44-ФЗ

```text
reestr_contract_44_fz
reestr_contract_44_fz_commission_work
reestr_contract_44_fz_unknown
reestr_contract_44_fz_unclear
reestr_contract_44_fz_awarded
reestr_contract_44_fz_completed
```

### 223-ФЗ

```text
reestr_contract_223_fz
reestr_contract_223_fz_commission_work
reestr_contract_223_fz_unclear
reestr_contract_223_fz_awarded
reestr_contract_223_fz_completed
```

### Lookup priority

Обычный полный lifecycle lookup:

```text
main
  ↓
commission_work
  ↓
unknown        # только 44-ФЗ
  ↓
unclear
  ↓
awarded
```

`completed` не входит в обычный update lookup.

### Completed invariant

`*_completed` считается конечным состоянием для этого контура:

- обычный recouped updater его не обновляет;
- awarded promoter его не переносит;
- не следует возвращать completed-контракт назад в активный lifecycle обычным parser path.

---

## 11. Recouped sync и awarded

Основной per-record sync слой:

```text
database_work/recouped_contract_sync.py
```

Связь:

```text
AdvancedXMLParser
      ↓
RecoupedContractSync
      │
      ├─ ContractRegistryLocator
      ├─ ContractRegistryUpdater
      └─ ContractAwardedPromoter
```

### Locator

`contract_registry_locator.py`:

- нормализует contract number;
- ищет запись по lifecycle;
- имеет one-query lookup с сохранением приоритета;
- для active path может использовать сокращённый поиск.

### Updater

`contract_registry_updater.py` обновляет только whitelist полей:

```text
contractor_id
delivery_start_date
delivery_end_date
final_price
initial_price
guarantee_amount
okpd_id
auction_name
region_id
```

Нельзя превращать произвольный parser dict в SQL columns.

### Awarded: 44-ФЗ

Для 44-ФЗ переход в `*_awarded` требует:

```text
contractor_id IS NOT NULL
AND
delivery_end_date IS NOT NULL
```

Источник должен находиться в promotable lifecycle state.

### Awarded: 223-ФЗ

Для 223-ФЗ concluded-contract source является authoritative сигналом заключённого контракта; supplier/execution fields могут быть неполными.

Поэтому caller обязан сохранять семантику источника: **решение "223 → awarded" нельзя вызывать для XML, который не доказывает заключение контракта.**

В текущей архитектуре authoritative concluded-contract path связан с `contractCutted`.

### Физическое перемещение записи

Promotion сохраняет `id`:

```text
INSERT INTO <awarded>
SELECT *
FROM <source>
WHERE id = ...

DELETE FROM <source>
WHERE id = ...
```

При удалении временно используется:

```text
session_replication_role = replica
```

чтобы обойти существующие FK при переносе записи.

Любое изменение этого механизма требует отдельной проверки ссылочной целостности.

---

## 12. 223-ФЗ recouped path

223-ФЗ RD223 в текущем разделении не использует 44-ФЗ batch planner.

Путь:

```text
RD223 XML
  ↓
process_contract_files()
  ↓
AdvancedXMLParser
  ↓
RecoupedContractSync
  ↓
locator / updater / promoter
```

Из XML извлекаются и нормализуются:

- contract number;
- contractor;
- contract subject/title;
- OKPD2 codes;
- lifecycle/update поля, заданные required-tags contract;
- ссылки на документацию.

Полный список XML paths должен определяться `required_tags/`, а не дублироваться в Python-коде или README.

---

## 13. Documents / links

Документные ссылки извлекаются отдельно от canonical contract fields.

Нужно различать:

1. саму карточку контракта/закупки;
2. список документов;
3. provenance — из какого документа получено конкретное значение.

Исторически FK `links_documentation_44_fz_contract_id_fkey` ориентирован на main 44 registry. Поэтому при изменении lifecycle/link storage нельзя считать, что ссылка автоматически переживёт перенос записи в awarded.

Для новых изменений сначала проверять:

```text
main → awarded
awarded update
document link FK
contract_id preservation
```

---

## 14. Конфигурация и секреты

### `config.ini.example`

Это non-secret contract конфигурации.

Runtime copy:

```text
/opt/tendermonitor/config.ini
```

может содержать реальные пути и **не должна коммититься как production-filled copy**.

### EIS token

Token должен жить во внешнем env-файле, путь к которому задаётся через `[path] env_file`.

### Database credentials

Текущий код ожидает отдельный credentials/env contract для БД, в том числе `TENDER_MONITOR_DB_*`.

Никогда не коммитить:

```text
.env
.env.*
.env.bak*
config.ini с production values
database_work/db_credintials.env
tokens
DB passwords
runtime dumps
```

### Что можно коммитить

```text
.env.example
config.ini.example
db_credintials.env.example
source code
tests
systemd templates/factual copies без credentials
```

---

## 15. Runtime artifacts не являются source code

На production-сервере рядом с кодом могут существовать:

- `processed_dates.json`;
- `region_progress.json`;
- backward state;
- `.cache`;
- XML/ZIP data;
- logs;
- dumps;
- temporary files;
- backups.

Они **не должны автоматически попадать в Git**.

Нельзя использовать на production рабочем дереве:

```bash
git add -A
```

без предварительной проверки staged set.

---

## 16. Основные модули

| Модуль | Ответственность |
|---|---|
| `main.py` | production entrypoint, startup checks, runtime wiring |
| `orchestration/monitoring_service.py` | даты, forward/backward, region progress |
| `eis_requester.py` | SOAP-запросы ЕИС, обход регионов/subsystems |
| `proxy_runner.py` | локальный stunnel/proxy runtime |
| `eis_download_fix.py` | переписывание download URL через stunnel |
| `file_downloader.py` | download, unzip, routing в parser |
| `archive_extractor.py` | распаковка архивов |
| `parsing_xml/okpd_parser.py` | верхнеуровневая маршрутизация XML |
| `parsing_xml/xml_parser.py` | основной parser новых закупок |
| `parsing_xml/xml_parser_recouped_contract.py` | recouped/RGK parser и 223 sync |
| `parsing_xml/rgk_record.py` | parsing одного 44 RGK XML в typed record |
| `parsing_xml/rgk_batch.py` | bounded batch orchestration 44 RGK |
| `database_work/rgk_plan.py` | pure mutation planning |
| `database_work/rgk_batch_store.py` | bulk lookup + persistence |
| `database_work/rgk_batch_sql.py` | SQL builders для batch path |
| `database_work/rgk_dirty.py` | определение meaningful row changes |
| `database_work/recouped_contract_sync.py` | единая per-row sync orchestration |
| `database_work/contract_registry_locator.py` | lifecycle lookup |
| `database_work/contract_registry_updater.py` | controlled UPDATE |
| `database_work/contract_awarded_promoter.py` | перенос в awarded |
| `database_work/registry_tables.py` | authoritative table map и field whitelist |
| `database_work/database_operations.py` | canonical DB operations |
| `database_work/database_id_fetcher.py` | разрешение FK/id |
| `utils/source_day_metrics.py` | метрики source/date processing |
| `utils/memory_guard.py` | memory safety между датами |

---

## 17. Operational commands

### Forward

```bash
systemctl status tendermonitor-eis-parser.service
journalctl -u tendermonitor-eis-parser.service -n 200 --no-pager
journalctl -u tendermonitor-eis-parser.service -f
systemctl restart tendermonitor-eis-parser.service
```

### Backward

```bash
systemctl status tendermonitor-eis-parser-backward.service
journalctl -u tendermonitor-eis-parser-backward.service -n 200 --no-pager
journalctl -u tendermonitor-eis-parser-backward.service -f
systemctl restart tendermonitor-eis-parser-backward.service
```

Перед restart backward проверить его фактические environment overrides:

```bash
systemctl cat tendermonitor-eis-parser-backward.service
```

Не копировать `STOP_BEFORE` из README или старого unit snapshot: production boundary должна задаваться текущей задачей catchup.

---

## 18. Проверки после изменения parser/database path

Минимальный локальный gate:

```bash
cd /opt/tendermonitor

python -m compileall -q .
python -m pytest -q
git diff --check
```

Если изменение касается конкретного ФЗ, дополнительно нужен regression test на реальный XML fixture.

Для lifecycle/RGK изменений проверять минимум:

```text
existing main contract
existing unclear/unknown contract
existing awarded contract
completed contract
new canonical contract
missing OKPD
missing title
unchanged XML version
changed XML version
contractor absent/present
delivery dates absent/present
```

После deployment:

```text
service active/running
NRestarts не растёт
нет нового traceback
нет массовых DB rollback/deadlock
date/region progress движется
нет placeholder canonical rows
```

---

## 19. Performance invariants

Целевая производительность оценивается **по source date**, а не по скорости одного XML.

Особенно контролировать:

- SQL round-trips;
- commits;
- повторный parsing одного XML;
- unchanged updates;
- repeated lifecycle lookup;
- retries ЕИС;
- время на регион;
- время 44-ФЗ;
- время 223-ФЗ;
- время RGK;
- memory growth.

44 RGK batch path специально существует для устранения per-row DB overhead. Не заменять его обратно на вызов `RecoupedContractSync` для каждого XML без отдельного benchmark.

---

## 20. Правила изменения этого контура

Перед production patch:

1. определить source/subsystem, который меняется;
2. определить canonical fields, которые он имеет право менять;
3. проверить lifecycle semantics;
4. не расширять SQL field list через произвольный parser payload;
5. не создавать placeholder contracts;
6. не модифицировать `completed`;
7. сохранить backward/forward state isolation;
8. добавить regression test;
9. проверить import/syntax;
10. проверить secrets/staged diff;
11. после restart проверить фактический runtime.

### Главное правило

**Парсер должен либо сохранить доказанные source facts, либо оставить запись unresolved. Он не должен додумывать недостающие факты ради заполнения карточки.**
