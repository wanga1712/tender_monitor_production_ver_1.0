# Настройка модуля уведомлений

## 📦 Установка

Модуль уведомлений уже включен в проект. Дополнительные зависимости:
- `requests` (уже установлен)
- `smtplib` (встроен в Python)

## ⚙️ Настройка

### 1. Добавьте секцию в config.ini:

```ini
[notifications]
enabled = true
channels = email,telegram,file

[notifications.email]
enabled = true
smtp_host = smtp.gmail.com
smtp_port = 587
smtp_user = your_email@gmail.com
smtp_password = your_app_password
from_email = your_email@gmail.com
to_emails = admin@example.com,manager@example.com

[notifications.telegram]
enabled = true
bot_token = 123456789:ABCdefGHIjklMNOpqrsTUVwxyz
chat_id = -1001234567890

[notifications.webhook]
enabled = false
url = https://hooks.slack.com/services/YOUR/WEBHOOK/URL

[notifications.file]
log_dir = notifications

[notifications.reports]
daily_report_time = 23:00
daily_report_enabled = true
critical_errors_enabled = true
warnings_enabled = true
```

### 2. Настройка Telegram бота:

1. Создайте бота через [@BotFather](https://t.me/BotFather)
2. Получите токен бота
3. Узнайте chat_id:
   - Добавьте бота в группу или напишите ему
   - Отправьте сообщение боту
   - Перейдите по ссылке: `https://api.telegram.org/bot<TOKEN>/getUpdates`
   - Найдите `chat.id` в ответе

### 3. Настройка Email (Gmail):

1. Включите двухфакторную аутентификацию
2. Создайте пароль приложения:
   - Google Account → Security → 2-Step Verification → App passwords
3. Используйте пароль приложения в `smtp_password`

## 🔧 Интеграция в код

### В main.py:

```python
from utils.notifications import NotificationManager

# Инициализация (в начале main)
notifier = NotificationManager()

# При критической ошибке
try:
    # код
except CriticalError as e:
    notifier.send_critical(
        title="Критическая ошибка БД",
        message="Программа остановлена",
        error_details=str(e)
    )
    sys.exit(1)

# Ежедневный отчет
stats = {
    "date": date_str,
    "dates_processed": processed_count,
    "customers_added": customers_added,
    "contractors_added": contractors_added,
    "contracts_added": contracts_total,
    "errors_count": error_count,
    "uptime": "12:34:56"
}
notifier.send_daily_report(stats)
```

## 📊 Примеры использования

### Критическая ошибка:
```python
notifier.send_critical(
    title="Остановка программы",
    message="Критическая ошибка подключения к БД",
    error_details="Connection timeout after 30 seconds"
)
```

### Предупреждение:
```python
notifier.send_warning(
    title="Долгое отсутствие данных",
    message="Данные за 2026-01-18 не обнаружены более 24 часов"
)
```

### Информационное:
```python
notifier.send_info(
    title="Переход в режим мониторинга",
    message="Программа перешла в режим непрерывного мониторинга"
)
```

## 🎯 Рекомендации

### Для продакшена:
- ✅ **Telegram** - для критических ошибок (мгновенно)
- ✅ **Email** - для ежедневных отчетов
- ✅ **File** - для истории всех событий

### Для разработки:
- ✅ **Telegram** - быстрые уведомления
- ✅ **File** - для отладки

## 📝 Файлы уведомлений

Уведомления сохраняются в:
- `notifications/notifications_YYYY-MM-DD.log` - текстовые логи
- `notifications/notifications_YYYY-MM-DD.json` - JSON формат

## 🔍 Мониторинг

Проверка отправки уведомлений:
```bash
# Логи приложения
tail -f errors.log | grep notification

# Файлы уведомлений
ls -la notifications/
cat notifications/notifications_2026-01-19.log
```
