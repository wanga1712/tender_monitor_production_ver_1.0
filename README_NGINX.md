# TenderMonitor - Настройка nginx на Linux

## Важно!

На Linux используется **nginx** вместо stunnel для проксирования запросов к ЕИС.

## Быстрая настройка nginx

### 1. Установка nginx

```bash
# RedOS, CentOS, RHEL
sudo yum install -y nginx

# Debian, Ubuntu
sudo apt-get update
sudo apt-get install -y nginx
```

### 2. Настройка конфигурации

Скопируйте файл `nginx_eis.conf` в директорию конфигурации nginx:

```bash
sudo cp nginx_eis.conf /etc/nginx/conf.d/eis.conf
```

Или используйте автоматический скрипт:

```bash
sudo chmod +x setup_nginx.sh
sudo ./setup_nginx.sh
```

### 3. Проверка и запуск

```bash
# Проверка конфигурации
sudo nginx -t

# Запуск nginx
sudo systemctl enable nginx
sudo systemctl start nginx

# Проверка статуса
sudo systemctl status nginx

# Проверка порта 8080
curl http://localhost:8080
```

## Конфигурация nginx

Файл `nginx_eis.conf` настраивает nginx как reverse proxy:
- Слушает на `localhost:8080`
- Проксирует запросы на `int44.zakupki.gov.ru:443`
- Поддерживает SOAP запросы к `/eis-integration/`

## Отличия от Windows

| Компонент | Windows | Linux |
|-----------|---------|-------|
| Прокси | stunnel_msspi.exe | nginx |
| Конфигурация | stunnel.conf | nginx_eis.conf |
| Управление | Запуск процесса | systemctl |
| Логи | stunnel.log | /var/log/nginx/eis_*.log |

## Устранение проблем

### Порт 8080 недоступен

```bash
# Проверьте, что nginx запущен
sudo systemctl status nginx

# Проверьте конфигурацию
sudo nginx -t

# Проверьте логи
sudo tail -f /var/log/nginx/eis_error.log
```

### Nginx не запускается

```bash
# Проверьте синтаксис конфигурации
sudo nginx -t

# Проверьте, не занят ли порт 8080
sudo netstat -tuln | grep 8080

# Проверьте права доступа
sudo ls -la /etc/nginx/conf.d/eis.conf
```

## Логи

- Ошибки: `/var/log/nginx/eis_error.log`
- Доступ: `/var/log/nginx/eis_access.log`

```bash
# Просмотр логов в реальном времени
sudo tail -f /var/log/nginx/eis_error.log
sudo tail -f /var/log/nginx/eis_access.log
```

