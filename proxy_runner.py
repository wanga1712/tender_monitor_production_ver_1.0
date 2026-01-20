import subprocess
import os
import sys
import time
import socket
import platform

from utils.logger_config import get_logger
from secondary_functions import load_config

# Получаем logger (только ошибки в файл)
logger = get_logger()


class ProxyRunner:
    def __init__(self, config_path="config.ini"):
        """
        Инициализирует объект ProxyRunner, загружает настройки из конфигурации и проверяет существование необходимых файлов.
        Поддерживает Windows (stunnel) и Linux (nginx) платформы.
        На Linux stunnel не используется, только nginx.

        :param config_path: Путь к файлу конфигурации (по умолчанию "config.ini").
        :raises ValueError: Если не удается загрузить конфигурацию.
        :raises FileNotFoundError: Если не найден исполняемый файл stunnel (Windows) или nginx не запущен (Linux).
        """
        
        # Определяем платформу
        self.platform = platform.system().lower()
        is_windows = self.platform == 'windows'
        is_linux = self.platform == 'linux'
        
        # Загружаем настройки из конфигурации
        self.config = load_config(config_path)
        if not self.config:
            raise ValueError("Ошибка загрузки конфигурации!")

        # Получаем настройки из конфигурационного файла
        # На Windows используется для stunnel, на Linux - не используется (там nginx)
        self.stunnel_dir = self.config.get('stunnel', 'stunnel_dir', fallback=".")
        self.config_file = self.config.get('stunnel', 'config_file', fallback="stunnel.conf")

        # Формируем путь к исполняемому файлу в зависимости от платформы
        if is_windows:
            # Windows: используем stunnel_msspi.exe
            self.stunnel_exe = os.path.join(self.stunnel_dir, "stunnel_msspi.exe")
            if not os.path.exists(self.stunnel_exe):
                raise FileNotFoundError(f"Файл {self.stunnel_exe} не найден! Проверьте путь в конфигурации.")
        elif is_linux:
            # Linux: используем nginx (stunnel не используется)
            # Проверяем, что nginx запущен
            self.stunnel_exe = None  # На Linux stunnel не используется
            try:
                result = subprocess.run(
                    ['systemctl', 'is-active', 'nginx'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode != 0:
                    logger.warning("Nginx не запущен. Убедитесь, что nginx установлен и настроен как reverse proxy.")
            except (subprocess.TimeoutExpired, FileNotFoundError):
                # systemctl может быть недоступен, проверяем через ps
                try:
                    result = subprocess.run(
                        ['pgrep', '-f', 'nginx'],
                        capture_output=True,
                        timeout=5
                    )
                    if result.returncode != 0:
                        logger.warning("Nginx процесс не найден. Убедитесь, что nginx запущен.")
                except:
                    pass
        else:
            raise RuntimeError(f"Неподдерживаемая платформа: {self.platform}")

    def check_port_available(self, host="localhost", port=8080, timeout=30):
        """
        Проверяет, доступен ли порт для подключения.
        
        :param host: Хост для проверки (по умолчанию localhost)
        :param port: Порт для проверки (по умолчанию 8080)
        :param timeout: Максимальное время ожидания в секундах (по умолчанию 30)
        :return: True если порт доступен, False в противном случае
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                result = sock.connect_ex((host, port))
                sock.close()
                if result == 0:
                    return True
            except Exception:
                pass
            time.sleep(1)
        return False

    def _check_windows_stunnel_logs(self, log_file_path, timeout=5):
        """
        Проверяет логи Stunnel на наличие ошибок с сертификатом.
        Приватный метод, используется ТОЛЬКО на Windows для проверки stunnel.
        На Linux не используется (там nginx).
        
        :param log_file_path: Путь к файлу логов Stunnel
        :param timeout: Время ожидания появления логов в секундах
        :return: Список найденных ошибок или пустой список
        """
        errors = []
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                if os.path.exists(log_file_path):
                    with open(log_file_path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                        # Проверяем на ошибки с сертификатом
                        if "msspi_set_mycert_options failed" in content:
                            # Извлекаем информацию о сертификате из лога
                            import re
                            cert_match = re.search(r'cert = "([^"]+)"', content)
                            pin_match = re.search(r'pin = "([^"]+)"', content)
                            cert_path = cert_match.group(1) if cert_match else "не указан"
                            pin = pin_match.group(1) if pin_match else "не указан"
                            errors.append(f"Ошибка загрузки сертификата: {cert_path}, PIN: {pin}")
                        if "LOG3" in content and "failed" in content.lower():
                            # Ищем другие критические ошибки
                            lines = content.split('\n')
                            for line in lines:
                                if "LOG3" in line and "failed" in line.lower():
                                    errors.append(line.strip())
            except Exception:
                pass
            time.sleep(0.5)
        
        return errors

    def run_proxy(self):
        """
        Запускает прокси-соединение к ЕИС.
        На Windows: запускает stunnel процесс.
        На Linux: проверяет, что nginx работает как reverse proxy (stunnel не используется).

        :return: Процесс stunnel (Windows) или None (Linux).
        :raises Exception: При возникновении ошибки.
        """

        try:
            if self.platform == 'windows':
                # Windows: запускаем stunnel
                # Формируем команду для запуска stunnel
                command = [self.stunnel_exe, self.config_file]
                log_file_path = os.path.join(self.stunnel_dir, "stunnel.log")
                
                # Открываем файл для записи логов stunnel
                with open(log_file_path, "w") as log_file:
                    # Запускаем процесс stunnel
                    proc = subprocess.Popen(
                        command,
                        cwd=self.stunnel_dir,
                        stdout=log_file,
                        stderr=subprocess.STDOUT
                    )

                # Даем Stunnel время на инициализацию
                time.sleep(3)
                
                # Проверяем логи на наличие ошибок с сертификатом (только Windows/stunnel)
                cert_errors = self._check_windows_stunnel_logs(log_file_path, timeout=5)
                if cert_errors:
                    error_details = "\n".join(cert_errors)
                    error_msg = f"Ошибка конфигурации Stunnel (проблема с сертификатом):\n{error_details}\n\nПроверьте:\n- Существует ли файл сертификата\n- Правильность пути к сертификату в stunnel.conf\n- Правильность PIN-кода сертификата\n- Установлен ли сертификат в системе"
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)
                
                # Проверяем, что порт 8080 доступен
                if not self.check_port_available("localhost", 8080, timeout=30):
                    error_msg = "Stunnel запущен, но порт 8080 недоступен. Проверьте конфигурацию Stunnel и логи в stunnel.log"
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)

                return proc
                
            elif self.platform == 'linux':
                # Linux: проверяем nginx (stunnel не используется)
                logger.info("Linux платформа: проверяем nginx")
                
                # Проверяем, что nginx запущен
                try:
                    result = subprocess.run(
                        ['systemctl', 'is-active', 'nginx'],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if result.returncode != 0:
                        error_msg = "Nginx не запущен. Запустите: systemctl start nginx"
                        logger.error(error_msg)
                        raise RuntimeError(error_msg)
                    logger.info("Nginx активен")
                except (subprocess.TimeoutExpired, FileNotFoundError):
                    # Проверяем через ps как fallback
                    try:
                        result = subprocess.run(
                            ['pgrep', '-f', 'nginx'],
                            capture_output=True,
                            timeout=5
                        )
                        if result.returncode != 0:
                            error_msg = "Nginx процесс не найден. Убедитесь, что nginx установлен и запущен."
                            logger.error(error_msg)
                            raise RuntimeError(error_msg)
                        logger.info("Nginx процесс найден")
                    except Exception as e:
                        logger.warning(f"Не удалось проверить статус nginx: {e}")
                
                # Проверяем, что порт 8080 доступен (nginx должен проксировать на int44.zakupki.gov.ru:443)
                if not self.check_port_available("localhost", 8080, timeout=10):
                    error_msg = "Порт 8080 недоступен. Проверьте конфигурацию nginx и убедитесь, что он слушает на localhost:8080"
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)
                
                logger.info("Порт 8080 доступен, nginx работает корректно")
                return None  # На Linux не возвращаем процесс, т.к. nginx управляется systemd
            else:
                raise RuntimeError(f"Неподдерживаемая платформа: {self.platform}")

        except Exception as e:
            # Логируем ошибку
            logger.error(f"Ошибка при проверке/запуске прокси: {e}", exc_info=True)
            raise

