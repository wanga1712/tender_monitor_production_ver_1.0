import subprocess
from loguru import logger
import os

from secondary_functions import load_config


class StunnelRunner:
    def __init__(self, config_path="config.ini"):
        """
        Инициализирует объект StunnelRunner, загружает настройки из конфигурации и проверяет существование необходимых файлов.

        :param config_path: Путь к файлу конфигурации (по умолчанию "config.ini").
        :raises ValueError: Если не удается загрузить конфигурацию.
        :raises FileNotFoundError: Если не найден файл stunnel_msspi.exe по указанному пути.
        """
        
        # Загружаем настройки из конфигурации
        self.config = load_config(config_path)
        if not self.config:
            raise ValueError("Ошибка загрузки конфигурации!")  # Логируем ошибку, если конфигурация не загружена

        # Получаем настройки из конфигурационного файла
        self.stunnel_dir = self.config.get('stunnel', 'stunnel_dir', fallback=".")  # Путь к директории с stunnel
        self.config_file = self.config.get('stunnel', 'config_file', fallback="stunnel.conf")  # Путь к файлу конфигурации

        # Формируем путь к исполняемому файлу stunnel_msspi.exe
        self.stunnel_exe = os.path.join(self.stunnel_dir, "stunnel_msspi.exe")
        if not os.path.exists(self.stunnel_exe):
            raise FileNotFoundError(f"Файл {self.stunnel_exe} не найден! Проверьте путь в конфигурации.")  # Ошибка, если файл не найден

    def run_stunnel(self):
        """
        Запускает stunnel с конфигурационным файлом и перенаправляет логи в файл.

        :return: Процесс, запущенный с помощью subprocess.Popen, или None в случае ошибки.
        :raises Exception: При возникновении ошибки во время запуска stunnel.
        """
        # Формируем команду для запуска stunnel с указанием исполняемого файла и конфигурации
        command = [self.stunnel_exe, self.config_file]

        try:
            # Логируем команду перед запуском
            logger.info(f"Запускаю stunnel: {' '.join(command)} в {self.stunnel_dir}")

            # Открываем файл для записи логов stunnel
            with open(os.path.join(self.stunnel_dir, "stunnel.log"), "w") as log_file:
                # Запускаем процесс stunnel, перенаправляем stdout и stderr в log_file
                proc = subprocess.Popen(command, cwd=self.stunnel_dir, stdout=log_file, stderr=subprocess.STDOUT)

            # Логируем успешный запуск
            logger.info("stunnel успешно запущен (процесс выполняется в фоне).")

            # Возвращаем объект процесса, если нужно контролировать выполнение
            return proc

        except Exception as e:
            # Логируем ошибку, если не удалось запустить stunnel
            logger.error(f"Ошибка при запуске stunnel: {e}")
            return None  # Возвращаем None в случае ошибки
