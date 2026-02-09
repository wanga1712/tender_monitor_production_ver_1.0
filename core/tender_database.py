"""
MODULE: core.tender_database
RESPONSIBILITY: Specialized database manager for TenderMonitor DB.
ALLOWED: Connection management, advisory locks, monitoring queries.
FORBIDDEN: General business logic unrelated to DB operations.
ERRORS: DatabaseConnectionError, DatabaseQueryError.

Менеджер базы данных tender_monitor

Отдельный модуль для работы с базой данных торгов.
Использует Singleton паттерн для единого подключения.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, List, Dict, Any, Tuple
from loguru import logger
from config.settings import DatabaseConfig
from core.exceptions import DatabaseConnectionError, DatabaseQueryError


class TenderDatabaseManager:
    """
    Менеджер базы данных tender_monitor (Singleton)
    
    Управляет подключением к базе данных торгов и выполнением запросов.
    Поддерживает оффлайн-режим при недоступности БД.
    """
    
    _instance: Optional['TenderDatabaseManager'] = None
    _connection: Optional[psycopg2.extensions.connection] = None
    _offline_mode: bool = False
    _connection_error: Optional[str] = None
    
    def __new__(cls, config: Optional[DatabaseConfig] = None):
        """
        Реализация Singleton паттерна
        
        Args:
            config: Конфигурация базы данных (используется только при первом создании)
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._config = config
            cls._instance._connection = None
            cls._instance._offline_mode = False
            cls._instance._connection_error = None
        return cls._instance
    
    def connect(self, fallback_to_offline: bool = True) -> None:
        """
        Установка подключения к базе данных
        
        Args:
            fallback_to_offline: Автоматически переходить в оффлайн-режим при ошибке
        
        Raises:
            DatabaseConnectionError: Если не удалось подключиться и отключен fallback
        """
        if self._connection and not self._connection.closed:
            logger.debug("Подключение к БД tender_monitor уже установлено")
            return
        
        if not self._config:
            if fallback_to_offline:
                self._offline_mode = True
                self._connection_error = "Конфигурация БД tender_monitor не задана"
                logger.warning("Конфигурация БД не задана - переход в оффлайн-режим")
                return
            raise DatabaseConnectionError("Конфигурация БД tender_monitor не задана")
        
        try:
            self._connection = psycopg2.connect(
                host=self._config.host,
                database=self._config.database,
                user=self._config.user,
                password=self._config.password,
                port=self._config.port,
                cursor_factory=RealDictCursor
            )
            self._connection.autocommit = False
            self._offline_mode = False
            self._connection_error = None
            logger.info(f"Успешное подключение к БД tender_monitor: {self._config.database}")
        except psycopg2.OperationalError as e:
            error_msg = f"Ошибка подключения к БД tender_monitor {self._config.database}: {e}"
            logger.error(error_msg)
            self._connection_error = error_msg
            
            if fallback_to_offline:
                self._offline_mode = True
                logger.warning("Переход в оффлайн-режим работы без БД")
            else:
                raise DatabaseConnectionError(error_msg) from e
        except Exception as e:
            error_msg = f"Неожиданная ошибка при подключении к БД tender_monitor: {e}"
            logger.error(error_msg)
            self._connection_error = error_msg
            
            if fallback_to_offline:
                self._offline_mode = True
                logger.warning("Переход в оффлайн-режим работы без БД")
            else:
                raise DatabaseConnectionError(error_msg) from e
    
    def disconnect(self) -> None:
        """Закрытие подключения к базе данных с защитой от Access Violation"""
        try:
            if self._connection and not self._connection.closed:
                try:
                    self._connection.close()
                    logger.info("Подключение к БД tender_monitor закрыто")
                except Exception as close_error:
                    logger.warning(f"Ошибка при закрытии подключения к БД tender_monitor: {close_error}")
        except Exception as e:
            logger.warning(f"Ошибка при проверке состояния подключения tender_monitor: {e}")
        finally:
            self._connection = None
    
    def is_offline_mode(self) -> bool:
        """Проверка, работает ли менеджер в оффлайн-режиме"""
        return self._offline_mode
    
    def get_connection_error(self) -> Optional[str]:
        """Получение последней ошибки подключения"""
        return self._connection_error
    
    def set_offline_mode(self, offline: bool = True) -> None:
        """Установка оффлайн-режима"""
        self._offline_mode = offline
        if offline:
            logger.warning("Включен оффлайн-режим работы без БД")
        else:
            logger.info("Оффлайн-режим отключен")
    
    def execute_query(
        self,
        query: str,
        params: Optional[Tuple] = None,
        cursor_factory=None
    ) -> List[Dict[str, Any]]:
        # #region agent log
        import json
        import os
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_path = os.path.join(project_root, ".cursor", "debug.log")
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "transaction-debug",
                    "hypothesisId": "TRANSACTION_A",
                    "location": "tender_database.py:execute_query:entry",
                    "message": "Начинаем execute_query",
                    "data": {
                        "query_start": query[:100] + "..." if len(query) > 100 else query,
                        "has_params": params is not None,
                        "params_count": len(params) if params else 0,
                        "autocommit": getattr(self._connection, 'autocommit', 'unknown') if self._connection else None,
                        "offline_mode": self._offline_mode
                    },
                    "timestamp": int(__import__('time').time() * 1000)
                }) + "\n")
        except Exception:
            pass  # Игнорируем ошибки логирования
        # #endregion
        """
        Выполнение SELECT запроса
        
        Args:
            query: SQL запрос
            params: Параметры запроса
            cursor_factory: Фабрика курсора (например, RealDictCursor)
        
        Returns:
            Список результатов запроса в виде словарей
        
        Raises:
            DatabaseQueryError: Если произошла ошибка при выполнении запроса
        """
        if self._offline_mode:
            logger.warning(f"Оффлайн-режим: запрос '{query[:50]}...' пропущен")
            return []
            
        if not self._connection or self._connection.closed:
            raise DatabaseConnectionError("Нет подключения к БД tender_monitor")
        
        try:
            # По умолчанию используем RealDictCursor для совместимости с DatabaseManager
            if cursor_factory is None:
                cursor_factory = RealDictCursor
            
            with self._connection.cursor(cursor_factory=cursor_factory) as cursor:
                cursor.execute(query, params)
                
                # Проверяем, есть ли результаты для выборки (через description)
                # Если description не None, значит есть результаты
                has_results = cursor.description is not None
                
                if has_results:
                    result = cursor.fetchall()
                    # Проверяем, есть ли RETURNING в запросе (для INSERT/UPDATE/DELETE с возвратом данных)
                    query_upper = query.strip().upper()
                    has_returning = 'RETURNING' in query_upper
                    if has_returning:
                        self._connection.commit()
                        # #region agent log
                        try:
                            with open(log_path, "a", encoding="utf-8") as f:
                                f.write(json.dumps({
                                    "sessionId": "debug-session",
                                    "runId": "transaction-debug",
                                    "hypothesisId": "TRANSACTION_A",
                                    "location": "tender_database.py:execute_query:commit_select",
                                    "message": "COMMIT после SELECT с RETURNING",
                                    "data": {"result_count": len(result), "query_type": "SELECT_RETURNING"},
                                    "timestamp": int(__import__('time').time() * 1000)
                                }) + "\n")
                        except Exception:
                            pass
                        # #endregion
                        logger.debug(f"Выполнен запрос с RETURNING к tender_monitor, возвращено {len(result)} строк")
                    else:
                        logger.debug(f"Выполнен SELECT запрос к tender_monitor, возвращено {len(result)} строк")
                    return result
                else:
                    # Нет результатов (DDL команды, обычные INSERT/UPDATE/DELETE без RETURNING)
                    self._connection.commit()
                    # #region agent log
                    try:
                        with open(log_path, "a", encoding="utf-8") as f:
                            f.write(json.dumps({
                                "sessionId": "debug-session",
                                "runId": "transaction-debug",
                                "hypothesisId": "TRANSACTION_A",
                                "location": "tender_database.py:execute_query:commit_other",
                                "message": "COMMIT после OTHER",
                                "data": {"query_type": "OTHER", "query_start": query[:50]},
                                "timestamp": int(__import__('time').time() * 1000)
                            }) + "\n")
                    except Exception:
                        pass
                    # #endregion
                    logger.debug(f"Выполнен запрос к tender_monitor (без результатов): {query[:50]}...")
                    return []
        except psycopg2.Error as e:
            self._connection.rollback()
            # #region agent log
            try:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "transaction-debug",
                        "hypothesisId": "TRANSACTION_B",
                        "location": "tender_database.py:execute_query:rollback_error",
                        "message": "ROLLBACK из-за ошибки",
                        "data": {"error_type": type(e).__name__, "error_msg": str(e)[:200]},
                        "timestamp": int(__import__('time').time() * 1000)
                    }) + "\n")
            except Exception:
                pass
            # #endregion
            error_msg = f"Ошибка выполнения запроса к БД tender_monitor: {e}"
            logger.error(error_msg)
            raise DatabaseQueryError(error_msg) from e
    
    def execute_update(
        self,
        query: str,
        params: Optional[Tuple] = None
    ) -> int:
        # #region agent log
        import json
        import os
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_path = os.path.join(project_root, ".cursor", "debug.log")
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "transaction-debug",
                    "hypothesisId": "TRANSACTION_C",
                    "location": "tender_database.py:execute_update:entry",
                    "message": "Начинаем execute_update",
                    "data": {
                        "query_start": query[:100] + "..." if len(query) > 100 else query,
                        "has_params": params is not None,
                        "params_count": len(params) if params else 0
                    },
                    "timestamp": int(__import__('time').time() * 1000)
                }) + "\n")
        except Exception:
            pass
        # #endregion
        """
        Выполнение INSERT/UPDATE/DELETE запроса
        
        Args:
            query: SQL запрос
            params: Параметры запроса
        
        Returns:
            Количество затронутых строк
        
        Raises:
            DatabaseQueryError: Если произошла ошибка при выполнении запроса
        """
        if not self._connection or self._connection.closed:
            raise DatabaseConnectionError("Нет подключения к БД tender_monitor")
        
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(query, params)
                affected_rows = cursor.rowcount
                self._connection.commit()
                # #region agent log
                try:
                    with open(log_path, "a", encoding="utf-8") as f:
                        f.write(json.dumps({
                            "sessionId": "debug-session",
                            "runId": "transaction-debug",
                            "hypothesisId": "TRANSACTION_C",
                            "location": "tender_database.py:execute_update:commit",
                            "message": "COMMIT после UPDATE",
                            "data": {"affected_rows": affected_rows, "query_start": query[:50]},
                            "timestamp": int(__import__('time').time() * 1000)
                        }) + "\n")
                except Exception:
                    pass
                # #endregion
                logger.debug(f"Выполнен UPDATE запрос, затронуто строк: {affected_rows}")
                return affected_rows
        except psycopg2.Error as e:
            self._connection.rollback()
            # #region agent log
            try:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "transaction-debug",
                        "hypothesisId": "TRANSACTION_D",
                        "location": "tender_database.py:execute_update:rollback_error",
                        "message": "ROLLBACK UPDATE из-за ошибки",
                        "data": {"error_type": type(e).__name__, "error_msg": str(e)[:200]},
                        "timestamp": int(__import__('time').time() * 1000)
                    }) + "\n")
            except Exception:
                pass
            # #endregion
            error_msg = f"Ошибка выполнения UPDATE запроса к БД tender_monitor: {e}"
            logger.error(error_msg)
            raise DatabaseQueryError(error_msg) from e
    
    def is_connected(self) -> bool:
        """Проверка наличия активного подключения"""
        return self._connection is not None and not self._connection.closed
    
    def acquire_advisory_lock(self, lock_id: int) -> bool:
        """
        Получение advisory lock (блокировка на уровне БД)
        
        Args:
            lock_id: Уникальный идентификатор блокировки
            
        Returns:
            True если блокировка получена, False если уже заблокировано другим процессом
        """
        return self._execute_advisory_lock_query(
            "SELECT pg_try_advisory_lock(%s)",
            lock_id,
            "pg_try_advisory_lock",
            "получении advisory lock"
        )
    
    def release_advisory_lock(self, lock_id: int) -> bool:
        """
        Освобождение advisory lock
        
        Args:
            lock_id: Уникальный идентификатор блокировки
            
        Returns:
            True если блокировка освобождена, False если блокировка не была установлена
        """
        return self._execute_advisory_lock_query(
            "SELECT pg_advisory_unlock(%s)",
            lock_id,
            "pg_advisory_unlock",
            "освобождении advisory lock"
        )
    
    def _execute_advisory_lock_query(
        self,
        query: str,
        lock_id: int,
        result_key: str,
        error_context: str
    ) -> bool:
        """Общий метод для выполнения запросов advisory lock"""
        if not self._connection or self._connection.closed:
            raise DatabaseConnectionError("Нет подключения к БД tender_monitor")
        
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(query, (lock_id,))
                result = cursor.fetchone()
                lock_value = self._extract_lock_result(result, result_key)
                self._connection.commit()
                return lock_value
        except psycopg2.Error as e:
            self._connection.rollback()
            error_msg = f"Ошибка при {error_context}: {e}"
            logger.error(error_msg, exc_info=True)
            raise DatabaseQueryError(error_msg) from e
        except Exception as e:
            self._connection.rollback()
            error_msg = f"Неожиданная ошибка при {error_context}: {type(e).__name__}: {e}"
            logger.error(error_msg, exc_info=True)
            raise DatabaseQueryError(error_msg) from e
    
    @staticmethod
    def _extract_lock_result(result, result_key: str) -> bool:
        """Извлечение и преобразование результата advisory lock в boolean"""
        if not result:
            return False
        
        if hasattr(result, 'get'):
            value = result.get(result_key)
        else:
            value = result[result_key]
        
        return bool(value) if value is not None else False
    
    @classmethod
    def get_instance(cls) -> Optional['TenderDatabaseManager']:
        """Получение экземпляра Singleton"""
        return cls._instance

