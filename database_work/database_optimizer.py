"""
Скрипт для анализа и оптимизации структуры базы данных.
Проверяет индексы, внешние ключи и предлагает улучшения для производительности.
"""
import os
from database_work.database_connection import DatabaseManager
from utils.logger_config import get_logger

logger = get_logger()


class DatabaseOptimizer:
    """Класс для анализа и оптимизации структуры базы данных."""
    
    def __init__(self):
        """Инициализация подключения к БД."""
        self.db_manager = DatabaseManager()
        self.cursor = self.db_manager.connection.cursor()
    
    def get_all_tables(self):
        """Получает список всех таблиц в БД."""
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """
        self.cursor.execute(query)
        return [row[0] for row in self.cursor.fetchall()]
    
    def get_table_columns(self, table_name):
        """Получает информацию о колонках таблицы."""
        query = """
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = %s
            ORDER BY ordinal_position;
        """
        self.cursor.execute(query, (table_name,))
        return self.cursor.fetchall()
    
    def get_indexes(self, table_name):
        """Получает список индексов для таблицы."""
        query = """
            SELECT 
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname = 'public' 
            AND tablename = %s;
        """
        self.cursor.execute(query, (table_name,))
        return self.cursor.fetchall()
    
    def get_indexed_columns_direct(self, table_name):
        """Получает список колонок с индексами напрямую из pg_index."""
        query = """
            SELECT 
                a.attname AS column_name
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
            AND c.relname = %s
            AND i.indisprimary = false;
        """
        self.cursor.execute(query, (table_name,))
        return {row[0] for row in self.cursor.fetchall()}
    
    def get_foreign_keys(self, table_name):
        """Получает список внешних ключей для таблицы."""
        query = """
            SELECT
                tc.constraint_name,
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_name = %s;
        """
        self.cursor.execute(query, (table_name,))
        return self.cursor.fetchall()
    
    def get_primary_keys(self, table_name):
        """Получает список первичных ключей для таблицы."""
        query = """
            SELECT
                kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_name = %s;
        """
        self.cursor.execute(query, (table_name,))
        return [row[0] for row in self.cursor.fetchall()]
    
    def analyze_table(self, table_name):
        """Анализирует таблицу и возвращает рекомендации."""
        recommendations = []
        
        # Получаем информацию о таблице
        columns = self.get_table_columns(table_name)
        indexes = self.get_indexes(table_name)
        foreign_keys = self.get_foreign_keys(table_name)
        primary_keys = self.get_primary_keys(table_name)
        
        # Создаем словарь колонок для быстрого поиска
        column_dict = {col[0]: col for col in columns}
        
        # Получаем колонки с индексами напрямую из системных таблиц
        indexed_columns = self.get_indexed_columns_direct(table_name)
        
        # Дополнительно парсим определения индексов для надежности
        for idx in indexes:
            idx_def = idx[1]
            if 'ON ' in idx_def:
                try:
                    after_on = idx_def.split('ON ')[1]
                    if '(' in after_on:
                        cols_part = after_on.split('(')[1].split(')')[0]
                        cols = [col.strip().strip('"').strip("'") for col in cols_part.split(',')]
                        for col in cols:
                            col_name = col.split()[0] if col.split() else col
                            indexed_columns.add(col_name)
                except Exception:
                    pass
        
        # Проверяем, какие колонки используются в WHERE запросах и не имеют индексов
        # Основные колонки для поиска (на основе кода)
        search_columns = {
            'customer': ['customer_inn'],
            'contractor': ['inn'],
            'reestr_contract_44_fz': ['contract_number'],
            'reestr_contract_223_fz': ['contract_number'],
            'file_names_xml': ['file_name'],
            'collection_codes_okpd': ['code', 'sub_code'],
            'region': ['code'],
            'trading_platform': ['trading_platform_name'],
            'links_documentation_44_fz': ['link'],
            'links_documentation_223_fz': ['link'],
        }
        
        if table_name in search_columns:
            for col in search_columns[table_name]:
                if col in column_dict and col not in indexed_columns:
                    recommendations.append({
                        'type': 'missing_index',
                        'table': table_name,
                        'column': col,
                        'priority': 'high',
                        'sql': f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{col} ON {table_name} ({col});"
                    })
        
        # Проверяем внешние ключи
        # Колонки, которые должны быть внешними ключами
        expected_fks = {
            'customer': {
                'region_id': 'region(id)',
            },
            'contractor': {},
            'reestr_contract_44_fz': {
                'customer_id': 'customer(id)',
                'contractor_id': 'contractor(id)',
                'region_id': 'region(id)',
                'okpd_id': 'collection_codes_okpd(id)',
                'trading_platform_id': 'trading_platform(id)',
            },
            'reestr_contract_223_fz': {
                'customer_id': 'customer(id)',
                'contractor_id': 'contractor(id)',
                'region_id': 'region(id)',
                'okpd_id': 'collection_codes_okpd(id)',
                'trading_platform_id': 'trading_platform(id)',
            },
            'links_documentation_44_fz': {
                'contract_id': 'reestr_contract_44_fz(id)',
            },
            'links_documentation_223_fz': {
                'contract_id': 'reestr_contract_223_fz(id)',
            },
        }
        
        if table_name in expected_fks:
            existing_fk_columns = {fk[2]: fk[3] for fk in foreign_keys}
            for col, ref in expected_fks[table_name].items():
                if col in column_dict and col not in existing_fk_columns:
                    ref_table, ref_col = ref.split('(')
                    ref_col = ref_col.rstrip(')')
                    recommendations.append({
                        'type': 'missing_foreign_key',
                        'table': table_name,
                        'column': col,
                        'reference': ref,
                        'priority': 'medium',
                        'sql': f"ALTER TABLE {table_name} ADD CONSTRAINT fk_{table_name}_{col} FOREIGN KEY ({col}) REFERENCES {ref_table}({ref_col}) ON DELETE CASCADE;"
                    })
        
        # Проверяем уникальные ограничения для важных полей
        unique_columns = {
            'customer': ['customer_inn'],
            'contractor': ['inn'],
            'reestr_contract_44_fz': ['contract_number'],
            'reestr_contract_223_fz': ['contract_number'],
            'file_names_xml': ['file_name'],
            'region': ['code'],
        }
        
        if table_name in unique_columns:
            # Проверяем существующие уникальные ограничения
            query = """
                SELECT
                    kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.constraint_type = 'UNIQUE'
                AND tc.table_name = %s;
            """
            self.cursor.execute(query, (table_name,))
            existing_unique = {row[0] for row in self.cursor.fetchall()}
            
            for col in unique_columns[table_name]:
                if col in column_dict and col not in existing_unique:
                    recommendations.append({
                        'type': 'missing_unique',
                        'table': table_name,
                        'column': col,
                        'priority': 'high',
                        'sql': f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_{col}_unique ON {table_name} ({col});"
                    })
        
        return recommendations
    
    def generate_optimization_script(self):
        """Генерирует SQL скрипт для оптимизации БД."""
        all_recommendations = []
        
        print("🔍 Анализ структуры базы данных...")
        tables = self.get_all_tables()
        print(f"📊 Найдено таблиц: {len(tables)}\n")
        
        for table in tables:
            print(f"  Анализ таблицы: {table}")
            recommendations = self.analyze_table(table)
            all_recommendations.extend(recommendations)
        
        # Группируем рекомендации по приоритету
        high_priority = [r for r in all_recommendations if r['priority'] == 'high']
        medium_priority = [r for r in all_recommendations if r['priority'] == 'medium']
        low_priority = [r for r in all_recommendations if r['priority'] == 'low']
        
        print(f"\n📈 Найдено рекомендаций:")
        print(f"   🔴 Высокий приоритет: {len(high_priority)}")
        print(f"   🟡 Средний приоритет: {len(medium_priority)}")
        print(f"   🟢 Низкий приоритет: {len(low_priority)}\n")
        
        # Генерируем SQL скрипт
        script_lines = [
            "-- SQL скрипт для оптимизации базы данных TenderMonitor",
            "-- Сгенерировано автоматически",
            "",
            "BEGIN;",
            ""
        ]
        
        # Добавляем индексы (высокий приоритет)
        if high_priority:
            script_lines.append("-- ========================================")
            script_lines.append("-- ИНДЕКСЫ (высокий приоритет)")
            script_lines.append("-- ========================================")
            for rec in high_priority:
                if rec['type'] == 'missing_index' or rec['type'] == 'missing_unique':
                    script_lines.append(rec['sql'])
            script_lines.append("")
        
        # Добавляем внешние ключи (средний приоритет)
        if medium_priority:
            script_lines.append("-- ========================================")
            script_lines.append("-- ВНЕШНИЕ КЛЮЧИ (средний приоритет)")
            script_lines.append("-- ========================================")
            for rec in medium_priority:
                if rec['type'] == 'missing_foreign_key':
                    script_lines.append(rec['sql'])
            script_lines.append("")
        
        script_lines.append("COMMIT;")
        
        return '\n'.join(script_lines)
    
    def apply_optimizations(self, dry_run=True):
        """Применяет оптимизации к БД."""
        script = self.generate_optimization_script()
        
        if dry_run:
            print("=" * 60)
            print("SQL СКРИПТ ДЛЯ ОПТИМИЗАЦИИ (DRY RUN)")
            print("=" * 60)
            print(script)
            print("=" * 60)
            print("\n⚠️  Это был DRY RUN. Для применения изменений запустите с dry_run=False")
        else:
            print("🚀 Применение оптимизаций...")
            try:
                # Выполняем команды по одной
                commands = [cmd.strip() for cmd in script.split(';') if cmd.strip() and not cmd.strip().startswith('--')]
                for cmd in commands:
                    if cmd and cmd.upper() not in ['BEGIN', 'COMMIT']:
                        try:
                            self.cursor.execute(cmd)
                            print(f"  ✓ Выполнено: {cmd[:60]}...")
                        except Exception as e:
                            logger.warning(f"Предупреждение при выполнении '{cmd[:60]}...': {e}")
                            # Продолжаем выполнение других команд
                
                self.db_manager.connection.commit()
                print("✅ Оптимизации успешно применены!")
            except Exception as e:
                self.db_manager.connection.rollback()
                logger.error(f"Ошибка при применении оптимизаций: {e}", exc_info=True)
                print(f"❌ Ошибка: {e}")
                raise
        
        return script
    
    def close(self):
        """Закрывает соединение с БД."""
        if self.cursor:
            self.cursor.close()
        if self.db_manager:
            self.db_manager.close()


if __name__ == "__main__":
    optimizer = DatabaseOptimizer()
    try:
        # Генерируем и показываем скрипт оптимизации
        script = optimizer.apply_optimizations(dry_run=True)
        
        # Сохраняем скрипт в файл
        script_path = "database_optimization.sql"
        with open(script_path, "w", encoding="utf-8") as f:
            f.write(script)
        print(f"\n💾 SQL скрипт сохранен в файл: {script_path}")
        print("\nДля применения оптимизаций:")
        print("1. Проверьте скрипт database_optimization.sql")
        print("2. Запустите: optimizer.apply_optimizations(dry_run=False)")
        print("   или выполните SQL скрипт вручную в БД")
        
    except Exception as e:
        logger.error(f"Ошибка при анализе БД: {e}", exc_info=True)
        print(f"❌ Ошибка: {e}")
    finally:
        optimizer.close()

