"""
Простой скрипт для запуска анализа и оптимизации БД.
"""
from database_work.database_optimizer import DatabaseOptimizer

if __name__ == "__main__":
    optimizer = DatabaseOptimizer()
    try:
        # Генерируем и показываем скрипт оптимизации (dry run)
        print("=" * 60)
        print("АНАЛИЗ И ОПТИМИЗАЦИЯ БАЗЫ ДАННЫХ")
        print("=" * 60)
        script = optimizer.apply_optimizations(dry_run=True)
        
        # Сохраняем скрипт в файл
        script_path = "database_optimization.sql"
        with open(script_path, "w", encoding="utf-8") as f:
            f.write(script)
        print(f"\n💾 SQL скрипт сохранен в файл: {script_path}")
        print("\n📝 Для применения оптимизаций:")
        print("   1. Проверьте скрипт database_optimization.sql")
        print("   2. Выполните его вручную в PostgreSQL или")
        print("   3. Измените dry_run=False в этом скрипте и запустите снова")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        optimizer.close()

