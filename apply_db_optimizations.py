"""
Скрипт для применения оптимизаций базы данных.
"""
from database_work.database_optimizer import DatabaseOptimizer

if __name__ == "__main__":
    print("=" * 60)
    print("ПРИМЕНЕНИЕ ОПТИМИЗАЦИЙ БАЗЫ ДАННЫХ")
    print("=" * 60)
    print("\n⚠️  ВНИМАНИЕ: Перед применением убедитесь, что:")
    print("   1. Сделана резервная копия БД")
    print("   2. Проверен SQL скрипт database_optimization.sql")
    print("\nПродолжить? (yes/no): ", end="")
    
    response = input().strip().lower()
    if response not in ['yes', 'y', 'да', 'д']:
        print("❌ Операция отменена")
        exit(0)
    
    optimizer = DatabaseOptimizer()
    try:
        # Применяем оптимизации
        optimizer.apply_optimizations(dry_run=False)
        print("\n✅ Оптимизации успешно применены!")
        print("\n💡 Рекомендуется перезапустить программу для использования новых индексов")
        
    except Exception as e:
        print(f"\n❌ Ошибка при применении оптимизаций: {e}")
        import traceback
        traceback.print_exc()
    finally:
        optimizer.close()

