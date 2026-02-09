#!/usr/bin/env python3
"""
MODULE: scripts.analyze_module_sizes
RESPONSIBILITY: Analyzing project module sizes for refactoring.
ALLOWED: os, pathlib.
FORBIDDEN: None.
ERRORS: None.

Анализ размеров модулей проекта для рефакторинга.
"""
import os
from pathlib import Path


def count_lines(file_path):
    """Подсчет строк в файле."""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return sum(1 for _ in f)
    except Exception:
        return 0


def analyze_modules():
    """Анализ всех Python модулей проекта."""
    project_root = Path(__file__).parent.parent
    exclude_dirs = {'.git', '__pycache__', 'venv', '.venv', 'node_modules', 'tests'}
    max_lines = 150
    
    results = []
    
    for root, dirs, files in os.walk(project_root):
        # Исключаем директории
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        
        for file in files:
            if not file.endswith('.py'):
                continue
            
            file_path = Path(root) / file
            try:
                rel_path = file_path.relative_to(project_root)
            except ValueError:
                continue
            
            lines = count_lines(file_path)
            if lines > max_lines:
                results.append((lines, str(rel_path)))
    
    # Сортируем по количеству строк (по убыванию)
    results.sort(reverse=True)
    
    print(f"\n{'='*80}")
    print(f"Модули, превышающие {max_lines} строк (всего: {len(results)})")
    print(f"{'='*80}\n")
    
    for lines, path in results:
        print(f"{lines:5d}  {path}")
    
    return results


if __name__ == '__main__':
    analyze_modules()

