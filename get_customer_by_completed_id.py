#!/usr/bin/env python3
"""
Вспомогательный скрипт: получить данные заказчика по контракту
из таблицы reestr_contract_44_fz_completed по ID контракта.
"""

from database_work.database_connection import DatabaseManager


def get_customer_by_completed_id(contract_id: int) -> None:
    db = DatabaseManager()
    try:
        cur = db.cursor
        cur.execute(
            """
            SELECT 
                c.id,
                c.customer_inn,
                c.customer_full_name,
                c.customer_short_name
            FROM reestr_contract_44_fz_completed rc
            JOIN customer c ON rc.customer_id = c.id
            WHERE rc.id = %s;
            """,
            (contract_id,),
        )
        row = cur.fetchone()
        if not row:
            print(f"Заказчик для контракта {contract_id} не найден")
            return

        cid, inn, full_name, short_name = row
        print("Заказчик по контракту из reestr_contract_44_fz_completed:")
        print(f"  ID: {cid}")
        print(f"  ИНН: {inn}")
        print(f"  Полное наименование: {full_name}")
        print(f"  Краткое наименование: {short_name}")
    finally:
        db.close()


if __name__ == "__main__":
    get_customer_by_completed_id(367870)

