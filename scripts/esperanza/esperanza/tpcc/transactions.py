from __future__ import annotations

import random

from .constants import DISTRICTS_PER_WAREHOUSE
from .random_utils import get_customer_id


def execute_transaction(conn, proc_call: str, params: tuple) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute("START TRANSACTION")
        cursor.execute(proc_call, params)
        cursor.execute("COMMIT")
    except Exception as e:
        cursor.execute("ROLLBACK")
        raise e
    finally:
        cursor.close()


def new_order(conn, w_id: int, num_warehouses: int) -> None:
    c_id = get_customer_id()
    d_id = random.randint(1, DISTRICTS_PER_WAREHOUSE)
    o_ol_cnt = random.randint(5, 15)

    ol_supply_w_id = w_id
    if random.randint(1, 100) == 1 and num_warehouses > 1:
        ol_supply_w_id = random.choice(
            [i for i in range(1, num_warehouses + 1) if i != w_id]
        )

    execute_transaction(
        conn,
        "CALL NewOrder(%s, %s, %s, %s, %s)",
        (w_id, c_id, d_id, o_ol_cnt, ol_supply_w_id),
    )


def payment(conn, w_id: int, num_warehouses: int) -> None:
    d_id = random.randint(1, DISTRICTS_PER_WAREHOUSE)
    c_id = get_customer_id()
    payment_amount = random.randint(100, 500000) / 100.0

    cust_w_id, cust_d_id = w_id, d_id
    if random.randint(1, 100) > 85 and num_warehouses > 1:
        cust_d_id = random.randint(1, DISTRICTS_PER_WAREHOUSE)
        cust_w_id = random.choice(
            [i for i in range(1, num_warehouses + 1) if i != w_id]
        )

    execute_transaction(
        conn,
        "CALL Payment(%s, %s, %s, %s, %s, %s)",
        (w_id, d_id, cust_d_id, cust_w_id, c_id, payment_amount),
    )


def delivery(conn, w_id: int) -> None:
    carrier_id = random.randint(1, 10)
    execute_transaction(
        conn,
        "CALL Delivery(%s, %s, %s, %s)",
        (w_id, 1, DISTRICTS_PER_WAREHOUSE, carrier_id),
    )


def order_status(conn) -> None:
    pass


def stock_level(conn) -> None:
    pass
