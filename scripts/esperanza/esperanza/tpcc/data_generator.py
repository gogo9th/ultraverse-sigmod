from __future__ import annotations

import random
import string
import sys
from datetime import datetime
from decimal import Decimal
from typing import Iterable

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional dependency for progress UI
    tqdm = None

from .constants import (
    C_LAST_LOAD,
    CUSTOMERS_PER_DISTRICT,
    DEFAULT_SCALE_FACTOR,
    DISTRICTS_PER_WAREHOUSE,
    ITEMS_COUNT,
    NEW_ORDERS_PER_DISTRICT,
)
from .random_utils import NURand, make_last_name, random_astring, random_nstring


class TPCCDataGenerator:
    def __init__(self, conn, scale_factor: int = DEFAULT_SCALE_FACTOR):
        self.conn = conn
        self.scale_factor = scale_factor
        self.batch_size = 1000

    def generate_all(self) -> None:
        """Generate all TPC-C initial data."""
        items_bar = self._new_progress(ITEMS_COUNT, "items")
        try:
            self.generate_items(items_bar)
        finally:
            self._close_progress(items_bar)

        warehouse_bar = self._new_progress(self.scale_factor, "warehouse")
        stock_bar = self._new_progress(ITEMS_COUNT * self.scale_factor, "stock")
        district_bar = self._new_progress(
            DISTRICTS_PER_WAREHOUSE * self.scale_factor,
            "district",
        )
        customer_total = CUSTOMERS_PER_DISTRICT * DISTRICTS_PER_WAREHOUSE * self.scale_factor
        customer_bar = self._new_progress(customer_total, "customer")
        history_bar = self._new_progress(customer_total, "history")
        order_total = CUSTOMERS_PER_DISTRICT * DISTRICTS_PER_WAREHOUSE * self.scale_factor
        order_bar = self._new_progress(order_total, "order")
        new_order_total = NEW_ORDERS_PER_DISTRICT * DISTRICTS_PER_WAREHOUSE * self.scale_factor
        new_order_bar = self._new_progress(new_order_total, "new_order")
        order_line_bar = self._new_progress(None, "order_line")

        try:
            for w_id in range(1, self.scale_factor + 1):
                self.generate_warehouse(w_id, warehouse_bar)
                self.generate_stock(w_id, stock_bar)
                self.generate_districts(w_id, district_bar)
                self.generate_customers(w_id, customer_bar, history_bar)
                self.generate_orders(w_id, order_bar, new_order_bar, order_line_bar)
        finally:
            self._close_progress(warehouse_bar)
            self._close_progress(stock_bar)
            self._close_progress(district_bar)
            self._close_progress(customer_bar)
            self._close_progress(history_bar)
            self._close_progress(order_bar)
            self._close_progress(new_order_bar)
            self._close_progress(order_line_bar)

    def generate_items(self, progress: object | None = None) -> None:
        """Generate 100,000 item rows."""
        sql = (
            "INSERT INTO item (i_id, i_name, i_price, i_data, i_im_id) "
            "VALUES (%s, %s, %s, %s, %s)"
        )

        def rows() -> Iterable[tuple]:
            for i_id in range(1, ITEMS_COUNT + 1):
                i_name = random_astring(14, 24)
                i_price = self._random_price()
                i_data = self._random_data_with_original(26, 50)
                i_im_id = random.randint(1, 10000)
                yield (i_id, i_name, i_price, i_data, i_im_id)

        self._execute_batches(sql, rows(), progress=progress)

    def generate_warehouse(self, w_id: int, progress: object | None = None) -> None:
        """Generate a single warehouse row."""
        sql = (
            "INSERT INTO warehouse "
            "(w_id, w_ytd, w_tax, w_name, w_street_1, w_street_2, w_city, w_state, w_zip) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        row = (
            w_id,
            Decimal("300000.00"),
            self._random_tax(),
            random_astring(6, 10),
            random_astring(10, 20),
            random_astring(10, 20),
            random_astring(10, 20),
            self._random_state(),
            self._random_zip(),
        )
        self._execute_batches(sql, [row], progress=progress)

    def generate_stock(self, w_id: int, progress: object | None = None) -> None:
        """Generate stock rows for a warehouse."""
        sql = (
            "INSERT INTO stock "
            "(s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, "
            "s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, "
            "s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )

        def rows() -> Iterable[tuple]:
            for i_id in range(1, ITEMS_COUNT + 1):
                s_quantity = random.randint(10, 100)
                s_data = self._random_data_with_original(26, 50)
                s_dist = [random_astring(24, 24) for _ in range(10)]
                yield (
                    w_id,
                    i_id,
                    s_quantity,
                    Decimal("0.00"),
                    0,
                    0,
                    s_data,
                    *s_dist,
                )

        self._execute_batches(sql, rows(), progress=progress)

    def generate_districts(self, w_id: int, progress: object | None = None) -> None:
        """Generate 10 districts for a warehouse."""
        sql = (
            "INSERT INTO district "
            "(d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, "
            "d_city, d_state, d_zip) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        rows = []
        for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
            rows.append(
                (
                    w_id,
                    d_id,
                    Decimal("30000.00"),
                    self._random_tax(),
                    3001,
                    random_astring(6, 10),
                    random_astring(10, 20),
                    random_astring(10, 20),
                    random_astring(10, 20),
                    self._random_state(),
                    self._random_zip(),
                )
            )
        self._execute_batches(sql, rows, progress=progress)

    def generate_customers(
        self,
        w_id: int,
        customer_progress: object | None = None,
        history_progress: object | None = None,
    ) -> None:
        """Generate customers and history rows for each district."""
        customer_sql = (
            "INSERT INTO customer "
            "(c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, "
            "c_credit_lim, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, "
            "c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, "
            "c_middle, c_data) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        history_sql = (
            "INSERT INTO history "
            "(h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        )

        for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
            customers = []
            histories = []
            now = datetime.now()
            for c_id in range(1, CUSTOMERS_PER_DISTRICT + 1):
                if c_id <= 1000:
                    c_last = make_last_name(c_id - 1)
                else:
                    c_last = make_last_name(NURand(255, C_LAST_LOAD, 0, 999))

                c_credit = "BC" if random.random() < 0.1 else "GC"
                customers.append(
                    (
                        w_id,
                        d_id,
                        c_id,
                        self._random_discount(),
                        c_credit,
                        c_last,
                        random_astring(8, 16),
                        Decimal("50000.00"),
                        Decimal("-10.00"),
                        Decimal("10.00"),
                        1,
                        0,
                        random_astring(10, 20),
                        random_astring(10, 20),
                        random_astring(10, 20),
                        self._random_state(),
                        self._random_zip(),
                        random_nstring(16, 16),
                        now,
                        "OE",
                        random_astring(300, 500),
                    )
                )
                histories.append(
                    (
                        c_id,
                        d_id,
                        w_id,
                        d_id,
                        w_id,
                        now,
                        Decimal("10.00"),
                        random_astring(12, 24),
                    )
                )

            self._execute_batches(customer_sql, customers, progress=customer_progress)
            self._execute_batches(history_sql, histories, progress=history_progress)

    def generate_orders(
        self,
        w_id: int,
        order_progress: object | None = None,
        new_order_progress: object | None = None,
        order_line_progress: object | None = None,
    ) -> None:
        """Generate orders, order lines, and new orders for each district."""
        order_sql = (
            "INSERT INTO oorder "
            "(o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        )
        new_order_sql = (
            "INSERT INTO new_order (no_w_id, no_d_id, no_o_id) VALUES (%s, %s, %s)"
        )
        order_line_sql = (
            "INSERT INTO order_line "
            "(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, "
            "ol_supply_w_id, ol_quantity, ol_dist_info) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )

        new_order_start = CUSTOMERS_PER_DISTRICT - NEW_ORDERS_PER_DISTRICT + 1

        for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
            customer_ids = list(range(1, CUSTOMERS_PER_DISTRICT + 1))
            random.shuffle(customer_ids)

            orders = []
            new_orders = []
            order_lines = []

            for o_id in range(1, CUSTOMERS_PER_DISTRICT + 1):
                o_c_id = customer_ids[o_id - 1]
                o_ol_cnt = random.randint(5, 15)
                o_entry_d = datetime.now()
                if o_id < new_order_start:
                    o_carrier_id = random.randint(1, 10)
                    ol_delivery_d = o_entry_d
                else:
                    o_carrier_id = None
                    ol_delivery_d = None
                    new_orders.append((w_id, d_id, o_id))

                orders.append(
                    (w_id, d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, 1, o_entry_d)
                )

                for ol_number in range(1, o_ol_cnt + 1):
                    order_lines.append(
                        (
                            w_id,
                            d_id,
                            o_id,
                            ol_number,
                            random.randint(1, ITEMS_COUNT),
                            ol_delivery_d,
                            self._random_amount(),
                            w_id,
                            random.randint(1, 10),
                            random_astring(24, 24),
                        )
                    )

            self._execute_batches(order_sql, orders, progress=order_progress)
            if new_orders:
                self._execute_batches(new_order_sql, new_orders, progress=new_order_progress)
            self._execute_batches(order_line_sql, order_lines, progress=order_line_progress)

    def _execute_batches(
        self,
        sql: str,
        rows: Iterable[tuple],
        progress: object | None = None,
    ) -> None:
        cursor = self.conn.cursor()
        batch = []
        for row in rows:
            batch.append(row)
            if len(batch) >= self.batch_size:
                cursor.executemany(sql, batch)
                self.conn.commit()
                if progress is not None:
                    progress.update(len(batch))
                batch.clear()
        if batch:
            cursor.executemany(sql, batch)
            self.conn.commit()
            if progress is not None:
                progress.update(len(batch))
        cursor.close()

    def _new_progress(self, total: int | None, desc: str) -> object | None:
        if tqdm is None:
            return None
        return tqdm(
            total=total,
            desc=desc,
            unit="rows",
            ascii=True,
            dynamic_ncols=True,
            mininterval=0.5,
            disable=not sys.stderr.isatty(),
        )

    def _close_progress(self, progress: object | None) -> None:
        if progress is not None:
            progress.close()

    def _random_tax(self) -> Decimal:
        return Decimal(random.randint(0, 2000)) / Decimal("10000")

    def _random_discount(self) -> Decimal:
        return Decimal(random.randint(0, 5000)) / Decimal("10000")

    def _random_price(self) -> Decimal:
        return Decimal(random.randint(100, 10000)) / Decimal("100")

    def _random_amount(self) -> Decimal:
        return Decimal(random.randint(1, 999999)) / Decimal("100")

    def _random_state(self) -> str:
        return "".join(random.choice(string.ascii_uppercase) for _ in range(2))

    def _random_zip(self) -> str:
        return random_nstring(4, 4) + "11111"

    def _random_data_with_original(self, min_len: int, max_len: int) -> str:
        data = random_astring(min_len, max_len)
        if random.random() < 0.1:
            marker = "ORIGINAL"
            pos = random.randint(0, len(data) - len(marker))
            data = data[:pos] + marker + data[pos + len(marker):]
        return data
