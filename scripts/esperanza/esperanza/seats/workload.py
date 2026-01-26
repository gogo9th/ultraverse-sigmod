from __future__ import annotations

import random
import sys
from collections import deque
from datetime import datetime, timedelta

from .constants import (
    FLIGHTS_NUM_SEATS,
    MAX_RESERVATION_IATTR,
    PROB_FIND_FLIGHTS_NEARBY_AIRPORT,
    PROB_REQUEUE_DELETED_RESERVATION,
    PROB_UPDATE_WITH_CUSTOMER_ID_STR,
    RESERVATION_PRICE_MAX,
    RESERVATION_PRICE_MIN,
    TRANSACTION_WEIGHTS,
)
from .transactions import (
    delete_reservation,
    find_flights,
    find_open_seats,
    new_reservation,
    update_customer,
    update_reservation,
)

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional dependency for progress UI
    tqdm = None


class SeatsWorkloadExecutor:
    RECENT_RESERVATION_LIMIT = 1000
    REQUEUE_LIMIT = 1000
    FLIGHT_REFRESH_INTERVAL = 1000
    MAX_NEW_RESERVATION_ATTEMPTS = 5
    RESERVATION_ATTR_COUNT = 9

    def __init__(self, conn, num_airports: int, num_customers: int, query_count: int) -> None:
        self.conn = conn
        self.num_airports = int(num_airports)
        self.num_customers = int(num_customers)
        self.query_count = int(query_count)
        self.rng = random.Random()

        self.flights: list[str] = []
        self.customers: list[tuple[str, str, int]] = []
        self.recent_reservations: deque[tuple] = deque(maxlen=self.RECENT_RESERVATION_LIMIT)
        self.reservation_counter: int = 0

        self._requeue_reservations: deque[tuple] = deque(maxlen=self.REQUEUE_LIMIT)

    def run(self) -> dict:
        self._build_cache()
        stats = {tx_name: 0 for tx_name in TRANSACTION_WEIGHTS}
        stats["errors"] = 0
        progress = self._new_progress(self.query_count, "workload")

        try:
            for i in range(self.query_count):
                if (
                    self.FLIGHT_REFRESH_INTERVAL > 0
                    and i > 0
                    and i % self.FLIGHT_REFRESH_INTERVAL == 0
                ):
                    self._refresh_flights()

                tx_type = self.rng.choices(
                    list(TRANSACTION_WEIGHTS.keys()),
                    weights=list(TRANSACTION_WEIGHTS.values()),
                    k=1,
                )[0]

                try:
                    ok = self._dispatch_transaction(tx_type)
                    if ok:
                        stats[tx_type] += 1
                    else:
                        stats["errors"] += 1
                except Exception as exc:
                    stats["errors"] += 1
                    print(f"SEATS workload error: {tx_type} failed: {exc}")

                if progress is not None:
                    progress.update(1)
        finally:
            if progress is not None:
                progress.close()

        return stats

    def _dispatch_transaction(self, tx_type: str) -> bool:
        if tx_type == "find_flights":
            return self._do_find_flights()
        if tx_type == "find_open_seats":
            return self._do_find_open_seats()
        if tx_type == "new_reservation":
            return self._do_new_reservation()
        if tx_type == "update_reservation":
            return self._do_update_reservation()
        if tx_type == "update_customer":
            return self._do_update_customer()
        if tx_type == "delete_reservation":
            return self._do_delete_reservation()
        raise ValueError(f"unknown transaction type: {tx_type}")

    def _build_cache(self) -> None:
        self._refresh_flights()
        self._refresh_customers()
        self._load_recent_reservations()
        self._load_reservation_counter()

    def _refresh_flights(self) -> None:
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT f_id FROM flight")
            self.flights = [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _refresh_customers(self) -> None:
        cursor = self.conn.cursor()
        try:
            sql = "SELECT c_id, c_id_str, c_base_ap_id FROM customer2"
            params = ()
            if self.num_customers > 0:
                sql += " LIMIT %s"
                params = (self.num_customers,)
            cursor.execute(sql, params)
            self.customers = [(row[0], row[1], int(row[2])) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _load_recent_reservations(self) -> None:
        limit = self.recent_reservations.maxlen or 0
        if limit <= 0:
            return
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "SELECT r_id, r_c_id, r_f_id, r_seat FROM reservation ORDER BY r_id DESC LIMIT %s",
                (limit,),
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()
        self.recent_reservations.clear()
        for row in rows:
            self.recent_reservations.append((row[0], row[1], row[2], int(row[3])))

    def _load_reservation_counter(self) -> None:
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT COALESCE(MAX(r_id), 0) + 1 FROM reservation")
            row = cursor.fetchone()
            self.reservation_counter = int(row[0]) if row else 0
        finally:
            cursor.close()

    def _do_find_flights(self) -> bool:
        if self.num_airports < 2:
            return False

        depart_airport_id = self.rng.randint(0, self.num_airports - 1)
        arrive_airport_id = self.rng.randint(0, self.num_airports - 1)
        while arrive_airport_id == depart_airport_id:
            arrive_airport_id = self.rng.randint(0, self.num_airports - 1)

        day_offset = self.rng.randint(-30, 30)
        target_date = datetime.now() + timedelta(days=day_offset)
        start_date = datetime(target_date.year, target_date.month, target_date.day)
        end_date = start_date + timedelta(days=1)

        distance = None
        if self.rng.randint(1, 100) <= PROB_FIND_FLIGHTS_NEARBY_AIRPORT:
            distance = 100

        cursor = self.conn.cursor()
        try:
            results = find_flights(
                self.conn,
                cursor,
                depart_airport_id,
                arrive_airport_id,
                start_date,
                end_date,
                distance,
            )
        finally:
            cursor.close()

        if results:
            self._merge_flights([row[0] for row in results])
        return True

    def _do_find_open_seats(self) -> bool:
        flight_id = self._choose_flight()
        if flight_id is None:
            return False

        open_seats = self._find_open_seats(flight_id)
        if open_seats:
            self._touch_flight(flight_id)
        else:
            self._drop_flight(flight_id)
        return True

    def _do_new_reservation(self) -> bool:
        requeued = self._requeue_reservations.popleft() if self._requeue_reservations else None
        customer: tuple[str, str, int] | None = None

        for _ in range(self.MAX_NEW_RESERVATION_ATTEMPTS):
            if requeued is not None:
                r_id, c_id, f_id, seatnum = requeued
                flight_id = f_id
            else:
                if customer is None:
                    customer = self._choose_customer()
                if customer is None:
                    return False
                flight_id = self._choose_flight()
                if flight_id is None:
                    return False
                c_id = customer[0]
                r_id = self._next_reservation_id()
                seatnum = None

            open_seats = self._find_open_seats(flight_id)
            if open_seats:
                if seatnum is None or seatnum not in open_seats:
                    seatnum = self.rng.choice(open_seats)
                price = self.rng.uniform(RESERVATION_PRICE_MIN, RESERVATION_PRICE_MAX)
                attrs = [self._random_iattr() for _ in range(self.RESERVATION_ATTR_COUNT)]

                cursor = self.conn.cursor()
                try:
                    new_reservation(
                        self.conn,
                        cursor,
                        r_id,
                        c_id,
                        flight_id,
                        seatnum,
                        price,
                        *attrs,
                    )
                finally:
                    cursor.close()

                self.recent_reservations.append((r_id, c_id, flight_id, seatnum))
                self._touch_flight(flight_id)
                return True

            self._drop_flight(flight_id)
            if requeued is not None:
                self._requeue_reservations.append(requeued)
                requeued = None

        return False

    def _do_update_reservation(self) -> bool:
        idx = self._choose_recent_reservation_index()
        if idx is None:
            return False

        r_id, c_id, f_id, seatnum = self.recent_reservations[idx]
        new_seatnum = self.rng.randint(0, FLIGHTS_NUM_SEATS - 1)
        attr_idx = 0
        if MAX_RESERVATION_IATTR > 0:
            attr_idx = self.rng.randint(0, MAX_RESERVATION_IATTR - 1)
        attr_val = self._random_iattr()

        cursor = self.conn.cursor()
        try:
            update_reservation(
                self.conn,
                cursor,
                r_id,
                f_id,
                c_id,
                new_seatnum,
                attr_idx,
                attr_val,
            )
        finally:
            cursor.close()

        self.recent_reservations[idx] = (r_id, c_id, f_id, new_seatnum)
        return True

    def _do_update_customer(self) -> bool:
        customer = self._choose_customer()
        if customer is None:
            return False

        c_id, c_id_str, _ = customer
        use_c_id_str = self.rng.randint(1, 100) <= PROB_UPDATE_WITH_CUSTOMER_ID_STR
        if use_c_id_str:
            c_id_val = None
            c_id_str_val = c_id_str
        else:
            c_id_val = c_id
            c_id_str_val = None

        attr0 = self._random_iattr()
        attr1 = self._random_iattr()

        cursor = self.conn.cursor()
        try:
            update_customer(self.conn, cursor, c_id_val, c_id_str_val, attr0, attr1)
        finally:
            cursor.close()

        return True

    def _do_delete_reservation(self) -> bool:
        idx = self._choose_recent_reservation_index()
        if idx is None:
            return False

        r_id, c_id, f_id, seatnum = self.recent_reservations[idx]

        cursor = self.conn.cursor()
        try:
            delete_reservation(self.conn, cursor, f_id, c_id, None, None, None)
        finally:
            cursor.close()

        del self.recent_reservations[idx]
        if self.rng.randint(1, 100) <= PROB_REQUEUE_DELETED_RESERVATION:
            self._requeue_reservations.append((r_id, c_id, f_id, seatnum))
            self._touch_flight(f_id)
        return True

    def _choose_flight(self) -> str | None:
        if not self.flights:
            self._refresh_flights()
        if not self.flights:
            return None
        return self.rng.choice(self.flights)

    def _choose_customer(self) -> tuple[str, str, int] | None:
        if not self.customers:
            self._refresh_customers()
        if not self.customers:
            return None
        return self.rng.choice(self.customers)

    def _choose_recent_reservation_index(self) -> int | None:
        if not self.recent_reservations:
            self._load_recent_reservations()
        if not self.recent_reservations:
            return None
        return self.rng.randrange(len(self.recent_reservations))

    def _find_open_seats(self, flight_id: str) -> list[int]:
        cursor = self.conn.cursor()
        try:
            return find_open_seats(self.conn, cursor, flight_id)
        finally:
            cursor.close()

    def _merge_flights(self, flight_ids: list[str]) -> None:
        if not flight_ids:
            return
        existing = set(self.flights)
        for flight_id in flight_ids:
            if flight_id not in existing:
                self.flights.append(flight_id)
                existing.add(flight_id)

    def _touch_flight(self, flight_id: str) -> None:
        try:
            self.flights.remove(flight_id)
        except ValueError:
            pass
        self.flights.append(flight_id)

    def _drop_flight(self, flight_id: str) -> None:
        try:
            self.flights.remove(flight_id)
        except ValueError:
            pass

    def _next_reservation_id(self) -> int:
        r_id = self.reservation_counter
        self.reservation_counter += 1
        return r_id

    def _random_iattr(self) -> int:
        return self.rng.randint(0, 1 << 30)

    def _new_progress(self, total: int, desc: str):
        if tqdm is None or not sys.stderr.isatty():
            return None
        return tqdm(
            total=total,
            desc=desc,
            unit="tx",
            ascii=True,
            dynamic_ncols=True,
            mininterval=0.5,
        )
