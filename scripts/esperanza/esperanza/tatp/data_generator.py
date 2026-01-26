from __future__ import annotations

import random
import sys
from typing import Iterable

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional dependency for progress UI
    tqdm = None

from .constants import DEFAULT_NUM_SUBSCRIBERS
from .random_utils import (
    generate_sub_nbr,
    random_ai_type,
    random_alpha_string,
    random_bit,
    random_byte2,
    random_data,
    random_hex,
    random_location,
    random_numberx,
    random_sf_type,
    random_start_time,
)


class TATPDataGenerator:
    def __init__(
        self,
        conn,
        num_subscribers: int = DEFAULT_NUM_SUBSCRIBERS,
        batch_size: int = 1000,
    ) -> None:
        self.conn = conn
        self.num_subscribers = num_subscribers
        self.batch_size = batch_size
        self.rng = random.Random()
        self._special_facility_types: list[list[int]] | None = None

    def generate_all(self) -> None:
        subscriber_bar = self._new_progress(self.num_subscribers, "subscriber")
        access_info_bar = self._new_progress(None, "access_info")
        special_facility_bar = self._new_progress(None, "special_facility")
        call_forwarding_bar = self._new_progress(None, "call_forwarding")

        try:
            self.generate_subscribers(subscriber_bar)
            self.generate_access_info(access_info_bar)
            self.generate_special_facility(special_facility_bar)
            self.generate_call_forwarding(call_forwarding_bar)
        finally:
            self._close_progress(subscriber_bar)
            self._close_progress(access_info_bar)
            self._close_progress(special_facility_bar)
            self._close_progress(call_forwarding_bar)

    def generate_subscribers(self, progress: object | None = None) -> None:
        sql = (
            "INSERT INTO subscriber ("
            "s_id, sub_nbr, "
            "bit_1, bit_2, bit_3, bit_4, bit_5, bit_6, bit_7, bit_8, bit_9, bit_10, "
            "hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, hex_7, hex_8, hex_9, hex_10, "
            "byte2_1, byte2_2, byte2_3, byte2_4, byte2_5, byte2_6, byte2_7, byte2_8, "
            "byte2_9, byte2_10, msc_location, vlr_location"
            ") VALUES ("
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
            ")"
        )

        def rows() -> Iterable[tuple]:
            for s_id in range(1, self.num_subscribers + 1):
                bits = [random_bit(self.rng) for _ in range(10)]
                hexes = [random_hex(self.rng) for _ in range(10)]
                byte2s = [random_byte2(self.rng) for _ in range(10)]
                yield (
                    s_id,
                    generate_sub_nbr(s_id),
                    *bits,
                    *hexes,
                    *byte2s,
                    random_location(self.rng),
                    random_location(self.rng),
                )

        self._execute_batches(sql, rows(), progress=progress)

    def generate_access_info(self, progress: object | None = None) -> None:
        sql = (
            "INSERT INTO access_info (s_id, ai_type, data1, data2, data3, data4) "
            "VALUES (%s, %s, %s, %s, %s, %s)"
        )

        def rows() -> Iterable[tuple]:
            for s_id in range(1, self.num_subscribers + 1):
                ai_count = self.rng.randint(1, 4)
                ai_types = self._unique_values(ai_count, random_ai_type)
                for ai_type in ai_types:
                    yield (
                        s_id,
                        ai_type,
                        random_data(self.rng),
                        random_data(self.rng),
                        random_alpha_string(self.rng, 3),
                        random_alpha_string(self.rng, 5),
                    )

        self._execute_batches(sql, rows(), progress=progress)

    def generate_special_facility(self, progress: object | None = None) -> None:
        sql = (
            "INSERT INTO special_facility "
            "(s_id, sf_type, is_active, error_cntrl, data_a, data_b) "
            "VALUES (%s, %s, %s, %s, %s, %s)"
        )
        self._special_facility_types = [[] for _ in range(self.num_subscribers + 1)]

        def rows() -> Iterable[tuple]:
            for s_id in range(1, self.num_subscribers + 1):
                sf_count = self.rng.randint(1, 4)
                sf_types = self._unique_values(sf_count, random_sf_type)
                self._special_facility_types[s_id] = sf_types
                for sf_type in sf_types:
                    yield (
                        s_id,
                        sf_type,
                        random_bit(self.rng),
                        random_data(self.rng),
                        random_data(self.rng),
                        random_alpha_string(self.rng, 5),
                    )

        self._execute_batches(sql, rows(), progress=progress)

    def generate_call_forwarding(self, progress: object | None = None) -> None:
        if self._special_facility_types is None:
            raise RuntimeError("generate_special_facility must be called before generate_call_forwarding")

        sql = (
            "INSERT INTO call_forwarding "
            "(s_id, sf_type, start_time, end_time, numberx) "
            "VALUES (%s, %s, %s, %s, %s)"
        )

        def rows() -> Iterable[tuple]:
            for s_id in range(1, self.num_subscribers + 1):
                for sf_type in self._special_facility_types[s_id]:
                    start_count = self.rng.randint(0, 3)
                    if start_count == 0:
                        continue
                    start_times = self._unique_values(start_count, random_start_time)
                    for start_time in start_times:
                        end_time = start_time + self.rng.randint(1, 8)
                        yield (
                            s_id,
                            sf_type,
                            start_time,
                            end_time,
                            random_numberx(self.rng),
                        )

        self._execute_batches(sql, rows(), progress=progress)

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

    def _unique_values(self, count: int, chooser) -> list[int]:
        values: list[int] = []
        while len(values) < count:
            value = chooser(self.rng)
            if value not in values:
                values.append(value)
        return values
