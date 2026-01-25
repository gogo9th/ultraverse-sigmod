from __future__ import annotations

import random
import sys

from .constants import TRANSACTION_WEIGHTS
from .random_utils import (
    generate_sub_nbr,
    random_ai_type,
    random_bit,
    random_data,
    random_location,
    random_numberx,
    random_s_id,
    random_sf_type,
    random_start_time,
)
from .transactions import (
    delete_call_forwarding,
    get_access_data,
    get_new_destination,
    get_subscriber_data,
    insert_call_forwarding,
    update_location,
    update_subscriber_data,
)

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional dependency for progress UI
    tqdm = None


class TATPWorkloadExecutor:
    def __init__(self, conn, num_subscribers: int, query_count: int) -> None:
        self.conn = conn
        self.num_subscribers = num_subscribers
        self.query_count = query_count
        self.rng = random.Random()

    def run(self) -> dict:
        stats = {tx_name: 0 for tx_name in TRANSACTION_WEIGHTS}
        progress = self._new_progress(self.query_count, "workload")

        try:
            for _ in range(self.query_count):
                tx_type = random.choices(
                    list(TRANSACTION_WEIGHTS.keys()),
                    weights=list(TRANSACTION_WEIGHTS.values()),
                    k=1,
                )[0]

                s_id = random_s_id(self.rng, self.num_subscribers)
                sub_nbr = generate_sub_nbr(s_id)
                ai_type = random_ai_type(self.rng)
                sf_type = random_sf_type(self.rng)
                start_time = random_start_time(self.rng)
                end_time = start_time + self.rng.randint(1, 8)

                cursor = None
                try:
                    if tx_type == "get_access_data":
                        cursor = self.conn.cursor()
                        get_access_data(self.conn, cursor, s_id, ai_type)
                    elif tx_type == "get_subscriber_data":
                        cursor = self.conn.cursor()
                        get_subscriber_data(self.conn, cursor, s_id)
                    elif tx_type == "get_new_destination":
                        cursor = self.conn.cursor()
                        get_new_destination(self.conn, cursor, s_id, sf_type, start_time, end_time)
                    elif tx_type == "update_location":
                        vlr_location = random_location(self.rng)
                        update_location(self.conn, sub_nbr, vlr_location)
                    elif tx_type == "delete_call_forwarding":
                        delete_call_forwarding(self.conn, sub_nbr, sf_type, start_time)
                    elif tx_type == "insert_call_forwarding":
                        numberx = random_numberx(self.rng)
                        insert_call_forwarding(
                            self.conn,
                            sub_nbr,
                            sf_type,
                            start_time,
                            end_time,
                            numberx,
                        )
                    elif tx_type == "update_subscriber_data":
                        bit_1 = random_bit(self.rng)
                        data_a = random_data(self.rng)
                        update_subscriber_data(self.conn, s_id, bit_1, data_a, sf_type)
                    else:
                        raise ValueError(f"unknown transaction type: {tx_type}")

                    stats[tx_type] += 1
                finally:
                    if cursor is not None:
                        cursor.close()

                if progress is not None:
                    progress.update(1)
        finally:
            if progress is not None:
                progress.close()

        return stats

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
