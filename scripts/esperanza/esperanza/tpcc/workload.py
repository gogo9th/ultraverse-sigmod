from __future__ import annotations

import random
import sys

from .constants import (
    DEFAULT_QUERY_COUNT,
    DEFAULT_SCALE_FACTOR,
    DELIVERY_WEIGHT,
    NEW_ORDER_WEIGHT,
    PAYMENT_WEIGHT,
)
from .transactions import delivery, new_order, payment

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional dependency for progress UI
    tqdm = None


class TPCCWorkloadExecutor:
    WEIGHTS = {
        "new_order": NEW_ORDER_WEIGHT,
        "payment": PAYMENT_WEIGHT,
        "delivery": DELIVERY_WEIGHT,
    }

    def __init__(
        self,
        conn,
        scale_factor: int = DEFAULT_SCALE_FACTOR,
        query_count: int = DEFAULT_QUERY_COUNT,
    ) -> None:
        self.conn = conn
        self.scale_factor = scale_factor
        self.query_count = query_count

        self.tx_choices: list[str] = []
        for tx_name, weight in self.WEIGHTS.items():
            self.tx_choices.extend([tx_name] * weight)

    def run(self) -> dict:
        stats = {"new_order": 0, "payment": 0, "delivery": 0, "errors": 0}
        progress = self._new_progress(self.query_count, "workload")

        try:
            for i in range(self.query_count):
                w_id = random.randint(1, self.scale_factor)
                tx_type = random.choice(self.tx_choices)

                try:
                    if tx_type == "new_order":
                        new_order(self.conn, w_id, self.scale_factor)
                        stats["new_order"] += 1
                    elif tx_type == "payment":
                        payment(self.conn, w_id, self.scale_factor)
                        stats["payment"] += 1
                    elif tx_type == "delivery":
                        delivery(self.conn, w_id)
                        stats["delivery"] += 1
                except Exception as exc:
                    stats["errors"] += 1
                    print(f"TPCC workload error: {tx_type} failed: {exc}")

                if progress is not None:
                    progress.update(1)
                elif (i + 1) % 10000 == 0:
                    print(f"Progress: {i + 1}/{self.query_count}")
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
