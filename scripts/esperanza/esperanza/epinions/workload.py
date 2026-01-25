from __future__ import annotations

import random
import sys

from .constants import NAME_LENGTH, TITLE_LENGTH, TRANSACTION_WEIGHTS
from .random_utils import random_string
from .transactions import (
    get_avg_rating_by_trusted_user,
    get_item_avg_rating,
    get_item_reviews_by_trusted_user,
    get_review_item_by_id,
    get_reviews_by_user,
    update_item_title,
    update_review_rating,
    update_trust_rating,
    update_user_name,
)

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional dependency for progress UI
    tqdm = None


class EpinionsWorkloadExecutor:
    def __init__(self, conn, num_users: int, num_items: int, query_count: int) -> None:
        self.conn = conn
        self.num_users = num_users
        self.num_items = num_items
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
                cursor = self.conn.cursor()
                try:
                    if tx_type == "get_review_item_by_id":
                        i_id = self.rng.randint(0, self.num_items - 1)
                        get_review_item_by_id(self.conn, cursor, i_id)
                    elif tx_type == "get_reviews_by_user":
                        u_id = self.rng.randint(0, self.num_users - 1)
                        get_reviews_by_user(self.conn, cursor, u_id)
                    elif tx_type == "get_avg_rating_by_trusted_user":
                        i_id = self.rng.randint(0, self.num_items - 1)
                        u_id = self.rng.randint(0, self.num_users - 1)
                        get_avg_rating_by_trusted_user(self.conn, cursor, i_id, u_id)
                    elif tx_type == "get_item_avg_rating":
                        i_id = self.rng.randint(0, self.num_items - 1)
                        get_item_avg_rating(self.conn, cursor, i_id)
                    elif tx_type == "get_item_reviews_by_trusted_user":
                        i_id = self.rng.randint(0, self.num_items - 1)
                        u_id = self.rng.randint(0, self.num_users - 1)
                        get_item_reviews_by_trusted_user(self.conn, cursor, i_id, u_id)
                    elif tx_type == "update_user_name":
                        u_id = self.rng.randint(0, self.num_users - 1)
                        name = random_string(self.rng, NAME_LENGTH)
                        update_user_name(self.conn, cursor, u_id, name)
                    elif tx_type == "update_item_title":
                        i_id = self.rng.randint(0, self.num_items - 1)
                        title = random_string(self.rng, TITLE_LENGTH)
                        update_item_title(self.conn, cursor, i_id, title)
                    elif tx_type == "update_review_rating":
                        i_id = self.rng.randint(0, self.num_items - 1)
                        u_id = self.rng.randint(0, self.num_users - 1)
                        rating = self.rng.randint(0, 999)
                        update_review_rating(self.conn, cursor, i_id, u_id, rating)
                    elif tx_type == "update_trust_rating":
                        source_u_id = self.rng.randint(0, self.num_users - 1)
                        target_u_id = self.rng.randint(0, self.num_users - 1)
                        trust = self.rng.randint(0, 1)
                        update_trust_rating(
                            self.conn,
                            cursor,
                            source_u_id,
                            target_u_id,
                            trust,
                        )
                    else:
                        raise ValueError(f"unknown transaction type: {tx_type}")

                    stats[tx_type] += 1
                finally:
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
