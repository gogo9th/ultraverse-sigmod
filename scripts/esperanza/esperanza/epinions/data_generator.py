from __future__ import annotations

import random
import sys
from datetime import datetime
from typing import Iterable

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional dependency for progress UI
    tqdm = None

from .constants import (
    COMMENT_LENGTH,
    COMMENT_MIN_LENGTH,
    DESCRIPTION_LENGTH,
    NAME_LENGTH,
    NUM_ITEMS,
    NUM_REVIEWS,
    NUM_TRUST,
    NUM_USERS,
    TITLE_LENGTH,
)
from .random_utils import (
    ScrambledZipfianGenerator,
    ZipfianGenerator,
    random_email,
    random_string,
)


class EpinionsDataGenerator:
    def __init__(self, conn, num_users: int = NUM_USERS, num_items: int = NUM_ITEMS) -> None:
        self.conn = conn
        self.num_users = num_users
        self.num_items = num_items
        self.batch_size = 1000
        self.rng = random.Random()

    def generate_all(self) -> None:
        users_bar = self._new_progress(self.num_users, "users")
        items_bar = self._new_progress(self.num_items, "items")
        reviews_bar = self._new_progress(None, "reviews")
        trusts_bar = self._new_progress(None, "trusts")

        try:
            self.generate_users(users_bar)
            self.generate_items(items_bar)
            self.generate_reviews(reviews_bar)
            self.generate_trusts(trusts_bar)
        finally:
            self._close_progress(users_bar)
            self._close_progress(items_bar)
            self._close_progress(reviews_bar)
            self._close_progress(trusts_bar)

    def generate_users(self, progress: object | None = None) -> None:
        sql = (
            "INSERT INTO useracct (u_id, name, email, creation_date) "
            "VALUES (%s, %s, %s, %s)"
        )

        def rows() -> Iterable[tuple]:
            for u_id in range(self.num_users):
                yield (
                    u_id,
                    random_string(self.rng, NAME_LENGTH),
                    random_email(self.rng),
                    datetime.now(),
                )

        self._execute_batches(sql, rows(), progress=progress)

    def generate_items(self, progress: object | None = None) -> None:
        sql = (
            "INSERT INTO item2 (i_id, title, description, creation_date) "
            "VALUES (%s, %s, %s, %s)"
        )
        description_length = ZipfianGenerator(self.rng, DESCRIPTION_LENGTH)

        def rows() -> Iterable[tuple]:
            for i_id in range(self.num_items):
                desc_len = description_length.nextInt() + 1
                yield (
                    i_id,
                    random_string(self.rng, TITLE_LENGTH),
                    random_string(self.rng, desc_len),
                    datetime.now(),
                )

        self._execute_batches(sql, rows(), progress=progress)

    def generate_reviews(self, progress: object | None = None) -> None:
        sql = (
            "INSERT INTO review (a_id, u_id, i_id, rating, `rank`, comment, creation_date) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        )
        num_reviews = ZipfianGenerator(self.rng, NUM_REVIEWS, zipfian_constant=1.8)
        reviewer = ZipfianGenerator(self.rng, self.num_users)
        comment_length = ZipfianGenerator(self.rng, COMMENT_LENGTH - COMMENT_MIN_LENGTH)

        def rows() -> Iterable[tuple]:
            review_id = 0
            for i_id in range(self.num_items):
                review_count = num_reviews.nextInt() + 1
                reviewers: set[int] = set()
                now = datetime.now()
                for _ in range(review_count):
                    u_id = reviewer.nextInt()
                    while u_id in reviewers:
                        u_id = reviewer.nextInt()
                    reviewers.add(u_id)
                    comment = random_string(
                        self.rng,
                        comment_length.nextInt() + COMMENT_MIN_LENGTH,
                    )
                    yield (
                        review_id,
                        u_id,
                        i_id,
                        self.rng.randint(0, 4),
                        None,
                        comment,
                        now,
                    )
                    review_id += 1

        self._execute_batches(sql, rows(), progress=progress)

    def generate_trusts(self, progress: object | None = None) -> None:
        sql = (
            "INSERT INTO trust (source_u_id, target_u_id, trust, creation_date) "
            "VALUES (%s, %s, %s, %s)"
        )
        num_trust = ZipfianGenerator(self.rng, NUM_TRUST, zipfian_constant=1.95)
        trusted = ScrambledZipfianGenerator(self.rng, self.num_users)

        def rows() -> Iterable[tuple]:
            for source_id in range(self.num_users):
                trust_count = num_trust.nextInt()
                trusted_users: set[int] = set()
                now = datetime.now()
                for _ in range(trust_count):
                    target_id = trusted.nextInt()
                    while target_id in trusted_users:
                        target_id = trusted.nextInt()
                    trusted_users.add(target_id)
                    yield (
                        source_id,
                        target_id,
                        self.rng.randint(0, 1),
                        now,
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
