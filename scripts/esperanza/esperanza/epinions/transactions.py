from __future__ import annotations


def get_review_item_by_id(conn, cursor, i_id: int) -> None:
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "SELECT * FROM review r, item2 i WHERE i.i_id = r.i_id and r.i_id=%s "
        "ORDER BY rating DESC, r.creation_date DESC LIMIT 10",
        (i_id,),
    )
    cursor.fetchall()
    cursor.execute("COMMIT")


def get_reviews_by_user(conn, cursor, u_id: int) -> None:
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "SELECT * FROM review r, useracct u WHERE u.u_id = r.u_id AND r.u_id=%s "
        "ORDER BY rating DESC, r.creation_date DESC LIMIT 10",
        (u_id,),
    )
    cursor.fetchall()
    cursor.execute("COMMIT")


def get_avg_rating_by_trusted_user(conn, cursor, i_id: int, u_id: int) -> None:
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "SELECT avg(rating) FROM review r, trust t WHERE r.u_id=t.target_u_id "
        "AND r.i_id=%s AND t.source_u_id=%s",
        (i_id, u_id),
    )
    cursor.fetchall()
    cursor.execute("COMMIT")


def get_item_avg_rating(conn, cursor, i_id: int) -> None:
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "SELECT avg(rating) FROM review r WHERE r.i_id=%s",
        (i_id,),
    )
    cursor.fetchall()
    cursor.execute("COMMIT")


def get_item_reviews_by_trusted_user(conn, cursor, i_id: int, u_id: int) -> None:
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "SELECT * FROM review r WHERE r.i_id=%s ORDER BY creation_date DESC",
        (i_id,),
    )
    cursor.fetchall()
    cursor.execute(
        "SELECT * FROM trust t WHERE t.source_u_id=%s",
        (u_id,),
    )
    cursor.fetchall()
    cursor.execute("COMMIT")


def update_user_name(conn, cursor, u_id: int, name: str) -> None:
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "UPDATE useracct SET name = %s WHERE u_id=%s",
        (name, u_id),
    )
    cursor.execute("COMMIT")


def update_item_title(conn, cursor, i_id: int, title: str) -> None:
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "UPDATE item2 SET title = %s WHERE i_id=%s",
        (title, i_id),
    )
    cursor.execute("COMMIT")


def update_review_rating(conn, cursor, i_id: int, u_id: int, rating: int) -> None:
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "UPDATE review SET rating = %s WHERE i_id=%s AND u_id=%s",
        (rating, i_id, u_id),
    )
    cursor.execute("COMMIT")


def update_trust_rating(
    conn,
    cursor,
    source_u_id: int,
    target_u_id: int,
    trust: int,
) -> None:
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "UPDATE trust SET trust = %s WHERE source_u_id=%s AND target_u_id=%s",
        (trust, source_u_id, target_u_id),
    )
    cursor.execute("COMMIT")
