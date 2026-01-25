from __future__ import annotations

from typing import Callable, TypeVar

from .constants import FLIGHTS_NUM_SEATS

T = TypeVar("T")


def execute_transaction(cursor, fn: Callable[[], T]) -> T:
    cursor.execute("START TRANSACTION")
    try:
        result = fn()
        cursor.execute("COMMIT")
        return result
    except Exception as e:
        cursor.execute("ROLLBACK")
        raise e


def _drain_results(cursor) -> None:
    if getattr(cursor, "with_rows", False):
        cursor.fetchall()
    nextset = getattr(cursor, "nextset", None)
    while callable(nextset) and nextset():
        if getattr(cursor, "with_rows", False):
            cursor.fetchall()


def find_flights(
    conn,
    cursor,
    depart_airport_id: int,
    arrive_airport_id: int,
    start_date,
    end_date,
    distance: float | None = None,
) -> list[tuple]:
    def _run() -> list[tuple]:
        if distance is None:
            cursor.execute(
                "SELECT f_id, f_al_id, f_seats_left, f_depart_time, f_arrive_time, f_base_price "
                "FROM flight "
                "WHERE f_depart_ap_id = %s AND f_arrive_ap_id = %s "
                "AND f_depart_time >= %s AND f_depart_time <= %s",
                (depart_airport_id, arrive_airport_id, start_date, end_date),
            )
        else:
            nearby_sql = (
                "SELECT d_ap_id1 FROM airport_distance WHERE d_ap_id0 = %s AND d_distance <= %s "
                "UNION "
                "SELECT d_ap_id0 FROM airport_distance WHERE d_ap_id1 = %s AND d_distance <= %s"
            )
            sql = (
                "SELECT f_id, f_al_id, f_seats_left, f_depart_time, f_arrive_time, f_base_price "
                "FROM flight "
                "WHERE (f_depart_ap_id = %s OR f_depart_ap_id IN ("
                + nearby_sql
                + ")) "
                "AND (f_arrive_ap_id = %s OR f_arrive_ap_id IN ("
                + nearby_sql
                + ")) "
                "AND f_depart_time >= %s AND f_depart_time <= %s"
            )
            cursor.execute(
                sql,
                (
                    depart_airport_id,
                    depart_airport_id,
                    distance,
                    depart_airport_id,
                    distance,
                    arrive_airport_id,
                    arrive_airport_id,
                    distance,
                    arrive_airport_id,
                    distance,
                    start_date,
                    end_date,
                ),
            )
        return cursor.fetchall()

    return execute_transaction(cursor, _run)


def find_open_seats(conn, cursor, flight_id: str) -> list[int]:
    def _run() -> list[int]:
        cursor.execute(
            "SELECT r_seat FROM reservation WHERE r_f_id = %s",
            (flight_id,),
        )
        reserved = {row[0] for row in cursor.fetchall()}
        return [seat for seat in range(FLIGHTS_NUM_SEATS) if seat not in reserved]

    return execute_transaction(cursor, _run)


def new_reservation(
    conn,
    cursor,
    r_id: int,
    c_id: str,
    f_id: str,
    seatnum: int,
    price: float,
    attr0: int,
    attr1: int,
    attr2: int,
    attr3: int,
    attr4: int,
    attr5: int,
    attr6: int,
    attr7: int,
    attr8: int,
) -> None:
    def _run() -> None:
        cursor.execute(
            "CALL NewReservation(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                r_id,
                c_id,
                f_id,
                seatnum,
                price,
                attr0,
                attr1,
                attr2,
                attr3,
                attr4,
                attr5,
                attr6,
                attr7,
                attr8,
            ),
        )
        _drain_results(cursor)

    execute_transaction(cursor, _run)


def update_reservation(
    conn,
    cursor,
    r_id: int,
    f_id: str,
    c_id: str,
    seatnum: int,
    attr_idx: int,
    attr_val: int,
) -> None:
    def _run() -> None:
        cursor.execute(
            "CALL UpdateReservation(%s, %s, %s, %s, %s, %s)",
            (r_id, f_id, c_id, seatnum, attr_idx, attr_val),
        )
        _drain_results(cursor)

    execute_transaction(cursor, _run)


def update_customer(
    conn,
    cursor,
    c_id: str | None,
    c_id_str: str | None,
    attr0: int,
    attr1: int,
) -> None:
    def _run() -> None:
        cursor.execute(
            "CALL UpdateCustomer(%s, %s, %s, %s)",
            (c_id, c_id_str, attr0, attr1),
        )
        _drain_results(cursor)

    execute_transaction(cursor, _run)


def delete_reservation(
    conn,
    cursor,
    f_id: str,
    c_id: str | None,
    c_id_str: str | None,
    ff_c_id_str: str | None,
    ff_al_id: int | None,
) -> None:
    def _run() -> None:
        cursor.execute(
            "CALL DeleteReservation(%s, %s, %s, %s, %s)",
            (f_id, c_id, c_id_str, ff_c_id_str, ff_al_id),
        )
        _drain_results(cursor)

    execute_transaction(cursor, _run)
