from __future__ import annotations

from .constants import FLIGHTS_NUM_SEATS


def _drain_stored_results(cursor) -> None:
    """Drain all result sets from stored procedure calls."""
    stored_results = getattr(cursor, "stored_results", None)
    if callable(stored_results):
        for result in stored_results():
            try:
                result.fetchall()
            except Exception:
                pass


def find_flights(
    conn,
    cursor,
    depart_airport_id: int,
    arrive_airport_id: int,
    start_date,
    end_date,
    distance: float | None = None,
) -> list[tuple]:
    try:
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
        result = cursor.fetchall()
        conn.commit()
        return result
    except Exception as e:
        conn.rollback()
        raise e


def find_open_seats(conn, cursor, flight_id: str) -> list[int]:
    try:
        cursor.execute(
            "SELECT r_seat FROM reservation WHERE r_f_id = %s",
            (flight_id,),
        )
        reserved = {row[0] for row in cursor.fetchall()}
        conn.commit()
        return [seat for seat in range(FLIGHTS_NUM_SEATS) if seat not in reserved]
    except Exception as e:
        conn.rollback()
        raise e


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
    try:
        cursor.callproc(
            "NewReservation",
            (r_id, c_id, f_id, seatnum, price, attr0, attr1, attr2, attr3, attr4, attr5, attr6, attr7, attr8),
        )
        _drain_stored_results(cursor)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


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
    try:
        cursor.callproc(
            "UpdateReservation",
            (r_id, f_id, c_id, seatnum, attr_idx, attr_val),
        )
        _drain_stored_results(cursor)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def update_customer(
    conn,
    cursor,
    c_id: str | None,
    c_id_str: str | None,
    attr0: int,
    attr1: int,
) -> None:
    try:
        cursor.callproc(
            "UpdateCustomer",
            (c_id, c_id_str, attr0, attr1),
        )
        _drain_stored_results(cursor)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def delete_reservation(
    conn,
    cursor,
    f_id: str,
    c_id: str | None,
    c_id_str: str | None,
    ff_c_id_str: str | None,
    ff_al_id: int | None,
) -> None:
    try:
        cursor.callproc(
            "DeleteReservation",
            (f_id, c_id, c_id_str, ff_c_id_str, ff_al_id),
        )
        _drain_stored_results(cursor)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
