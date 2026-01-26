from __future__ import annotations

import random

from .constants import START_TIME_SLOTS


def _drain_stored_results(cursor) -> None:
    """Drain all result sets from stored procedure calls."""
    stored_results = getattr(cursor, "stored_results", None)
    if callable(stored_results):
        for result in stored_results():
            try:
                result.fetchall()
            except Exception:
                pass


def get_access_data(conn, cursor, s_id: int, ai_type: int) -> None:
    try:
        cursor.execute(
            "SELECT data1, data2, data3, data4 FROM access_info WHERE s_id = %s AND ai_type = %s",
            (s_id, ai_type),
        )
        cursor.fetchall()
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def get_subscriber_data(conn, cursor, s_id: int) -> None:
    try:
        cursor.execute(
            "SELECT sub_nbr, "
            "bit_1, bit_2, bit_3, bit_4, bit_5, bit_6, bit_7, bit_8, bit_9, bit_10, "
            "hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, hex_7, hex_8, hex_9, hex_10, "
            "byte2_1, byte2_2, byte2_3, byte2_4, byte2_5, byte2_6, byte2_7, byte2_8, "
            "byte2_9, byte2_10, msc_location, vlr_location "
            "FROM subscriber WHERE s_id = %s",
            (s_id,),
        )
        cursor.fetchall()
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def get_new_destination(
    conn,
    cursor,
    s_id: int,
    sf_type: int,
    start_time: int,
    end_time: int,
) -> None:
    try:
        cursor.execute(
            "SELECT cf.numberx "
            "FROM special_facility sf, call_forwarding cf "
            "WHERE sf.s_id = %s AND sf.sf_type = %s AND sf.is_active = 1 "
            "AND cf.s_id = sf.s_id AND cf.sf_type = sf.sf_type "
            "AND cf.start_time <= %s AND cf.end_time > %s",
            (s_id, sf_type, start_time, end_time),
        )
        cursor.fetchall()
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def update_location(conn, sub_nbr: str, vlr_location: int) -> None:
    cursor = conn.cursor()
    try:
        cursor.callproc("UpdateLocation", (vlr_location, sub_nbr))
        _drain_stored_results(cursor)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()


def delete_call_forwarding(conn, sub_nbr: str, sf_type: int, start_time: int) -> None:
    cursor = conn.cursor()
    try:
        cursor.callproc("DeleteCallForwarding", (sub_nbr, sf_type, start_time))
        _drain_stored_results(cursor)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()


def insert_call_forwarding(
    conn,
    sub_nbr: str,
    sf_type: int,
    start_time: int,
    end_time: int,
    numberx: str,
) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT s_id FROM subscriber WHERE sub_nbr = %s", (sub_nbr,))
        row = cursor.fetchone()
        if not row:
            conn.commit()
            return
        s_id = row[0]

        cursor.execute(
            "SELECT sf_type FROM special_facility WHERE s_id = %s",
            (s_id,),
        )
        sf_types = [r[0] for r in cursor.fetchall()]
        if not sf_types:
            conn.commit()
            return
        chosen_sf = sf_type if sf_type in sf_types else random.choice(sf_types)

        cursor.execute(
            "SELECT start_time FROM call_forwarding WHERE s_id = %s AND sf_type = %s",
            (s_id, chosen_sf),
        )
        used_times = {r[0] for r in cursor.fetchall()}
        available_times = [t for t in START_TIME_SLOTS if t not in used_times]
        if not available_times:
            conn.commit()
            return

        chosen_start = start_time if start_time in available_times else random.choice(available_times)
        duration = max(1, end_time - start_time)
        chosen_end = chosen_start + duration

        cursor.callproc("InsertCallForwarding", (sub_nbr, chosen_sf, chosen_start, chosen_end, numberx))
        _drain_stored_results(cursor)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()


def update_subscriber_data(
    conn,
    s_id: int,
    bit_1: int,
    data_a: int,
    sf_type: int,
) -> None:
    cursor = conn.cursor()
    try:
        cursor.callproc("UpdateSubscriberData", (s_id, bit_1, data_a, sf_type))
        _drain_stored_results(cursor)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
