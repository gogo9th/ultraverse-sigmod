from __future__ import annotations


def execute_procedure(conn, proc_call: str, params: tuple) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute("START TRANSACTION")
        cursor.execute(proc_call, params)
        # Drain all result sets to avoid "Unread result found" in mysql-connector.
        while True:
            if cursor.with_rows:
                cursor.fetchall()
            if not cursor.nextset():
                break
        cursor.execute("COMMIT")
    except Exception as e:
        cursor.execute("ROLLBACK")
        raise e
    finally:
        cursor.close()


def get_access_data(conn, cursor, s_id: int, ai_type: int) -> None:
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "SELECT data1, data2, data3, data4 FROM access_info WHERE s_id = %s AND ai_type = %s",
        (s_id, ai_type),
    )
    cursor.fetchall()
    cursor.execute("COMMIT")


def get_subscriber_data(conn, cursor, s_id: int) -> None:
    cursor.execute("START TRANSACTION")
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
    cursor.execute("COMMIT")


def get_new_destination(
    conn,
    cursor,
    s_id: int,
    sf_type: int,
    start_time: int,
    end_time: int,
) -> None:
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "SELECT cf.numberx "
        "FROM special_facility sf, call_forwarding cf "
        "WHERE sf.s_id = %s AND sf.sf_type = %s AND sf.is_active = 1 "
        "AND cf.s_id = sf.s_id AND cf.sf_type = sf.sf_type "
        "AND cf.start_time <= %s AND cf.end_time > %s",
        (s_id, sf_type, start_time, end_time),
    )
    cursor.fetchall()
    cursor.execute("COMMIT")


def update_location(conn, sub_nbr: str, vlr_location: int) -> None:
    execute_procedure(
        conn,
        "CALL UpdateLocation(%s, %s)",
        (vlr_location, sub_nbr),
    )


def delete_call_forwarding(conn, sub_nbr: str, sf_type: int, start_time: int) -> None:
    execute_procedure(
        conn,
        "CALL DeleteCallForwarding(%s, %s, %s)",
        (sub_nbr, sf_type, start_time),
    )


def insert_call_forwarding(
    conn,
    sub_nbr: str,
    sf_type: int,
    start_time: int,
    end_time: int,
    numberx: str,
) -> None:
    execute_procedure(
        conn,
        "CALL InsertCallForwarding(%s, %s, %s, %s, %s)",
        (sub_nbr, sf_type, start_time, end_time, numberx),
    )


def update_subscriber_data(
    conn,
    s_id: int,
    bit_1: int,
    data_a: int,
    sf_type: int,
) -> None:
    execute_procedure(
        conn,
        "CALL UpdateSubscriberData(%s, %s, %s, %s)",
        (s_id, bit_1, data_a, sf_type),
    )
