CREATE PROCEDURE NewReservation(IN var_r_id BIGINT, 
                                IN var_c_id VARCHAR(128), 
                                IN var_f_id VARCHAR(128), 
                                IN var_seatnum BIGINT,
                                IN var_price DECIMAL(7,3),
                                IN var_attr0 BIGINT,
                                IN var_attr1 BIGINT,
                                IN var_attr2 BIGINT,
                                IN var_attr3 BIGINT,
                                IN var_attr4 BIGINT,
                                IN var_attr5 BIGINT,
                                IN var_attr6 BIGINT,
                                IN var_attr7 BIGINT,
                                IN var_attr8 BIGINT
                                )
NewReservation_Label:BEGIN

DECLARE __ultraverse_callinfo VARCHAR(512) DEFAULT JSON_ARRAY(
    UUID_SHORT(), 'NewReservation',
    var_r_id, var_c_id, var_f_id, var_seatnum, var_price,
    var_attr0, var_attr1, var_attr2, var_attr3, var_attr4,
    var_attr5, var_attr6, var_attr7, var_attr8
);

DECLARE var_airline_id BIGINT;
DECLARE var_seats_left INT DEFAULT -1;
DECLARE var_found BIGINT DEFAULT -1;
DECLARE var_taken_seat_r_id BIGINT DEFAULT -1;

INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callinfo) VALUES (__ultraverse_callinfo);

SELECT F_AL_ID, F_SEATS_LEFT INTO var_airline_id, var_seats_left
  FROM flight, airline
  WHERE F_ID = var_f_id AND F_AL_ID = AL_ID;

IF var_seats_left = -1 THEN
  SELECT "Error: no seats available";
  LEAVE NewReservation_Label;
END IF;

SELECT R_ID INTO var_taken_seat_r_id FROM reservation
  WHERE R_F_ID = var_f_id AND R_SEAT = var_seatnum;

IF var_taken_seat_r_id != -1 THEN
  SELECT "Error: this seat is already taken by another customer";
  LEAVE NewReservation_Label;
END IF;

SELECT R_ID INTO var_found FROM reservation
  WHERE R_C_ID = var_c_id AND R_F_ID = var_f_id;

IF var_found != -1 THEN
  SELECT "Error: this customer already has a seat on this flight";
  LEAVE NewReservation_Label;
END IF;

SELECT C_BASE_AP_ID INTO var_found
  FROM customer2
  WHERE C_ID = var_c_id;

IF var_found = -1 THEN
  SELECT "Error: customer not found";
  LEAVE NewReservation_Label;
END IF;

INSERT INTO reservation
  (R_ID, R_C_ID, R_F_ID, R_SEAT, R_PRICE, R_IATTR00, R_IATTR01, R_IATTR02, R_IATTR03, R_IATTR04, R_IATTR05, R_IATTR06, R_IATTR07, R_IATTR08)
  VALUES (var_r_id, var_c_id, var_f_id, var_seatnum, var_price, var_attr0, var_attr1, var_attr2, var_attr3, var_attr4, var_attr5, var_attr6, var_attr7, var_attr8);

UPDATE flight SET F_SEATS_LEFT = F_SEATS_LEFT - 1 WHERE F_ID = var_f_id;

UPDATE customer2
  SET C_IATTR10 = C_IATTR10 + 1, C_IATTR11 = C_IATTR11 + 1, C_IATTR12 = var_attr0, C_IATTR13 = var_attr1, C_IATTR14 = var_attr2, C_IATTR15 = var_attr3
  WHERE C_ID = var_c_id;

UPDATE frequent_flyer
  SET FF_IATTR10 = FF_IATTR10 + 1, FF_IATTR11 = var_attr4, FF_IATTR12 = var_attr5, FF_IATTR13 = var_attr6, FF_IATTR14 = var_attr7
  WHERE FF_C_ID = var_c_id AND FF_AL_ID = var_airline_id;

END
