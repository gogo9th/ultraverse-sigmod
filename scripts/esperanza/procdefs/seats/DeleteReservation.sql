CREATE PROCEDURE DeleteReservation(IN var_f_id VARCHAR(128),
                                   IN var_c_id VARCHAR(128),
                                   IN var_c_id_str VARCHAR(64),
                                   IN var_ff_c_id_str VARCHAR(64),
                                   IN var_ff_al_id BIGINT)
DeleteReservation_Label:BEGIN

DECLARE __ultraverse_callinfo VARCHAR(512) DEFAULT JSON_ARRAY(
    UUID_SHORT(), 'DeleteReservation',
    var_f_id, var_c_id, var_c_id_str, var_ff_c_id_str,
    var_ff_al_id
);

DECLARE var_c_iattr00 BIGINT;
DECLARE var_r_id BIGINT;
DECLARE var_r_price DECIMAL(7,3);

INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callinfo) VALUES (__ultraverse_callinfo);

IF (var_c_id_str IS NOT NULL AND LENGTH(var_c_id_str) > 0) THEN
  SELECT C_ID INTO var_c_id FROM customer2 WHERE C_ID_STR = var_c_id_str;
ELSE
  SELECT C_ID, FF_AL_ID INTO var_c_id, var_ff_al_id FROM customer2, frequent_flyer WHERE FF_C_ID_STR = var_ff_c_id_str AND FF_C_ID = C_ID LIMIT 1;
END IF;

IF var_c_id IS NULL OR LENGTH(var_c_id) = 0 THEN
  SELECT "Error: no customer record was found";
  LEAVE DeleteReservation_Label;
END IF;

SELECT C_IATTR00, R_ID, R_PRICE INTO var_c_iattr00, var_r_id, var_r_price
  FROM customer2, flight, reservation
  WHERE C_ID = var_c_id AND C_ID = R_C_ID AND F_ID = var_f_id AND F_ID = R_F_ID;

IF var_r_price IS NULL THEN
  SELECT "Error: no reservation is found";
  LEAVE DeleteReservation_Label;
END IF;

DELETE FROM reservation WHERE R_ID = var_r_id AND R_C_ID = var_c_id AND R_F_ID = var_f_id;

UPDATE flight SET F_SEATS_LEFT = F_SEATS_LEFT + 1 WHERE F_ID = var_f_id;

UPDATE customer2
  SET C_BALANCE = C_BALANCE - var_r_price, C_IATTR00 = var_c_iattr00, C_IATTR10 = C_IATTR10 - 1, C_IATTR11 = C_IATTR10 - 1
  WHERE C_ID = var_c_id;

UPDATE frequent_flyer
  SET FF_IATTR10 = FF_IATTR10 - 1 WHERE FF_C_ID = var_c_id AND FF_AL_ID = var_ff_al_id;

END
