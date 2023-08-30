CREATE PROCEDURE UpdateReservation(IN var_r_id BIGINT,
                                   IN var_f_id VARCHAR(128),
                                   IN var_c_id VARCHAR(128),
                                   IN var_seatnum BIGINT,
                                   IN var_attr_idx BIGINT,
                                   IN var_attr_val BIGINT)
UpdateReservation_Label:BEGIN

DECLARE var_check_r_id BIGINT DEFAULT -1;

SELECT R_ID INTO var_check_r_id FROM reservation WHERE R_F_ID = var_f_id AND R_SEAT = var_seatnum;

IF var_check_r_id != -1 THEN
  SELECT "Error: The seat is already taken by another customer";
  LEAVE UpdateReservation_Label;
END IF;

SELECT R_ID INTO var_check_r_id FROM reservation
  WHERE R_C_ID = var_c_id AND R_F_ID = var_f_id;

IF var_check_r_id = -1 THEN
  SELECT "Error: the customer does not have a seat on this flight";
  LEAVE UpdateReservation_Label;
END IF;

IF var_attr_idx = 0 THEN
  UPDATE reservation SET R_SEAT = var_seatnum, R_IATTR00 = var_attr_val
  WHERE R_ID = var_r_id AND R_C_ID = var_c_id AND R_F_ID = var_f_id;
ELSEIF var_attr_idx = 1 THEN
  UPDATE reservation SET R_SEAT = var_seatnum, R_IATTR01 = var_attr_val
  WHERE R_ID = var_r_id AND R_C_ID = var_c_id AND R_F_ID = var_f_id;
ELSEIF var_attr_idx = 2 THEN
  UPDATE reservation SET R_SEAT = var_seatnum, R_IATTR02 = var_attr_val
  WHERE R_ID = var_r_id AND R_C_ID = var_c_id AND R_F_ID = var_f_id;
ELSE
  UPDATE reservation SET R_SEAT = var_seatnum, R_IATTR03 = var_attr_val
  WHERE R_ID = var_r_id AND R_C_ID = var_c_id AND R_F_ID = var_f_id;
END IF;

END
