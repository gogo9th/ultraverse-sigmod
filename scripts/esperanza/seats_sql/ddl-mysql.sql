
SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

-- Drop Tables
DROP TABLE IF EXISTS config_profile CASCADE;
DROP TABLE IF EXISTS config_histograms CASCADE;
DROP TABLE IF EXISTS reservation CASCADE;
DROP TABLE IF EXISTS frequent_flyer CASCADE;
DROP TABLE IF EXISTS customer2 CASCADE;
DROP TABLE IF EXISTS flight CASCADE;
DROP TABLE IF EXISTS airport_distance CASCADE;
DROP TABLE IF EXISTS airport CASCADE;
DROP TABLE IF EXISTS airline CASCADE;
DROP TABLE IF EXISTS country CASCADE;

-- Ultraverse Procedure Hint Table
-- This BLACKHOLE table captures procedure call information for retroactive operation tracking

CREATE TABLE IF NOT EXISTS __ULTRAVERSE_PROCEDURE_HINT (
    callid BIGINT UNSIGNED NOT NULL,
    procname VARCHAR(255) NOT NULL,
    args VARCHAR(4096),
    vars VARCHAR(4096),
    PRIMARY KEY (callid)
) ENGINE = BLACKHOLE;


-- 
-- CONFIG_PROFILE
--
CREATE TABLE config_profile (
    cfp_scale_factor        float                               NOT NULL,
    cfp_aiport_max_customer longtext                            NOT NULL,
    cfp_flight_start        timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    cfp_flight_upcoming     timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    cfp_flight_past_days    int                                 NOT NULL,
    cfp_flight_future_days  int                                 NOT NULL,
    cfp_flight_offset       int,
    cfp_reservation_offset  int,
    cfp_num_reservations    bigint                              NOT NULL,
    cfp_code_ids_xrefs      longtext                            NOT NULL
)  ;

--
-- CONFIG_HISTOGRAMS
--
CREATE TABLE config_histograms (
    cfh_name       varchar(128)   NOT NULL,
    cfh_data       varchar(10005) NOT NULL,
    cfh_is_airport tinyint DEFAULT 0,
    PRIMARY KEY (cfh_name)
)  ;

-- 
-- COUNTRY
--
CREATE TABLE country (
    co_id     bigint      NOT NULL,
    co_name   varchar(64) NOT NULL,
    co_code_2 varchar(2)  NOT NULL,
    co_code_3 varchar(3)  NOT NULL,
    PRIMARY KEY (co_id)
)  ;

--
-- AIRPORT
--
CREATE TABLE airport (
    ap_id          bigint       NOT NULL,
    ap_code        varchar(3)   NOT NULL,
    ap_name        varchar(128) NOT NULL,
    ap_city        varchar(64)  NOT NULL,
    ap_postal_code varchar(12),
    ap_co_id       bigint       NOT NULL,
    ap_longitude   decimal(10,7),
    ap_latitude    decimal(10,7),
    ap_gmt_offset  decimal(6,2),
    ap_wac         bigint,
    ap_iattr00     bigint,
    ap_iattr01     bigint,
    ap_iattr02     bigint,
    ap_iattr03     bigint,
    ap_iattr04     bigint,
    ap_iattr05     bigint,
    ap_iattr06     bigint,
    ap_iattr07     bigint,
    ap_iattr08     bigint,
    ap_iattr09     bigint,
    ap_iattr10     bigint,
    ap_iattr11     bigint,
    ap_iattr12     bigint,
    ap_iattr13     bigint,
    ap_iattr14     bigint,
    ap_iattr15     bigint,
    PRIMARY KEY (ap_id),
    FOREIGN KEY (ap_co_id) REFERENCES country (co_id)
)  ;

--
-- AIRPORT_DISTANCE
--
CREATE TABLE airport_distance (
    d_ap_id0   bigint NOT NULL,
    d_ap_id1   bigint NOT NULL,
    d_distance decimal(9,6)  NOT NULL,
    PRIMARY KEY (d_ap_id0, d_ap_id1),
    FOREIGN KEY (d_ap_id0) REFERENCES airport (ap_id),
    FOREIGN KEY (d_ap_id1) REFERENCES airport (ap_id)
)  ;

--
-- AIRLINE
--
CREATE TABLE airline (
    al_id        bigint       NOT NULL,
    al_iata_code varchar(3),
    al_icao_code varchar(3),
    al_call_sign varchar(32),
    al_name      varchar(128) NOT NULL,
    al_co_id     bigint       NOT NULL,
    al_iattr00   bigint,
    al_iattr01   bigint,
    al_iattr02   bigint,
    al_iattr03   bigint,
    al_iattr04   bigint,
    al_iattr05   bigint,
    al_iattr06   bigint,
    al_iattr07   bigint,
    al_iattr08   bigint,
    al_iattr09   bigint,
    al_iattr10   bigint,
    al_iattr11   bigint,
    al_iattr12   bigint,
    al_iattr13   bigint,
    al_iattr14   bigint,
    al_iattr15   bigint,
    PRIMARY KEY (al_id),
    FOREIGN KEY (al_co_id) REFERENCES country (co_id)
)  ;

--
-- CUSTOMER
--
CREATE TABLE customer2 (
    c_id         varchar(128)             NOT NULL,
    c_id_str     varchar(64) UNIQUE NOT NULL,
    c_base_ap_id bigint,
    c_balance    decimal(12,7)        NOT NULL,
    c_sattr00    varchar(32),
    c_sattr01    varchar(8),
    c_sattr02    varchar(8),
    c_sattr03    varchar(8),
    c_sattr04    varchar(8),
    c_sattr05    varchar(8),
    c_sattr06    varchar(8),
    c_sattr07    varchar(8),
    c_sattr08    varchar(8),
    c_sattr09    varchar(8),
    c_sattr10    varchar(8),
    c_sattr11    varchar(8),
    c_sattr12    varchar(8),
    c_sattr13    varchar(8),
    c_sattr14    varchar(8),
    c_sattr15    varchar(8),
    c_sattr16    varchar(8),
    c_sattr17    varchar(8),
    c_sattr18    varchar(8),
    c_sattr19    varchar(8),
    c_iattr00    bigint,
    c_iattr01    bigint,
    c_iattr02    bigint,
    c_iattr03    bigint,
    c_iattr04    bigint,
    c_iattr05    bigint,
    c_iattr06    bigint,
    c_iattr07    bigint,
    c_iattr08    bigint,
    c_iattr09    bigint,
    c_iattr10    bigint,
    c_iattr11    bigint,
    c_iattr12    bigint,
    c_iattr13    bigint,
    c_iattr14    bigint,
    c_iattr15    bigint,
    c_iattr16    bigint,
    c_iattr17    bigint,
    c_iattr18    bigint,
    c_iattr19    bigint,
    PRIMARY KEY (c_id),
    FOREIGN KEY (c_base_ap_id) REFERENCES airport (ap_id)
)  ;

--
-- FREQUENT_FLYER
--
CREATE TABLE frequent_flyer (
    ff_c_id     varchar(128)      NOT NULL,
    ff_al_id    bigint      NOT NULL,
    ff_c_id_str varchar(64) NOT NULL,
    ff_sattr00  varchar(32),
    ff_sattr01  varchar(32),
    ff_sattr02  varchar(32),
    ff_sattr03  varchar(32),
    ff_iattr00  bigint,
    ff_iattr01  bigint,
    ff_iattr02  bigint,
    ff_iattr03  bigint,
    ff_iattr04  bigint,
    ff_iattr05  bigint,
    ff_iattr06  bigint,
    ff_iattr07  bigint,
    ff_iattr08  bigint,
    ff_iattr09  bigint,
    ff_iattr10  bigint,
    ff_iattr11  bigint,
    ff_iattr12  bigint,
    ff_iattr13  bigint,
    ff_iattr14  bigint,
    ff_iattr15  bigint,
    PRIMARY KEY (ff_c_id, ff_al_id),
    FOREIGN KEY (ff_c_id) REFERENCES customer2 (c_id),
    FOREIGN KEY (ff_al_id) REFERENCES airline (al_id)
)  ;
CREATE INDEX idx_ff_customer_id ON frequent_flyer (ff_c_id_str);

--
-- FLIGHT
--
CREATE TABLE flight (
    f_id           varchar(128)                              NOT NULL,
    f_al_id        bigint                              NOT NULL,
    f_depart_ap_id bigint                              NOT NULL,
    f_depart_time  timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    f_arrive_ap_id bigint                              NOT NULL,
    f_arrive_time  timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    f_status       bigint                              NOT NULL,
    f_base_price   decimal(7,3)                        NOT NULL,
    f_seats_total  bigint                              NOT NULL,
    f_seats_left   bigint                              NOT NULL,
    f_iattr00      bigint,
    f_iattr01      bigint,
    f_iattr02      bigint,
    f_iattr03      bigint,
    f_iattr04      bigint,
    f_iattr05      bigint,
    f_iattr06      bigint,
    f_iattr07      bigint,
    f_iattr08      bigint,
    f_iattr09      bigint,
    f_iattr10      bigint,
    f_iattr11      bigint,
    f_iattr12      bigint,
    f_iattr13      bigint,
    f_iattr14      bigint,
    f_iattr15      bigint,
    f_iattr16      bigint,
    f_iattr17      bigint,
    f_iattr18      bigint,
    f_iattr19      bigint,
    f_iattr20      bigint,
    f_iattr21      bigint,
    f_iattr22      bigint,
    f_iattr23      bigint,
    f_iattr24      bigint,
    f_iattr25      bigint,
    f_iattr26      bigint,
    f_iattr27      bigint,
    f_iattr28      bigint,
    f_iattr29      bigint,
    PRIMARY KEY (f_id),
    FOREIGN KEY (f_al_id) REFERENCES airline (al_id),
    FOREIGN KEY (f_depart_ap_id) REFERENCES airport (ap_id),
    FOREIGN KEY (f_arrive_ap_id) REFERENCES airport (ap_id)
)  ;
CREATE INDEX f_depart_time_idx ON flight (f_depart_time);

--
-- RESERVATION
--
CREATE TABLE reservation (
    r_id      bigint NOT NULL,
    r_c_id    varchar(128) NOT NULL,
    r_f_id    varchar(128) NOT NULL,
    r_seat    bigint NOT NULL,
    r_price   decimal(7,3)  NOT NULL,
    r_iattr00 bigint,
    r_iattr01 bigint,
    r_iattr02 bigint,
    r_iattr03 bigint,
    r_iattr04 bigint,
    r_iattr05 bigint,
    r_iattr06 bigint,
    r_iattr07 bigint,
    r_iattr08 bigint,
    UNIQUE (r_f_id, r_seat),
    PRIMARY KEY (r_id, r_c_id, r_f_id),
    FOREIGN KEY (r_c_id) REFERENCES customer2 (c_id),
    FOREIGN KEY (r_f_id) REFERENCES flight (f_id)
)  ;

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;


-- UpdateCustomer transaction (procedure)
DROP PROCEDURE IF EXISTS UpdateCustomer;
DELIMITER //
CREATE PROCEDURE UpdateCustomer(IN var_c_id VARCHAR(128), 
                                IN var_c_id_str VARCHAR(64), 
                                IN var_attr0 BIGINT, 
                                IN var_attr1 BIGINT)
UpdateCustomer_Label:BEGIN

   DECLARE var_c_base_ap_id BIGINT DEFAULT -1; 

   IF var_c_id IS NULL THEN
      SELECT c_id INTO var_c_id FROM customer2 WHERE c_id_str = var_c_id_str;
   END IF;

   IF var_c_id IS NULL THEN
      SELECT "c_id is not found";
      INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), 'UpdateCustomer', JSON_OBJECT('var_c_id', var_c_id, 'var_c_id_str', var_c_id_str, 'var_attr0', var_attr0, 'var_attr1', var_attr1), JSON_OBJECT('var_c_base_ap_id', var_c_base_ap_id));
      LEAVE UpdateCustomer_Label;      
   END IF;

   SELECT  c_base_ap_id INTO var_c_base_ap_id FROM customer2 WHERE c_id = var_c_id;

   IF var_c_base_ap_id = -1 THEN
      SELECT "c_base_ap_id is not found";
      INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), 'UpdateCustomer', JSON_OBJECT('var_c_id', var_c_id, 'var_c_id_str', var_c_id_str, 'var_attr0', var_attr0, 'var_attr1', var_attr1), JSON_OBJECT('var_c_base_ap_id', var_c_base_ap_id));
      LEAVE UpdateCustomer_Label;      
   END IF;

   SELECT * FROM airport, country WHERE ap_id = var_c_base_ap_id AND ap_co_id = co_id;

   UPDATE customer2 SET c_iattr00 = var_attr0, c_iattr01 = var_attr1 WHERE c_id = var_c_id;

   UPDATE frequent_flyer SET ff_iattr00 = var_attr0, ff_iattr01 = var_attr1 WHERE ff_c_id = var_c_id AND ff_al_id IN (SELECT ff_al_id FROM (SELECT ff_al_id FROM frequent_flyer WHERE ff_c_id = var_c_id) AS frequent_flyer_alias);

   INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), 'UpdateCustomer', JSON_OBJECT('var_c_id', var_c_id, 'var_c_id_str', var_c_id_str, 'var_attr0', var_attr0, 'var_attr1', var_attr1), JSON_OBJECT('var_c_base_ap_id', var_c_base_ap_id));
END//
DELIMITER ;



-- NewReservation transaction (procedure)
DROP PROCEDURE IF EXISTS NewReservation;
DELIMITER //
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

DECLARE var_airline_id BIGINT;
DECLARE var_seats_left INT DEFAULT -1;
DECLARE var_found BIGINT DEFAULT -1;
DECLARE var_taken_seat_r_id BIGINT DEFAULT -1;

SELECT  F_AL_ID, F_SEATS_LEFT INTO var_airline_id, var_seats_left
  FROM flight, airline
  WHERE F_ID = var_f_id AND F_AL_ID = AL_ID;

IF var_seats_left = -1 THEN
  SELECT "Error: no seats available";
  INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), 'NewReservation', JSON_OBJECT('var_r_id', var_r_id, 'var_c_id', var_c_id, 'var_f_id', var_f_id, 'var_seatnum', var_seatnum, 'var_price', var_price, 'var_attr0', var_attr0, 'var_attr1', var_attr1, 'var_attr2', var_attr2, 'var_attr3', var_attr3, 'var_attr4', var_attr4, 'var_attr5', var_attr5, 'var_attr6', var_attr6, 'var_attr7', var_attr7, 'var_attr8', var_attr8), JSON_OBJECT('var_found', var_found, 'var_taken_seat_r_id', var_taken_seat_r_id, 'var_airline_id', var_airline_id, 'var_seats_left', var_seats_left));
  LEAVE NewReservation_Label;
END IF;

SELECT  R_ID INTO var_taken_seat_r_id FROM reservation 
  WHERE R_F_ID = var_f_id AND R_SEAT = var_seatnum;

IF var_taken_seat_r_id != -1 THEN
  SELECT "Error: this seat is already taken by another customer";
  INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), 'NewReservation', JSON_OBJECT('var_r_id', var_r_id, 'var_c_id', var_c_id, 'var_f_id', var_f_id, 'var_seatnum', var_seatnum, 'var_price', var_price, 'var_attr0', var_attr0, 'var_attr1', var_attr1, 'var_attr2', var_attr2, 'var_attr3', var_attr3, 'var_attr4', var_attr4, 'var_attr5', var_attr5, 'var_attr6', var_attr6, 'var_attr7', var_attr7, 'var_attr8', var_attr8), JSON_OBJECT('var_seats_left', var_seats_left, 'var_found', var_found, 'var_taken_seat_r_id', var_taken_seat_r_id, 'var_airline_id', var_airline_id));
  LEAVE NewReservation_Label;
END IF;

SELECT  R_ID INTO var_found FROM reservation
  WHERE R_C_ID = var_c_id AND R_F_ID = var_f_id;

IF var_found != -1 THEN
  SELECT "Error: this customer already has a seat on this flight";
  INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), 'NewReservation', JSON_OBJECT('var_r_id', var_r_id, 'var_c_id', var_c_id, 'var_f_id', var_f_id, 'var_seatnum', var_seatnum, 'var_price', var_price, 'var_attr0', var_attr0, 'var_attr1', var_attr1, 'var_attr2', var_attr2, 'var_attr3', var_attr3, 'var_attr4', var_attr4, 'var_attr5', var_attr5, 'var_attr6', var_attr6, 'var_attr7', var_attr7, 'var_attr8', var_attr8), JSON_OBJECT('var_seats_left', var_seats_left, 'var_found', var_found, 'var_taken_seat_r_id', var_taken_seat_r_id, 'var_airline_id', var_airline_id));
  LEAVE NewReservation_Label;
END IF;

SELECT  C_BASE_AP_ID INTO var_found
  FROM customer2 
  WHERE C_ID = var_c_id;

IF var_found = -1 THEN
  SELECT "Error: customer not found";
  INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), 'NewReservation', JSON_OBJECT('var_r_id', var_r_id, 'var_c_id', var_c_id, 'var_f_id', var_f_id, 'var_seatnum', var_seatnum, 'var_price', var_price, 'var_attr0', var_attr0, 'var_attr1', var_attr1, 'var_attr2', var_attr2, 'var_attr3', var_attr3, 'var_attr4', var_attr4, 'var_attr5', var_attr5, 'var_attr6', var_attr6, 'var_attr7', var_attr7, 'var_attr8', var_attr8), JSON_OBJECT('var_airline_id', var_airline_id, 'var_seats_left', var_seats_left, 'var_found', var_found, 'var_taken_seat_r_id', var_taken_seat_r_id));
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

  INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), 'NewReservation', JSON_OBJECT('var_r_id', var_r_id, 'var_c_id', var_c_id, 'var_f_id', var_f_id, 'var_seatnum', var_seatnum, 'var_price', var_price, 'var_attr0', var_attr0, 'var_attr1', var_attr1, 'var_attr2', var_attr2, 'var_attr3', var_attr3, 'var_attr4', var_attr4, 'var_attr5', var_attr5, 'var_attr6', var_attr6, 'var_attr7', var_attr7, 'var_attr8', var_attr8), JSON_OBJECT('var_airline_id', var_airline_id, 'var_seats_left', var_seats_left, 'var_found', var_found, 'var_taken_seat_r_id', var_taken_seat_r_id));
END//
DELIMITER ;


-- UpdateReservation transaction (procedure)

DROP PROCEDURE IF EXISTS UpdateReservation;
DELIMITER //
CREATE PROCEDURE UpdateReservation(IN var_r_id BIGINT,
                                   IN var_f_id VARCHAR(128),
                                   IN var_c_id VARCHAR(128),
                                   IN var_seatnum BIGINT,
                                   IN var_attr_idx BIGINT,
                                   IN var_attr_val BIGINT)
UpdateReservation_Label:BEGIN

DECLARE var_check_r_id BIGINT DEFAULT -1;

SELECT  R_ID INTO var_check_r_id FROM reservation WHERE R_F_ID = var_f_id AND R_SEAT = var_seatnum;

IF var_check_r_id != -1 THEN
  SELECT "Error: The seat is already taken by another customer";
  INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), 'UpdateReservation', JSON_OBJECT('var_r_id', var_r_id, 'var_f_id', var_f_id, 'var_c_id', var_c_id, 'var_seatnum', var_seatnum, 'var_attr_idx', var_attr_idx, 'var_attr_val', var_attr_val), JSON_OBJECT('var_check_r_id', var_check_r_id));
  LEAVE UpdateReservation_Label;
END IF;

SELECT R_ID INTO var_check_r_id FROM reservation
  WHERE R_C_ID = var_c_id AND R_F_ID = var_f_id;

IF var_check_r_id = -1 THEN
  SELECT "Error: the customer does not have a seat on this flight";
  INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), 'UpdateReservation', JSON_OBJECT('var_r_id', var_r_id, 'var_f_id', var_f_id, 'var_c_id', var_c_id, 'var_seatnum', var_seatnum, 'var_attr_idx', var_attr_idx, 'var_attr_val', var_attr_val), JSON_OBJECT('var_check_r_id', var_check_r_id));
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
  
    INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), 'UpdateReservation', JSON_OBJECT('var_r_id', var_r_id, 'var_f_id', var_f_id, 'var_c_id', var_c_id, 'var_seatnum', var_seatnum, 'var_attr_idx', var_attr_idx, 'var_attr_val', var_attr_val), JSON_OBJECT('var_check_r_id', var_check_r_id));
END//
DELIMITER ;

-- DeleteReservation transaction (procedure)

DROP PROCEDURE IF EXISTS DeleteReservation;
DELIMITER //
CREATE PROCEDURE DeleteReservation(IN var_f_id VARCHAR(128),
                                   IN var_c_id VARCHAR(128),
                                   IN var_c_id_str VARCHAR(64),
                                   IN var_ff_c_id_str VARCHAR(64),
                                   IN var_ff_al_id BIGINT)
DeleteReservation_Label:BEGIN

DECLARE var_c_iattr00 BIGINT;
DECLARE var_r_id BIGINT;
DECLARE var_r_price DECIMAL(7,3);

IF (var_c_id_str IS NOT NULL AND LENGTH(var_c_id_str) > 0) THEN
  SELECT C_ID INTO var_c_id FROM customer2 WHERE C_ID_STR = var_c_id_str;
ELSE
  SELECT  var_c_id = C_ID, var_ff_al_id = FF_AL_ID FROM customer2, frequent_flyer WHERE FF_C_ID_STR = var_ff_c_id_str AND FF_C_ID = C_ID LIMIT 1;
END IF;

IF var_c_id IS NULL OR LENGTH(var_c_id) = 0 THEN
  SELECT "Error: no customer record was found";
  INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), 'DeleteReservation', JSON_OBJECT('var_f_id', var_f_id, 'var_c_id', var_c_id, 'var_c_id_str', var_c_id_str, 'var_ff_c_id_str', var_ff_c_id_str, 'var_ff_al_id', var_ff_al_id), JSON_OBJECT('var_c_iattr00', var_c_iattr00, 'var_r_id', var_r_id, 'var_r_price', var_r_price));
  LEAVE DeleteReservation_Label;
END IF;

SELECT C_IATTR00, R_ID, R_PRICE INTO var_r_id, var_c_iattr00, var_r_price 
  FROM customer2, flight, reservation
  WHERE C_ID = var_c_id AND C_ID = R_C_ID AND F_ID = var_f_id AND F_ID = R_F_ID;

IF var_r_price IS NULL THEN
  SELECT "Error: no reservation is found";
  INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), 'DeleteReservation', JSON_OBJECT('var_f_id', var_f_id, 'var_c_id', var_c_id, 'var_c_id_str', var_c_id_str, 'var_ff_c_id_str', var_ff_c_id_str, 'var_ff_al_id', var_ff_al_id), JSON_OBJECT('var_r_price', var_r_price, 'var_c_iattr00', var_c_iattr00, 'var_r_id', var_r_id));
  LEAVE DeleteReservation_Label;
END IF;

DELETE FROM reservation WHERE R_ID = var_r_id AND R_C_ID = var_c_id AND R_F_ID = var_f_id;

UPDATE flight SET F_SEATS_LEFT = F_SEATS_LEFT + 1 WHERE F_ID = var_f_id;

UPDATE customer2 
  SET C_BALANCE = C_BALANCE - var_r_price, C_IATTR00 = var_c_iattr00, C_IATTR10 = C_IATTR10 - 1, C_IATTR11 = C_IATTR10 - 1 
  WHERE C_ID = var_c_id;

UPDATE frequent_flyer
  SET FF_IATTR10 = FF_IATTR10 - 1 WHERE FF_C_ID = var_c_id AND FF_AL_ID = var_ff_al_id;

  INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), 'DeleteReservation', JSON_OBJECT('var_f_id', var_f_id, 'var_c_id', var_c_id, 'var_c_id_str', var_c_id_str, 'var_ff_c_id_str', var_ff_c_id_str, 'var_ff_al_id', var_ff_al_id), JSON_OBJECT('var_c_iattr00', var_c_iattr00, 'var_r_id', var_r_id, 'var_r_price', var_r_price));
END//
DELIMITER ;
