SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS history;
DROP TABLE IF EXISTS new_order;
DROP TABLE IF EXISTS order_line;
DROP TABLE IF EXISTS oorder;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS district;
DROP TABLE IF EXISTS stock;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS warehouse;

CREATE TABLE warehouse (
    w_id       int            NOT NULL,
    w_ytd      decimal(14, 2) NOT NULL,
    w_tax      decimal(4, 4)  NOT NULL,
    w_name     varchar(10)    NOT NULL,
    w_street_1 varchar(20)    NOT NULL,
    w_street_2 varchar(20)    NOT NULL,
    w_city     varchar(20)    NOT NULL,
    w_state    char(2)        NOT NULL,
    w_zip      char(9)        NOT NULL,
    PRIMARY KEY (w_id)
)  ;

CREATE TABLE item (
    i_id    int           NOT NULL,
    i_name  varchar(24)   NOT NULL,
    i_price decimal(5, 2) NOT NULL,
    i_data  varchar(50)   NOT NULL,
    i_im_id int           NOT NULL,
    PRIMARY KEY (i_id)
)  ;

CREATE TABLE stock (
    s_w_id       int           NOT NULL,
    s_i_id       int           NOT NULL,
    s_quantity   int           NOT NULL,
    s_ytd        decimal(14, 2) NOT NULL,
    s_order_cnt  int           NOT NULL,
    s_remote_cnt int           NOT NULL,
    s_data       varchar(50)   NOT NULL,
    s_dist_01    char(24)      NOT NULL,
    s_dist_02    char(24)      NOT NULL,
    s_dist_03    char(24)      NOT NULL,
    s_dist_04    char(24)      NOT NULL,
    s_dist_05    char(24)      NOT NULL,
    s_dist_06    char(24)      NOT NULL,
    s_dist_07    char(24)      NOT NULL,
    s_dist_08    char(24)      NOT NULL,
    s_dist_09    char(24)      NOT NULL,
    s_dist_10    char(24)      NOT NULL,
    FOREIGN KEY (s_w_id) REFERENCES warehouse (w_id) ON DELETE CASCADE,
    FOREIGN KEY (s_i_id) REFERENCES item (i_id) ON DELETE CASCADE,
    PRIMARY KEY (s_w_id, s_i_id)
)  ;

CREATE TABLE district (
    d_w_id      int            NOT NULL,
    d_id        int            NOT NULL,
    d_ytd       decimal(14, 2) NOT NULL,
    d_tax       decimal(4, 4)  NOT NULL,
    d_next_o_id int            NOT NULL,
    d_name      varchar(10)    NOT NULL,
    d_street_1  varchar(20)    NOT NULL,
    d_street_2  varchar(20)    NOT NULL,
    d_city      varchar(20)    NOT NULL,
    d_state     char(2)        NOT NULL,
    d_zip       char(9)        NOT NULL,
    FOREIGN KEY (d_w_id) REFERENCES warehouse (w_id) ON DELETE CASCADE,
    PRIMARY KEY (d_w_id, d_id)
)  ;

CREATE TABLE customer (
    c_w_id         int            NOT NULL,
    c_d_id         int            NOT NULL,
    c_id           int            NOT NULL,
    c_discount     decimal(4, 4)  NOT NULL,
    c_credit       char(2)        NOT NULL,
    c_last         varchar(16)    NOT NULL,
    c_first        varchar(16)    NOT NULL,
    c_credit_lim   decimal(12, 2) NOT NULL,
    c_balance      decimal(12, 2) NOT NULL,
    c_ytd_payment  float          NOT NULL,
    c_payment_cnt  int            NOT NULL,
    c_delivery_cnt int            NOT NULL,
    c_street_1     varchar(20)    NOT NULL,
    c_street_2     varchar(20)    NOT NULL,
    c_city         varchar(20)    NOT NULL,
    c_state        char(2)        NOT NULL,
    c_zip          char(9)        NOT NULL,
    c_phone        char(16)       NOT NULL,
    c_since        timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    c_middle       char(2)        NOT NULL,
    c_data         varchar(500)   NOT NULL,
    FOREIGN KEY (c_w_id, c_d_id) REFERENCES district (d_w_id, d_id) ON DELETE CASCADE,
    PRIMARY KEY (c_w_id, c_d_id, c_id)
)  ;

CREATE TABLE history (
    h_c_id   int           NOT NULL,
    h_c_d_id int           NOT NULL,
    h_c_w_id int           NOT NULL,
    h_d_id   int           NOT NULL,
    h_w_id   int           NOT NULL,
    h_date   timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    h_amount decimal(6, 2) NOT NULL,
    h_data   varchar(24)   NOT NULL,
    FOREIGN KEY (h_c_w_id, h_c_d_id, h_c_id) REFERENCES customer (c_w_id, c_d_id, c_id) ON DELETE CASCADE,
    FOREIGN KEY (h_w_id, h_d_id) REFERENCES district (d_w_id, d_id) ON DELETE CASCADE
)  ;

CREATE TABLE oorder (
    o_w_id       int       NOT NULL,
    o_d_id       int       NOT NULL,
    o_id         int       NOT NULL,
    o_c_id       int       NOT NULL,
    o_carrier_id int                DEFAULT NULL,
    o_ol_cnt     int       NOT NULL,
    o_all_local  int       NOT NULL,
    o_entry_d    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (o_w_id, o_d_id, o_id),
    FOREIGN KEY (o_w_id, o_d_id, o_c_id) REFERENCES customer (c_w_id, c_d_id, c_id) ON DELETE CASCADE,
    UNIQUE (o_w_id, o_d_id, o_c_id, o_id)
)  ;

CREATE TABLE new_order (
    no_w_id int NOT NULL,
    no_d_id int NOT NULL,
    no_o_id int NOT NULL,
    FOREIGN KEY (no_w_id, no_d_id, no_o_id) REFERENCES oorder (o_w_id, o_d_id, o_id) ON DELETE CASCADE,
    PRIMARY KEY (no_w_id, no_d_id, no_o_id)
)  ;

CREATE TABLE order_line (
    ol_w_id        int           NOT NULL,
    ol_d_id        int           NOT NULL,
    ol_o_id        int           NOT NULL,
    ol_number      int           NOT NULL,
    ol_i_id        int           NOT NULL,
    ol_delivery_d  timestamp     NULL DEFAULT NULL,
    ol_amount      decimal(6, 2) NOT NULL,
    ol_supply_w_id int           NOT NULL,
    ol_quantity    int           NOT NULL,
    ol_dist_info   char(24)      NOT NULL,
    FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id) REFERENCES oorder (o_w_id, o_d_id, o_id) ON DELETE CASCADE,
    FOREIGN KEY (ol_supply_w_id, ol_i_id) REFERENCES stock (s_w_id, s_i_id) ON DELETE CASCADE,
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
)  ;

CREATE INDEX idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first);
CREATE INDEX idx_order ON oorder (o_w_id, o_d_id, o_c_id, o_id);

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;


-- Utility transactions (procedures)
DROP FUNCTION IF EXISTS RandomNumber;
DELIMITER //
CREATE FUNCTION RandomNumber(minval INT, maxval INT) RETURNS FLOAT
BEGIN
  RETURN FLOOR(RAND()*(maxval - minval + 1)) + minval;
END//
DELIMITER ;

DROP FUNCTION IF EXISTS NonUniformRandom;
DELIMITER //
CREATE FUNCTION NonUniformRandom(a INT, c INT, minval INT, maxval INT) RETURNS FLOAT
BEGIN
  RETURN (((randomNumber(0, a) | randomNumber(minval, maxval)) + c) MOD (maxval - minval + 1)) + minval;
END//
DELIMITER ;


-- NewOrder transaction (procedure)
DROP PROCEDURE IF EXISTS NewOrder;
DELIMITER //
CREATE PROCEDURE NewOrder(IN var_w_id INT,
                                   IN var_c_id INT,
                                   IN var_d_id INT,
                                   IN var_o_ol_cnt INT,
                                   IN var_ol_supply_w_id INT
                        )
NewOrder_Label:BEGIN
  DECLARE var_loop_cnt INT DEFAULT 0;

  DECLARE var_i_id INT;
  DECLARE var_ol_quantity INT;
  DECLARE var_s_quantity INT;
  DECLARE var_s_dist_info VARCHAR(24);
  DECLARE var_o_all_local INT DEFAULT 1;
  DECLARE var_s_remote_cnt_increment INT;
  DECLARE var_d_next_o_id INT DEFAULT -1;
  DECLARE var_i_price DECIMAL(5, 2);

  IF ((SELECT COUNT(*) FROM customer
  WHERE C_W_ID = var_w_id AND C_D_ID = var_d_id AND C_ID = var_c_id) < 1) THEN
    SELECT "Error: the customer is not found";
    LEAVE NewOrder_Label;
  ELSEIF ((SELECT COUNT(*) FROM warehouse WHERE W_ID = var_w_id) < 1) THEN
    SELECT "Error: the warehouse is not found";
    LEAVE NewOrder_Label;
  END IF;

  SELECT  D_NEXT_O_ID INTO var_d_next_o_id FROM district WHERE D_W_ID = var_w_id AND D_ID = var_d_id FOR UPDATE;

  IF var_d_next_o_id = -1 THEN
    SELECT "Error: the district is not found";
    LEAVE NewOrder_Label;
  END IF;

  UPDATE district SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = var_w_id AND d_id = var_d_id;

  INSERT INTO oorder (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)
  VALUES (var_d_next_o_id, var_d_id, var_w_id, var_c_id, CURRENT_TIMESTAMP(), var_loop_cnt, var_o_all_local);

  INSERT INTO new_order (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (var_d_next_o_id, var_d_id, var_w_id);

  Order_Loop:WHILE (var_loop_cnt < var_o_ol_cnt) DO
    SET var_i_id = (NonUniformRandom(8191, 7911, 1, 100000));
    SET var_ol_quantity = RandomNumber(1, 10);

    SELECT  I_PRICE INTO var_i_price  FROM item WHERE I_ID = var_i_id;
    IF (var_ol_supply_w_id = var_w_id) THEN
      SET var_s_remote_cnt_increment = 0;
    ELSE
      SET var_s_remote_cnt_increment = 1;
    END IF;

    SELECT S_QUANTITY, S_DIST_01 INTO var_s_quantity,  var_s_dist_info FROM stock
    WHERE S_I_ID = var_i_id AND S_W_ID = var_ol_supply_w_id FOR UPDATE;

    SET var_s_quantity := var_s_quantity - var_ol_quantity;
    IF (var_s_quantity < 10) THEN
      SET var_s_quantity := var_s_quantity + 91;
    END IF;

    INSERT INTO order_line (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (var_d_next_o_id, var_d_id, var_w_id, var_loop_cnt, var_i_id, var_ol_supply_w_id, var_ol_quantity, var_ol_quantity * var_i_price, var_s_dist_info);

    UPDATE stock SET S_QUANTITY = var_s_quantity, 
                     S_YTD := S_YTD + var_ol_quantity,
                     S_ORDER_CNT := S_ORDER_CNT + 1,
                     S_REMOTE_CNT := S_REMOTE_CNT + var_s_remote_cnt_increment
    WHERE S_I_ID = var_i_id AND S_W_ID = var_ol_supply_w_id;

    SET var_loop_cnt := var_loop_cnt + 1;
  END WHILE;
END//
DELIMITER ;


-- Payment transaction (procedure)
DROP PROCEDURE IF EXISTS Payment;
DELIMITER //
CREATE PROCEDURE Payment(IN var_w_id INT,
                                   IN var_d_id INT,
                                   IN var_customerDistrictID INT,
                                   IN var_customerWarehouseID INT,
                                   IN var_c_id INT,
                                   IN var_paymentAmount DECIMAL(6,2)
                         )
Payment_Label:BEGIN

  DECLARE var_w_name VARCHAR(10) DEFAULT NULL;
  DECLARE var_d_name VARCHAR(10) DEFAULT NULL;
  DECLARE var_x INT;
  DECLARE var_c_balance DECIMAL(12,2);     
  DECLARE var_c_ytd_payment FLOAT;
  DECLARE var_c_payment_cnt INT DEFAULT -1;
  DECLARE var_c_data VARCHAR(500);
  DECLARE var_c_credit VARCHAR(2);


  SELECT W_NAME INTO var_w_name FROM warehouse WHERE W_ID = var_w_id;

  IF (var_w_name IS NULL) THEN
    SELECT "Error: w_id is not found";
    LEAVE Payment_Label;
  END IF;

  UPDATE warehouse SET W_YTD = W_YTD + var_paymentAmount WHERE W_ID = var_w_id;

  SELECT  D_NAME INTO var_d_name FROM district
  WHERE D_W_ID = var_w_id AND D_ID = var_d_id;

  IF (var_d_name IS NULL) THEN
    SELECT "Error: d_id is not found";
    LEAVE Payment_Label;
  END IF;

  UPDATE district SET D_YTD = D_YTD + var_paymentAmount 
  WHERE D_W_ID = var_w_id AND D_ID = var_d_id;

  SELECT C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_CREDIT, C_DATA INTO var_c_balance, var_c_ytd_payment, var_c_payment_cnt, var_c_credit, var_c_data FROM customer
  WHERE C_W_ID = var_customerWarehouseID AND C_D_ID = var_customerDistrictID AND C_ID = var_c_id;

  IF (var_c_payment_cnt = -1) THEN
    SELECT "Error: paid customer is not found";
    LEAVE Payment_Label;
  END IF;
  
  SET var_c_balance := var_c_balance - var_paymentAmount;
  SET var_c_ytd_payment := var_c_ytd_payment + var_paymentAmount;
  SET var_c_payment_cnt := var_c_payment_cnt + 1;
  IF (var_c_credit = 'BC') THEN

    UPDATE customer SET C_BALANCE = var_c_balance, C_YTD_PAYMENT = var_c_ytd_payment, 
    C_PAYMENT_CNT = var_c_payment_cnt, C_DATA = var_c_data 
    WHERE C_W_ID = var_customerWarehouseID AND C_D_ID = var_customerDistrictID 
    AND C_ID = var_c_id;
    
  ELSE
    UPDATE customer SET C_BALANCE = var_c_balance, C_YTD_PAYMENT = var_c_ytd_payment, 
    C_PAYMENT_CNT = var_c_payment_cnt
    WHERE C_W_ID = var_customerWarehouseID AND C_D_ID = var_customerDistrictID 
    AND C_ID = var_c_id;
  
  END IF;
  
  INSERT INTO history (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) VALUES (var_customerDistrictID, var_customerWarehouseID, var_c_id, var_d_id, var_w_id, CURRENT_TIMESTAMP(), var_paymentAmount, CONCAT(var_w_name, '  ', var_d_name));
END//
DELIMITER ;

-- Delivery transaction (procedure)

DROP PROCEDURE IF EXISTS Delivery;
DELIMITER //
CREATE PROCEDURE Delivery(IN var_w_id INT,
                         IN var_terminalDistrictLowerID INT,
                         IN var_terminalDistrictUpperID INT,
                         IN va_o_carrier_id INT
                        )
Delivery_Label:BEGIN

  DECLARE var_d_id INT DEFAULT 1;
  DECLARE var_no_o_id INT;
  DECLARE var_o_c_id INT;
  DECLARE var_o_carrier_id INT;
  DECLARE var_ol_total DECIMAL(8, 2);  

  SET var_o_carrier_id := RandomNumber(1, 10);
  
  Delivery_Loop:WHILE (var_d_id < var_terminalDistrictUpperID) DO
    SET var_no_o_id := -1;

    SELECT NO_O_ID INTO var_no_o_id  FROM new_order 
    WHERE NO_D_ID = var_d_id AND NO_W_ID = var_w_id 
    ORDER BY NO_O_ID ASC LIMIT 1;

    IF (var_no_o_id = -1) THEN
      SELECT "Warning: No new order";
    ELSE
      DELETE FROM new_order 
      WHERE NO_O_ID = var_no_o_id AND NO_D_ID = var_d_id AND NO_W_ID = var_w_id;

      UPDATE oorder SET O_CARRIER_ID = var_o_carrier_id 
      WHERE O_ID = var_no_o_id AND O_D_ID = var_d_id AND O_W_ID = var_w_id;
    
      UPDATE order_line SET OL_DELIVERY_D = CURRENT_TIMESTAMP()
      WHERE OL_O_ID = var_no_o_id AND OL_D_ID = var_d_id AND OL_W_ID = var_w_id;

      SELECT  O_C_ID INTO var_o_c_id FROM oorder 
      WHERE O_ID = var_no_o_id AND O_D_ID = var_d_id AND O_W_ID = var_w_id;

      SELECT  SUM(OL_AMOUNT) INTO var_ol_total FROM order_line
      WHERE OL_O_ID = var_no_o_id AND OL_D_ID = var_d_id AND OL_W_ID = var_w_id;

      UPDATE customer 
      SET C_BALANCE := C_BALANCE + var_ol_total, C_DELIVERY_CNT := C_DELIVERY_CNT + 1
      WHERE C_W_ID = var_w_id AND C_D_ID = var_d_id AND C_ID = var_o_c_id; 
    END IF;

    SET var_d_id := var_d_id + 1;
  END WHILE;
END//
DELIMITER ;
