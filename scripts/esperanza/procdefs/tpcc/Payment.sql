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
  DECLARE var_c_ytd_payment DECIMAL(12,4);
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
END
