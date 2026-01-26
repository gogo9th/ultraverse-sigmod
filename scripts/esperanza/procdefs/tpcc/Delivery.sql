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

  SET @tpcc_seed := UNIX_TIMESTAMP(CURRENT_TIMESTAMP());

  SET var_o_carrier_id := RandomNumber(@tpcc_seed, 1, 10);
  
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
END
