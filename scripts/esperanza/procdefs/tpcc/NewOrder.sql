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

  SET @tpcc_seed := UNIX_TIMESTAMP(CURRENT_TIMESTAMP());

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
    SET var_i_id = (NonUniformRandom(@tpcc_seed, 8191, 7911, 1, 100000));
    SET var_ol_quantity = RandomNumber(@tpcc_seed, 1, 10);

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
END
