USE benchbase;

DELIMITER $$

DROP PROCEDURE IF EXISTS add_item$$
CREATE PROCEDURE add_item(
    IN p_id INT,
    IN p_name VARCHAR(64),
    IN p_price INT,
    IN p_stock INT
)
BEGIN
    INSERT INTO items(id, name, price, stock)
    VALUES (p_id, p_name, p_price, p_stock);
END$$

DROP PROCEDURE IF EXISTS restock_item$$
CREATE PROCEDURE restock_item(
    IN p_id INT,
    IN p_additional INT
)
BEGIN
    UPDATE items
    SET stock = stock + p_additional
    WHERE id = p_id;
END$$

DROP PROCEDURE IF EXISTS buy_item$$
CREATE PROCEDURE buy_item(
    IN p_item_id INT,
    IN p_qty INT,
    OUT p_order_id BIGINT
)
BEGIN
    DECLARE v_price INT;

    SELECT price INTO v_price
    FROM items
    WHERE id = p_item_id;

    INSERT INTO orders(item_id, quantity, total_price, status)
    VALUES (p_item_id, p_qty, v_price * p_qty, 'PAID');

    SET p_order_id = LAST_INSERT_ID();

    UPDATE items
    SET stock = stock - p_qty
    WHERE id = p_item_id;
END$$

DROP PROCEDURE IF EXISTS refund_item$$
CREATE PROCEDURE refund_item(
    IN p_order_id BIGINT
)
BEGIN
    DECLARE v_item_id INT;
    DECLARE v_qty INT;
    DECLARE v_amount INT;

    SELECT item_id, quantity, total_price
    INTO v_item_id, v_qty, v_amount
    FROM orders
    WHERE order_id = p_order_id;

    UPDATE orders
    SET status = 'REFUNDED'
    WHERE order_id = p_order_id;

    UPDATE items
    SET stock = stock + v_qty
    WHERE id = v_item_id;

    INSERT INTO refunds(order_id, amount)
    VALUES (p_order_id, v_amount);
END$$

DELIMITER ;
