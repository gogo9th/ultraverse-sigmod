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
END
