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
END
