CREATE PROCEDURE restock_item(
    IN p_id INT,
    IN p_additional INT
)
BEGIN
    UPDATE items
    SET stock = stock + p_additional
    WHERE id = p_id;
END
