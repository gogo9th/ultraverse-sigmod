CREATE PROCEDURE add_item(
    IN p_id INT,
    IN p_name VARCHAR(64),
    IN p_price INT,
    IN p_stock INT
)
BEGIN
    INSERT INTO items(id, name, price, stock)
    VALUES (p_id, p_name, p_price, p_stock);
END
