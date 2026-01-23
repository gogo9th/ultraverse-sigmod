CREATE PROCEDURE UpdateRow(IN p_id INT)
BEGIN
    UPDATE updateonly SET value = value + 1 WHERE id = p_id;
END
