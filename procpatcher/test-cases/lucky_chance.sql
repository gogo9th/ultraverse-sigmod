DELIMITER $$

CREATE PROCEDURE lucky_chance(IN p_user_id INT)
BEGIN
    DECLARE v_random_val INT;

    -- 1부터 10 사이의 랜덤 정수 생성
    -- RAND()는 0 <= v < 1.0, * 10 하면 0~9.xxx, FLOOR 후 +1 하면 1~10
    SET v_random_val = FLOOR(1 + RAND() * 10);

    -- 랜덤 값이 7인 경우에만 잔액 10,000원 추가
    IF v_random_val = 7 THEN
        UPDATE accounts
        SET balance = balance + 10000
        WHERE user_id = p_user_id;

        SELECT CONCAT('Lucky! You rolled a 7. Balance updated for User ', p_user_id) AS result;
    ELSE
        SELECT CONCAT('Sorry, you rolled a ', v_random_val, '. No luck this time.') AS result;
    END IF;
END $$

DELIMITER ;