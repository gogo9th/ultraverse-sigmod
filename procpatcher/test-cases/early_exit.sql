DELIMITER $$

CREATE PROCEDURE early_exit(IN p_user_id INT, OUT p_status VARCHAR(50))
proc_body: BEGIN
    DECLARE v_balance DECIMAL(10, 2);
    DECLARE v_user_name VARCHAR(100);

    -- 사용자 조회
    SELECT name, balance INTO v_user_name, v_balance
    FROM users u
    JOIN accounts a ON u.id = a.user_id
    WHERE u.id = p_user_id;

    -- 사용자가 없으면 조기 종료
    IF v_user_name IS NULL THEN
        SET p_status = 'USER_NOT_FOUND';
        LEAVE proc_body;
    END IF;

    -- 잔액이 0 이하면 조기 종료
    IF v_balance <= 0 THEN
        SET p_status = 'NO_BALANCE';
        LEAVE proc_body;
    END IF;

    -- 중첩 블록에서의 LEAVE
    inner_block: BEGIN
        DECLARE v_bonus INT DEFAULT 0;

        IF v_balance < 1000 THEN
            SET v_bonus = 100;
            LEAVE inner_block;
        END IF;

        SET v_bonus = 500;

        UPDATE accounts
        SET balance = balance + v_bonus
        WHERE user_id = p_user_id;
    END inner_block;

    SET p_status = 'SUCCESS';
END proc_body $$

DELIMITER ;
