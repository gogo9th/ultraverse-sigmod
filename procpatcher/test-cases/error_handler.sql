DELIMITER $$

CREATE PROCEDURE error_handler(
    IN p_user_id INT,
    IN p_action VARCHAR(20)
)
BEGIN
    DECLARE v_balance DECIMAL(10, 2);
    DECLARE v_error_code INT DEFAULT 0;
    DECLARE v_error_msg VARCHAR(255);

    -- 사용자 정의 에러 조건
    DECLARE invalid_action CONDITION FOR SQLSTATE '45001';
    DECLARE insufficient_funds CONDITION FOR SQLSTATE '45002';

    -- 에러 핸들러
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            v_error_code = MYSQL_ERRNO,
            v_error_msg = MESSAGE_TEXT;

        ROLLBACK;

        -- RESIGNAL로 에러 재발생
        RESIGNAL SET MESSAGE_TEXT = CONCAT('Handled error: ', v_error_msg);
    END;

    START TRANSACTION;

    -- 잔액 조회
    SELECT balance INTO v_balance
    FROM accounts
    WHERE user_id = p_user_id
    FOR UPDATE;

    -- 액션 유효성 검사
    IF p_action NOT IN ('deposit', 'withdraw', 'transfer') THEN
        SIGNAL invalid_action
            SET MESSAGE_TEXT = 'Invalid action specified',
                MYSQL_ERRNO = 1001;
    END IF;

    -- 출금 시 잔액 부족 검사
    IF p_action = 'withdraw' AND v_balance < 1000 THEN
        SIGNAL insufficient_funds
            SET MESSAGE_TEXT = 'Insufficient funds for withdrawal',
                MYSQL_ERRNO = 1002;
    END IF;

    -- 정상 처리
    IF p_action = 'deposit' THEN
        UPDATE accounts
        SET balance = balance + 1000
        WHERE user_id = p_user_id;
    ELSEIF p_action = 'withdraw' THEN
        UPDATE accounts
        SET balance = balance - 1000
        WHERE user_id = p_user_id;
    END IF;

    COMMIT;
    SELECT 'Operation completed successfully' AS result;
END $$

DELIMITER ;
