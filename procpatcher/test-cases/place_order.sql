DELIMITER $$

CREATE PROCEDURE place_order(
    IN p_user_id INT,
    IN p_product_name VARCHAR(100),
    IN p_amount DECIMAL(10, 2)
)
BEGIN
    DECLARE v_current_balance DECIMAL(10, 2);

    -- 에러 핸들러: 예외 발생 시 롤백
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
ROLLBACK;
SELECT 'Transaction Failed: Order cancelled.' AS result;
END;

    -- 트랜잭션 시작
START TRANSACTION;

-- 현재 잔액 확인 (FOR UPDATE로 잠금)
SELECT balance INTO v_current_balance
FROM accounts
WHERE user_id = p_user_id
    FOR UPDATE;

-- 잔액이 충분한지 확인
IF v_current_balance >= p_amount THEN
        -- 1. 주문 내역 인서트
        INSERT INTO orders (user_id, product_name, amount, order_date)
        VALUES (p_user_id, p_product_name, p_amount, NOW());

        -- 2. 잔액 차감 업데이트
UPDATE accounts
SET balance = balance - p_amount
WHERE user_id = p_user_id;

COMMIT;
SELECT 'Order placed successfully.' AS result;
ELSE
        ROLLBACK; -- 잔액 부족 시 롤백 (사실 읽기만 했으므로 필수는 아니지만 관례상)
SELECT 'Insufficient funds.' AS result;
END IF;
END $$

DELIMITER ;