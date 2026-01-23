DELIMITER $$

CREATE PROCEDURE greeter(IN p_user_id INT)
BEGIN
    DECLARE v_user_name VARCHAR(100);

    -- 사용자 이름 조회
SELECT name INTO v_user_name
FROM users
WHERE id = p_user_id;

-- 사용자가 없을 경우 처리 (선택 사항)
IF v_user_name IS NULL THEN
SELECT 'User not found' AS message;
ELSE
        -- 결과 문자열 반환
SELECT CONCAT('Hello, ', v_user_name) AS message;
END IF;
END $$

DELIMITER ;