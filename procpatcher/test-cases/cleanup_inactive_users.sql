DELIMITER $$

CREATE PROCEDURE cleanup_inactive_users(IN p_days_threshold INT)
BEGIN
    DECLARE v_affected_rows INT;

    -- 지정된 일수(p_days_threshold)보다 오래 로그인하지 않은 활성 유저 업데이트
UPDATE users
SET status = 'Inactive'
WHERE status = 'Active'
  AND last_login_at < DATE_SUB(NOW(), INTERVAL p_days_threshold DAY);

-- 변경된 행의 개수 확인
SET v_affected_rows = ROW_COUNT();

SELECT CONCAT(v_affected_rows, ' users have been marked as Inactive.') AS result;
END $$

DELIMITER ;