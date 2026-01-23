DELIMITER //

CREATE PROCEDURE calculate_final_price(
    IN p_base_price DECIMAL(10, 2),  -- 상품 원가
    IN p_is_vip BOOLEAN              -- VIP 여부
)
BEGIN
    -- 1. 변수 선언 (DECLARE는 항상 BEGIN 바로 아래에 와야 함)
    DECLARE v_tax_rate DECIMAL(4, 2) DEFAULT 0.10; -- 기본 세율 10%
    DECLARE v_discount DECIMAL(10, 2) DEFAULT 0;   -- 할인 금액 초기화
    DECLARE v_final_price DECIMAL(10, 2);          -- 최종 금액

    -- 2. 조건에 따른 변수 값 변경 (SET 사용)
    IF p_is_vip THEN
        SET v_discount = 5000; -- VIP면 5000원 할인 설정
    END IF;

    -- 3. 계산 로직 (SET을 이용한 수식 적용)
    -- 공식: (원가 + (원가 * 세율)) - 할인금액
    SET v_final_price = (p_base_price + (p_base_price * v_tax_rate)) - v_discount;

    -- 결과가 음수면 0으로 보정
    IF v_final_price < 0 THEN
        SET v_final_price = 0;
    END IF;

    -- 결과 반환
    SELECT
        p_base_price AS original_price,
        v_tax_rate AS tax_applied,
        v_discount AS discount_amount,
        v_final_price AS final_price;
END //

DELIMITER ;