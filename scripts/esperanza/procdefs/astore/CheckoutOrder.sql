CREATE PROCEDURE CheckoutOrder(
    IN req_user_UserID INT,
    IN req_session_address_AddressID INT,
    IN req_session_cartSummary_subTotal DECIMAL(10, 2),
    IN req_session_cartSummary_discount DECIMAL(10, 2),
    IN req_session_cartSummary_shipCost DECIMAL(10, 2),
    IN req_session_cartSummary_total DECIMAL(10, 2),
    IN req_session_cart_json VARCHAR(1024)
)
CheckoutOrder_Label:BEGIN

DECLARE i INT DEFAULT 0;
DECLARE var_json_key VARCHAR(128);
DECLARE var_0 VARCHAR(1024);
DECLARE var_1 INT;
DECLARE var_2 INT;
DECLARE var_3 DECIMAL(10, 2);
DECLARE var_inserted_id_0 INT DEFAULT NULL;
DECLARE var_json_keys_0 JSON;
DROP TEMPORARY TABLE IF EXISTS table_0;
CREATE TEMPORARY TABLE IF NOT EXISTS table_0 LIKE Orders;
TRUNCATE table_0; 

INSERT INTO Orders (orderid, userid, addressid, subtotal, discount, shippingfee, total, orderdate, status)
VALUES(null, req_user_UserID, req_session_address_AddressID,
    req_session_cartSummary_subTotal, req_session_cartSummary_discount,
    req_session_cartSummary_shipCost, req_session_cartSummary_total, 
    NOW(), 'Order Received'); 
SET var_inserted_id_0 := LAST_INSERT_ID();

SELECT  JSON_KEYS(req_session_cart_json) INTO var_json_keys_0;
WHILE i < JSON_LENGTH(var_json_keys_0) DO
    SET var_json_key := JSON_EXTRACT(JSON_UNQUOTE(var_json_keys_0), CONCAT('$[', i, ']'));
    SET var_0 := JSON_EXTRACT(req_session_cart_json, CONCAT("$.", var_json_key));
    SET var_2 := JSON_EXTRACT(var_0, CONCAT("$.quantity"));
    IF (var_2 > 0) THEN
        SET var_1 := JSON_EXTRACT(var_0, "$.ProductID");
        SET var_3 := JSON_EXTRACT(var_0, "$.productTotal");
        INSERT INTO \`Order Details\` (orderid, productid, quantity, total) VALUES(var_inserted_id_0, var_1, var_2, var_3);
        UPDATE Products SET UnitsInStock = (UnitsInStock - var_2) 
        WHERE ProductID = var_1;
        SET i := i + 1;
                END IF;
            END WHILE;

            INSERT INTO table_0  
            SELECT * FROM Orders WHERE OrderID = var_inserted_id_0;

            SELECT * FROM Addresses WHERE AddressID = (SELECT AddressID FROM table_0 LIMIT 1);

            SELECT * FROM \`Order Details\` INNER JOIN (
                SELECT Products.*, Categories.CategorySlug
                FROM Products
                INNER JOIN Categories
                ON Products.CategoryID = Categories.CategoryID
            ) Tab ON \`Order Details\`.ProductID = Tab.ProductID
            WHERE OrderID = (SELECT OrderID FROM table_0 LIMIT 1); 
        END
