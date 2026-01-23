USE benchbase;

SET autocommit = 0;

START TRANSACTION;
CALL add_item(1, 'cola', 100, 10);
COMMIT;

START TRANSACTION;
CALL add_item(2, 'chips', 200, 5);
COMMIT;

START TRANSACTION;
CALL restock_item(1, 5);
COMMIT;

START TRANSACTION;
CALL buy_item(1, 3, @o1);
COMMIT;

START TRANSACTION;
CALL buy_item(2, 1, @o2);
COMMIT;

START TRANSACTION;
CALL refund_item(@o1);
COMMIT;

START TRANSACTION;
CALL restock_item(2, 2);
COMMIT;

START TRANSACTION;
CALL buy_item(1, 2, @o3);
COMMIT;
