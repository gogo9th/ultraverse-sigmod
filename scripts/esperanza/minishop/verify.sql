USE benchbase;

SELECT stock FROM items WHERE id = 1;
SELECT stock FROM items WHERE id = 2;

SELECT status
FROM orders
WHERE order_id = (
    SELECT MIN(order_id)
    FROM orders
    WHERE item_id = 1
);

SELECT COUNT(*)
FROM refunds
WHERE order_id = (
    SELECT MIN(order_id)
    FROM orders
    WHERE item_id = 1
);
