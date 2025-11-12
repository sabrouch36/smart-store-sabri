-- SQLite
SELECT *
FROM customer
WHERE name IS NULL OR region IS NULL OR join_date IS NULL;

SELECT *
FROM product
WHERE product_name IS NULL OR category IS NULL;

SELECT *
FROM sale
WHERE sale_date IS NULL
   OR customer_id IS NULL
   OR product_id IS NULL
   OR sale_amount IS NULL;

SELECT s.sale_id
FROM sale s
LEFT JOIN customer c ON s.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

SELECT s.sale_id
FROM sale s
LEFT JOIN product p ON s.product_id = p.product_id
WHERE p.product_id IS NULL;

SELECT customer_id, COUNT(*) AS n
FROM customer
GROUP BY customer_id
HAVING COUNT(*) > 1;

SELECT product_id, COUNT(*) AS n
FROM product
GROUP BY product_id
HAVING COUNT(*) > 1;

SELECT sale_id, COUNT(*) AS n
FROM sale
GROUP BY sale_id
HAVING COUNT(*) > 1;
