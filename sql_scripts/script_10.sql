-- target: derived_10
INSERT INTO derived_10 SELECT col_a, col_b, col_c FROM table_20 WHERE col_a > 100;
CREATE VIEW derived_10 AS SELECT col_a, SUM(col_b) AS total FROM table_11 GROUP BY col_a;
INSERT INTO derived_10 SELECT col_a, col_b, col_c FROM table_15 WHERE col_a > 100;