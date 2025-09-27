-- target: derived_12
INSERT INTO derived_12 SELECT col_a, col_b, col_c FROM table_20 WHERE col_a > 100;
CREATE VIEW derived_12 AS SELECT col_a, SUM(col_b) AS total FROM table_03 GROUP BY col_a;