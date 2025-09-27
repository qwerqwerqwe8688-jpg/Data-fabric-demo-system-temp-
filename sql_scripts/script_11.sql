-- target: derived_11
CREATE VIEW derived_11 AS SELECT col_a, SUM(col_b) AS total FROM table_42 GROUP BY col_a;
INSERT INTO derived_11 SELECT col_a, col_b, col_c FROM table_07 WHERE col_a > 100;