-- target: derived_06
INSERT INTO derived_06 SELECT col_a, col_b, col_c FROM table_16 WHERE col_a > 100;
CREATE VIEW derived_06 AS SELECT col_a, SUM(col_b) AS total FROM table_12 GROUP BY col_a;