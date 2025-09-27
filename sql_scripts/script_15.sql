-- target: derived_15
INSERT INTO derived_15 SELECT col_a, col_b, col_c FROM table_40 WHERE col_a > 100;
CREATE VIEW derived_15 AS SELECT col_a, SUM(col_b) AS total FROM table_39 GROUP BY col_a;