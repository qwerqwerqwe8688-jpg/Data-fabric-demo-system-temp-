-- target: derived_02
INSERT INTO derived_02 SELECT col_a, col_b, col_c FROM table_35 WHERE col_a > 100;
CREATE VIEW derived_02 AS SELECT col_a, SUM(col_b) AS total FROM table_31 GROUP BY col_a;
CREATE VIEW derived_02 AS SELECT col_a, SUM(col_b) AS total FROM table_50 GROUP BY col_a;