-- target: derived_01
CREATE VIEW derived_01 AS SELECT col_a, SUM(col_b) AS total FROM table_11 GROUP BY col_a;
CREATE VIEW derived_01 AS SELECT col_a, SUM(col_b) AS total FROM table_34 GROUP BY col_a;
CREATE VIEW derived_01 AS SELECT col_a, SUM(col_b) AS total FROM table_15 GROUP BY col_a;
INSERT INTO derived_01 SELECT col_a, col_b, col_c FROM table_28 WHERE col_a > 100;