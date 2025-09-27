-- target: derived_14
CREATE VIEW derived_14 AS SELECT col_a, SUM(col_b) AS total FROM table_27 GROUP BY col_a;
CREATE VIEW derived_14 AS SELECT col_a, SUM(col_b) AS total FROM table_40 GROUP BY col_a;
INSERT INTO derived_14 SELECT col_a, col_b, col_c FROM table_28 WHERE col_a > 100;