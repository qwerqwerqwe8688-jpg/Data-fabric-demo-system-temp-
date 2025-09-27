-- target: derived_19
INSERT INTO derived_19 SELECT col_a, col_b, col_c FROM table_21 WHERE col_a > 100;
CREATE VIEW derived_19 AS SELECT col_a, SUM(col_b) AS total FROM table_06 GROUP BY col_a;
CREATE VIEW derived_19 AS SELECT col_a, SUM(col_b) AS total FROM table_50 GROUP BY col_a;