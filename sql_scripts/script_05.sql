-- target: derived_05
CREATE VIEW derived_05 AS SELECT col_a, SUM(col_b) AS total FROM table_21 GROUP BY col_a;
MERGE INTO derived_05 USING table_34 ON derived_05.id = table_34.id WHEN MATCHED THEN UPDATE SET val = table_34.val;
CREATE VIEW derived_05 AS SELECT col_a, SUM(col_b) AS total FROM table_29 GROUP BY col_a;
INSERT INTO derived_05 SELECT col_a, col_b, col_c FROM table_11 WHERE col_a > 100;