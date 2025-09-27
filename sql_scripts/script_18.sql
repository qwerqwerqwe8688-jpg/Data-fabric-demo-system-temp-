-- target: derived_18
INSERT INTO derived_18 SELECT col_a, col_b, col_c FROM table_02 WHERE col_a > 100;
INSERT INTO derived_18 SELECT col_a, col_b, col_c FROM table_11 WHERE col_a > 100;
INSERT INTO derived_18 SELECT col_a, col_b, col_c FROM table_07 WHERE col_a > 100;
MERGE INTO derived_18 USING table_15 ON derived_18.id = table_15.id WHEN MATCHED THEN UPDATE SET val = table_15.val;