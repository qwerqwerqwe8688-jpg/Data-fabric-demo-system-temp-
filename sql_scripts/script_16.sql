-- target: derived_16
MERGE INTO derived_16 USING table_27 ON derived_16.id = table_27.id WHEN MATCHED THEN UPDATE SET val = table_27.val;
INSERT INTO derived_16 SELECT col_a, col_b, col_c FROM table_28 WHERE col_a > 100;