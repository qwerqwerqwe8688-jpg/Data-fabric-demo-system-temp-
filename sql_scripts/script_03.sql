-- target: derived_03
MERGE INTO derived_03 USING table_23 ON derived_03.id = table_23.id WHEN MATCHED THEN UPDATE SET val = table_23.val;
INSERT INTO derived_03 SELECT col_a, col_b, col_c FROM table_04 WHERE col_a > 100;
MERGE INTO derived_03 USING table_15 ON derived_03.id = table_15.id WHEN MATCHED THEN UPDATE SET val = table_15.val;