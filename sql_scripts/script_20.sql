-- target: derived_20
INSERT INTO derived_20 SELECT col_a, col_b, col_c FROM table_37 WHERE col_a > 100;
MERGE INTO derived_20 USING table_45 ON derived_20.id = table_45.id WHEN MATCHED THEN UPDATE SET val = table_45.val;