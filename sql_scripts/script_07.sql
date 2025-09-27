-- target: derived_07
INSERT INTO derived_07 SELECT col_a, col_b, col_c FROM table_25 WHERE col_a > 100;
MERGE INTO derived_07 USING table_45 ON derived_07.id = table_45.id WHEN MATCHED THEN UPDATE SET val = table_45.val;
MERGE INTO derived_07 USING table_43 ON derived_07.id = table_43.id WHEN MATCHED THEN UPDATE SET val = table_43.val;
INSERT INTO derived_07 SELECT col_a, col_b, col_c FROM table_23 WHERE col_a > 100;