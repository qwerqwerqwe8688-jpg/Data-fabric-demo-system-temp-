-- target: derived_17
MERGE INTO derived_17 USING table_35 ON derived_17.id = table_35.id WHEN MATCHED THEN UPDATE SET val = table_35.val;
INSERT INTO derived_17 SELECT col_a, col_b, col_c FROM table_42 WHERE col_a > 100;
INSERT INTO derived_17 SELECT col_a, col_b, col_c FROM table_43 WHERE col_a > 100;