-- target: derived_09
MERGE INTO derived_09 USING table_36 ON derived_09.id = table_36.id WHEN MATCHED THEN UPDATE SET val = table_36.val;
CREATE VIEW derived_09 AS SELECT col_a, SUM(col_b) AS total FROM table_05 GROUP BY col_a;