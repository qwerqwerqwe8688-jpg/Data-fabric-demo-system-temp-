-- target: derived_13
MERGE INTO derived_13 USING table_28 ON derived_13.id = table_28.id WHEN MATCHED THEN UPDATE SET val = table_28.val;
CREATE VIEW derived_13 AS SELECT col_a, SUM(col_b) AS total FROM table_12 GROUP BY col_a;
CREATE VIEW derived_13 AS SELECT col_a, SUM(col_b) AS total FROM table_31 GROUP BY col_a;