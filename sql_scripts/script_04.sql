-- target: derived_04
INSERT INTO derived_04 SELECT col_a, col_b, col_c FROM table_16 WHERE col_a > 100;
INSERT INTO derived_04 SELECT col_a, col_b, col_c FROM table_22 WHERE col_a > 100;
INSERT INTO derived_04 SELECT col_a, col_b, col_c FROM table_44 WHERE col_a > 100;