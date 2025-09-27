#!/usr/bin/env python3
"""
一键生成 50 张 CSV + 50 个 SQL 脚本
CSV：随机业务数据，UTF-8 编码
SQL：随机 INSERT / CREATE VIEW / MERGE，自动引用 CSV 表名，形成复杂血缘
"""
import csv
import random
import string
import shutil
from pathlib import Path

random.seed(42)

# ---------- 参数 ----------
N_CSV   = 50          # CSV 文件数
N_SQL   = 50          # SQL 脚本数
MIN_ROW = 50
MAX_ROW = 200
MIN_COL = 3
MAX_COL = 8
OUTPUT_CSV = Path("data")
OUTPUT_SQL = Path("sql_scripts")

# ---------- 工具 ----------
def random_name(k=6):
    return ''.join(random.choices(string.ascii_lowercase, k=k))

def random_columns(n):
    return [f"{random_name()}_col" for _ in range(n)]

def random_cell():
    return random.choice([
        ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(3, 12))),
        random.randint(1, 10000),
        round(random.uniform(1, 1000), 2),
        random.choice(["北京", "上海", "广州", "深圳", "杭州"]),
    ])

# ---------- 1. 生成 CSV ----------
OUTPUT_CSV.mkdir(exist_ok=True)
csv_files = []          # 保存文件名列表，供 SQL 引用
for i in range(1, N_CSV + 1):
    file = OUTPUT_CSV / f"table_{i:02d}.csv"
    csv_files.append(file.stem)
    n_col = random.randint(MIN_COL, MAX_COL)
    cols  = random_columns(n_col)
    n_row = random.randint(MIN_ROW, MAX_ROW)
    with file.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        for _ in range(n_row):
            writer.writerow([random_cell() for _ in cols])
print(f"✅ 生成 {N_CSV} 个 CSV 到 {OUTPUT_CSV}")

# ---------- 2. 生成 SQL ----------
OUTPUT_SQL.mkdir(exist_ok=True)
templates = [
    # 0 INSERT INTO
    lambda src, dst: f"INSERT INTO {dst} SELECT col_a, col_b, col_c FROM {src} WHERE col_a > 100;",
    # 1 CREATE VIEW
    lambda src, dst: f"CREATE VIEW {dst} AS SELECT col_a, SUM(col_b) AS total FROM {src} GROUP BY col_a;",
    # 2 MERGE（模拟）
    lambda src, dst: f"MERGE INTO {dst} USING {src} ON {dst}.id = {src}.id WHEN MATCHED THEN UPDATE SET val = {src}.val;",
]

for j in range(1, N_SQL + 1):
    file = OUTPUT_SQL / f"script_{j:02d}.sql"
    # 随机挑 2~4 个表作为依赖
    deps = random.sample(csv_files, k=random.randint(2, 4))
    target = f"derived_{j:02d}"   # 目标表/视图名
    sql_lines = [f"-- target: {target}"]
    for dep in deps:
        sql_lines.append(random.choice(templates)(dep, target))
    file.write_text("\n".join(sql_lines), encoding='utf-8')
print(f"✅ 生成 {N_SQL} 个 SQL 到 {OUTPUT_SQL}")

print("🎉 全部 mock 数据已生成，可直接运行 run_collectors.py！")