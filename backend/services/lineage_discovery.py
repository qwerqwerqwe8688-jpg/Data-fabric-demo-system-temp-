# backend/services/lineage_discovery.py
"""
方案2完整实现：自动发现时同时写 LINEAGE 边（与 DERIVED_FROM 同构）
"""
import os
import re
import hashlib
from difflib import SequenceMatcher
from typing import Dict, List, Tuple, Set
from pathlib import Path

import pandas as pd
import sqlglot
from neo4j import GraphDatabase

from backend.services.graph_service import GraphDatabase
from backend.services.graph_service import GraphService


# ------------------------------------------------------------------
# 工具函数
# ------------------------------------------------------------------
def _safe_id(raw: str) -> str:
    return re.sub(r'[^0-9A-Za-z._-]', '_', str(raw))

def _is_similar_name(n1: str, n2: str, threshold: float = 0.7) -> bool:
    n1_clean = n1.lower().replace('_col', '').replace('_', '')
    n2_clean = n2.lower().replace('_col', '').replace('_', '')
    return SequenceMatcher(None, n1_clean, n2_clean).ratio() >= threshold

def _column_fingerprint(series: pd.Series, sample: int = 100) -> str:
    valid = series.dropna().astype(str)
    if len(valid) == 0:
        return ""
    sampled = sorted(valid.sample(min(sample, len(valid))))
    return hashlib.sha256("".join(sampled).encode("utf-8")).hexdigest()

def _is_pk_candidate(series: pd.Series, unique_ratio: float = 0.8) -> bool:
    if len(series.dropna()) == 0:
        return False
    return series.nunique() / len(series.dropna()) >= unique_ratio

def _is_fk_candidate(series: pd.Series, pk_series: pd.Series) -> bool:
    fk_vals = set(series.dropna().astype(str))
    pk_vals = set(pk_series.dropna().astype(str))
    if not fk_vals:
        return False
    common_vals = fk_vals.intersection(pk_vals)
    return len(common_vals) > 0 and len(common_vals) / len(fk_vals) >= 0.1


# ------------------------------------------------------------------
# SQL 解析
# ------------------------------------------------------------------
def parse_sql_column_lineage(sql_path: Path) -> Dict[str, Set[str]]:
    lineage: Dict[str, Set[str]] = {}
    try:
        with open(sql_path, encoding="utf-8-sig") as f:
            sql_content = f.read()

        target_match = re.search(r"--\s*target:\s*(\w+)", sql_content, re.I)
        if not target_match:
            return lineage
        target_table = target_match.group(1)

        try:
            parsed = sqlglot.parse(sql_content)
            if not parsed:
                return lineage
        except Exception:
            return _fallback_sql_parsing(sql_content, target_table)

        for stmt in parsed:
            for select in stmt.find_all(sqlglot.expressions.Select):
                for expr in select.expressions:
                    src_tables, src_columns = set(), set()
                    for column in expr.find_all(sqlglot.expressions.Column):
                        if column.table:
                            src_tables.add(column.table)
                        if column.name:
                            src_columns.add(column.name)
                    target_col = expr.alias_or_name if hasattr(expr, 'alias_or_name') else "unknown"

                    for src_table in src_tables:
                        for src_col in src_columns:
                            src_full = f"file.{_safe_id(src_table)}.{_safe_id(src_col)}"
                            tgt_full = f"virtual.{_safe_id(target_table)}.{_safe_id(target_col)}"
                            lineage.setdefault(tgt_full, set()).add(src_full)
        return lineage

    except Exception as e:
        print(f"❌ SQL 解析失败 {sql_path}: {e}")
        return lineage


def _fallback_sql_parsing(sql_content: str, target_table: str) -> Dict[str, Set[str]]:
    lineage = {}
    insert_pattern = r'INSERT\s+INTO\s+(\w+)\s+SELECT\s+(.+?)\s+FROM\s+(\w+)'
    for m in re.finditer(insert_pattern, sql_content, re.I | re.S):
        tgt, cols, src = m.groups()
        for i, col in enumerate(cols.split(',')):
            src_col = f"col_{i}"
            src_full = f"file.{_safe_id(src)}.{_safe_id(src_col)}"
            tgt_full = f"virtual.{_safe_id(tgt)}.{_safe_id(col.strip())}"
            lineage.setdefault(tgt_full, set()).add(src_full)

    view_pattern = r'CREATE\s+VIEW\s+(\w+)\s+AS\s+SELECT\s+(.+?)\s+FROM\s+(\w+)'
    for m in re.finditer(view_pattern, sql_content, re.I | re.S):
        tgt, cols, src = m.groups()
        for col_expr in cols.split(','):
            col_match = re.match(r'(.+?)\s+AS\s+(\w+)', col_expr, re.I)
            if col_match:
                src_col = col_match.group(1).strip().split()[-1]
                tgt_col = col_match.group(2).strip()
                src_full = f"file.{_safe_id(src)}.{_safe_id(src_col)}"
                tgt_full = f"virtual.{_safe_id(tgt)}.{_safe_id(tgt_col)}"
                lineage.setdefault(tgt_full, set()).add(src_full)
    return lineage


# ------------------------------------------------------------------
# AutoLineageService – 方案2：同时写 LINEAGE 边
# ------------------------------------------------------------------
class AutoLineageService:
    def __init__(self, graph_service: GraphService):
        self.gs = graph_service

    # ---------------- 列名相似 ----------------
    def discover_by_name(self):
        print("🔍 开始列名相似性分析...")
        with self.gs.driver.session() as session:
            result = session.run("""
                MATCH (c:Column)
                WHERE c.id STARTS WITH 'file.'
                RETURN c.id as id, c.name as name
            """)
            columns = [(record["id"], record["name"]) for record in result]

            matches = 0
            for i, (id1, name1) in enumerate(columns):
                for j, (id2, name2) in enumerate(columns):
                    if i < j and _is_similar_name(name1, name2):
                        table1, table2 = id1.split('.')[1], id2.split('.')[1]
                        if table1 != table2:
                            session.run("""
                                MATCH (c1:Column {id: $id1}), (c2:Column {id: $id2})
                                MERGE (c1)-[:DERIVED_FROM {method: 'name_similarity'}]->(c2)
                                MERGE (c1)-[:LINEAGE {type: 'DERIVED_FROM'}]->(c2)
                            """, id1=id1, id2=id2)
                            matches += 1
            print(f"✅ 列名相似完成，发现 {matches} 个匹配")

    # ---------------- 主外键 ----------------
    def discover_by_fk(self, csv_root: Path):
        print("🔍 开始主外键分析...")
        tables = {}
        for csv_file in csv_root.glob("*.csv"):
            try:
                df = pd.read_csv(csv_file, encoding="utf-8-sig")
                tables[csv_file.stem] = df
            except Exception as e:
                print(f"⚠️ 读取 {csv_file} 失败: {e}")
                continue

        with self.gs.driver.session() as session:
            result = session.run("""
                MATCH (c:Column)
                WHERE c.id STARTS WITH 'file.'
                RETURN c.id as id, c.name as name
            """)
            col_map = {}
            for rec in result:
                parts = rec["id"].split('.')
                if len(parts) == 3:
                    table, col = parts[1], parts[2]
                    col_map.setdefault(table, {})[col] = rec["id"]

            matches = 0
            for table1, df1 in tables.items():
                for col1 in df1.columns:
                    if table1 not in col_map or col1 not in col_map[table1]:
                        continue
                    s1 = df1[col1]
                    if _is_pk_candidate(s1):
                        for table2, df2 in tables.items():
                            if table1 == table2:
                                continue
                            for col2 in df2.columns:
                                if table2 not in col_map or col2 not in col_map[table2]:
                                    continue
                                if _is_fk_candidate(df2[col2], s1):
                                    fk_id = col_map[table2][col2]
                                    pk_id = col_map[table1][col1]
                                    session.run("""
                                        MATCH (fk:Column {id: $fk}), (pk:Column {id: $pk})
                                        MERGE (fk)-[:DERIVED_FROM {method: 'foreign_key'}]->(pk)
                                        MERGE (fk)-[:LINEAGE {type: 'DERIVED_FROM'}]->(pk)
                                    """, fk=fk_id, pk=pk_id)
                                    matches += 1
            print(f"✅ 主外键分析完成，发现 {matches} 个关系")

    # ---------------- 数据指纹 ----------------
    def discover_by_fingerprint(self, csv_root: Path):
        print("🔍 开始数据指纹分析...")
        fingerprints = {}
        for csv_file in csv_root.glob("*.csv"):
            try:
                df = pd.read_csv(csv_file, encoding="utf-8-sig")
                for col in df.columns:
                    fp = _column_fingerprint(df[col])
                    if fp:
                        full_id = f"file.{_safe_id(csv_file.stem)}.{_safe_id(col)}"
                        fingerprints[full_id] = fp
            except Exception as e:
                print(f"⚠️ 指纹生成失败 {csv_file}: {e}")
                continue

        fp_to_cols = {}
        for cid, fp in fingerprints.items():
            fp_to_cols.setdefault(fp, []).append(cid)

        with self.gs.driver.session() as session:
            matches = 0
            for fp, cols in fp_to_cols.items():
                if len(cols) >= 2:
                    src = cols[0]
                    for tgt in cols[1:]:
                        session.run("""
                            MATCH (s:Column {id: $src}), (t:Column {id: $tgt})
                            MERGE (t)-[:DERIVED_FROM {method: 'fingerprint'}]->(s)
                            MERGE (t)-[:LINEAGE {type: 'DERIVED_FROM'}]->(s)
                        """, src=src, tgt=tgt)
                        matches += 1
            print(f"✅ 数据指纹分析完成，发现 {matches} 个匹配")

    # ---------------- SQL 解析 ----------------
    def discover_by_sql(self, sql_dir: Path):
        print("🔍 开始 SQL 解析分析...")
        if not sql_dir.exists():
            print(f"⚠️ SQL 目录不存在: {sql_dir}")
            return

        sql_files = list(sql_dir.glob("*.sql"))
        if not sql_files:
            print(f"⚠️ 未找到 SQL 文件")
            return

        total = 0
        with self.gs.driver.session() as session:
            for sql_file in sql_files:
                mapping = parse_sql_column_lineage(sql_file)
                for tgt, srcs in mapping.items():
                    for src in srcs:
                        session.run("""
                            MERGE (t:Column:DataAsset {id: $tgt})
                            ON CREATE SET t.name = split($tgt, '.')[2], t.type = 'column'
                            MERGE (s:Column:DataAsset {id: $src})
                            ON CREATE SET s.name = split($src, '.')[2], s.type = 'column'
                            MERGE (t)-[:DERIVED_FROM {method: 'sql_parsing', sql_file: $file}]->(s)
                            MERGE (t)-[:LINEAGE {type: 'DERIVED_FROM'}]->(s)
                        """, tgt=tgt, src=src, file=sql_file.name)
                        total += 1
        print(f"✅ SQL 解析完成，发现 {total} 个血缘关系")

    # ---------------- 一键全量 ----------------
    def discover_all(self, csv_root: Path, sql_dir: Path):
        print("🚀 开始全面血缘发现...")
        csv_root.mkdir(exist_ok=True)
        self.discover_by_name()
        self.discover_by_fk(csv_root)
        self.discover_by_fingerprint(csv_root)
        self.discover_by_sql(sql_dir)
        print("🎉 全面血缘发现完成")


# ------------------------------------------------------------------
# 向后兼容导出
# ------------------------------------------------------------------
def discover_sql_lineage_in_directory(sql_dir: str, graph_service: GraphService):
    AutoLineageService(graph_service).discover_by_sql(Path(sql_dir))
    return {"sql_files_found": len(list(Path(sql_dir).glob("*.sql"))), "successful_discoveries": True}

def discover_lineage_auto(graph_service: GraphService, csv_root: str, sql_dir: str):
    AutoLineageService(graph_service).discover_all(Path(csv_root), Path(sql_dir))

def get_lineage_graph_for_frontend() -> list:
    gs = GraphService("bolt://localhost:7687", "neo4j", "password")
    with gs.driver.session() as session:
        nodes = session.run("""
            MATCH (c:Column)
            RETURN c.id as id, c.name as label, 'column' as group
            LIMIT 100
        """).data()
        edges = session.run("""
            MATCH (src:Column)-[r:LINEAGE]->(dst:Column)
            RETURN src.id as from, dst.id as to, r.type as method
            LIMIT 200
        """).data()
    gs.close()

    result = []
    for node in nodes:
        result.append({"id": node["id"], "label": node["label"], "group": node["group"], "type": "node"})
    for edge in edges:
        result.append({"from": edge["from"], "to": edge["to"], "method": edge.get("method", "unknown"), "type": "edge"})
    return result