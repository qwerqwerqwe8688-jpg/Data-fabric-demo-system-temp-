# backend/collectors/sql_parser.py
import sqlparse
from sqlparse.sql import Identifier, Function
from typing import List, Dict, Set
import re


class SQLParser:
    """SQL解析器，用于自动发现血缘关系"""

    def __init__(self):
        self.table_aliases = {}

    def parse_sql(self, sql: str) -> Dict:
        """解析SQL语句，提取表和字段的血缘关系"""
        try:
            parsed = sqlparse.parse(sql)
            if not parsed:
                return {}

            stmt = parsed[0]

            # 提取所有表引用
            tables = self._extract_tables(stmt)

            # 提取SELECT字段
            select_columns = self._extract_select_columns(stmt)

            # 提取JOIN关系
            join_relationships = self._extract_joins(stmt)

            return {
                "tables": tables,
                "columns": select_columns,
                "joins": join_relationships,
                "operations": self._extract_operations(stmt)
            }
        except Exception as e:
            print(f"SQL解析错误: {e}")
            return {}

    def _extract_tables(self, stmt) -> Set[str]:
        """提取SQL中引用的所有表"""
        tables = set()

        # 从FROM子句提取表
        from_seen = False
        for token in stmt.tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
                from_seen = True
                continue

            if from_seen:
                if isinstance(token, Identifier):
                    table_name = self._get_table_name(token)
                    if table_name:
                        tables.add(table_name)
                elif token.ttype is sqlparse.tokens.Keyword:
                    break

        return tables

    def _extract_select_columns(self, stmt) -> List[Dict]:
        """提取SELECT语句中的字段"""
        columns = []
        select_seen = False

        for token in stmt.tokens:
            if token.ttype is sqlparse.tokens.DML and token.value.upper() == 'SELECT':
                select_seen = True
                continue

            if select_seen:
                if isinstance(token, Identifier):
                    column_info = {
                        "name": token.get_real_name(),
                        "alias": token.get_alias(),
                        "expression": str(token)
                    }
                    columns.append(column_info)
                elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
                    break

        return columns

    def _extract_joins(self, stmt) -> List[Dict]:
        """提取JOIN关系"""
        joins = []
        join_pattern = r'JOIN\s+(\w+)\s+ON\s+(.+?)(?=\s+JOIN|\s+WHERE|$)'

        sql_str = str(stmt).upper()
        matches = re.finditer(join_pattern, sql_str, re.IGNORECASE | re.DOTALL)

        for match in matches:
            join_table = match.group(1)
            join_condition = match.group(2)
            joins.append({
                "table": join_table,
                "condition": join_condition.strip()
            })

        return joins

    def _extract_operations(self, stmt) -> List[str]:
        """提取SQL操作类型"""
        operations = []
        sql_str = str(stmt).upper()

        if 'INSERT' in sql_str:
            operations.append('INSERT')
        if 'UPDATE' in sql_str:
            operations.append('UPDATE')
        if 'CREATE TABLE' in sql_str:
            operations.append('CREATE_TABLE')
        if 'SELECT' in sql_str:
            operations.append('SELECT')

        return operations

    def _get_table_name(self, identifier):
        """从标识符中提取表名"""
        if hasattr(identifier, 'get_real_name'):
            return identifier.get_real_name()
        return str(identifier)


# 使用示例
if __name__ == "__main__":
    parser = SQLParser()

    sample_sql = """
    SELECT 
        c.customer_name,
        p.product_name,
        SUM(s.quantity) as total_quantity
    FROM sales s
    JOIN customers c ON s.customer_id = c.id
    JOIN products p ON s.product_id = p.id
    WHERE s.sale_date >= '2024-01-01'
    GROUP BY c.customer_name, p.product_name
    """

    result = parser.parse_sql(sample_sql)
    print("解析结果:", result)