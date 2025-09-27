# collectors/base_collector.py
from abc import ABC, abstractmethod
from typing import List
from backend.models.metadata import DataAsset, Table, Column
from sqlalchemy import create_engine, inspect
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime


class BaseMetadataCollector(ABC):
    @abstractmethod
    def collect_metadata(self) -> List[DataAsset]:
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        pass


class MySQLCollector(BaseMetadataCollector):
    def __init__(self, host, port, user, password, database=None):
        self.connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database if database else ''}"
        self.engine = create_engine(self.connection_string)

    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                return True
        except Exception:
            return False

    def collect_metadata(self) -> List[Table]:
        inspector = inspect(self.engine)
        tables = []
        current_time = datetime.now().isoformat()

        # 获取数据库列表
        databases = inspector.get_schema_names()

        for schema in databases:
            for table_name in inspector.get_table_names(schema=schema):
                # 创建表资产
                table = Table(
                    id=f"mysql.{schema}.{table_name}",
                    name=table_name,
                    type="table",
                    description=f"MySQL table {schema}.{table_name}",
                    database="mysql",
                    schema=schema,
                    created_time=current_time,
                    updated_time=current_time,
                    columns=[]
                )

                # 收集列信息
                columns = []
                for column_info in inspector.get_columns(table_name, schema=schema):
                    col = Column(
                        id=f"mysql.{schema}.{table_name}.{column_info['name']}",
                        name=column_info['name'],
                        type="column",
                        data_type=str(column_info['type']),
                        description=f"Column {column_info['name']} in table {table_name}",
                        created_time=current_time,
                        updated_time=current_time
                    )
                    columns.append(col)

                table.columns = columns
                tables.append(table)

        return tables


class FileCollector(BaseMetadataCollector):
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)

    def test_connection(self) -> bool:
        return self.base_path.exists()

    def collect_metadata(self) -> List[DataAsset]:
        assets = []
        current_time = datetime.now().isoformat()

        # 创建示例文件数据（实际使用时可以扫描真实目录）
        sample_files = [
            {
                "name": "sales_data.csv",
                "type": "file",
                "description": "销售数据文件",
                "columns": ["date", "product", "quantity", "revenue"]
            },
            {
                "name": "customer_info.csv",
                "type": "file",
                "description": "客户信息文件",
                "columns": ["customer_id", "name", "email", "region"]
            }
        ]

        for file_info in sample_files:
            # 创建文件资产
            file_asset = DataAsset(
                id=f"file://{file_info['name']}",
                name=file_info['name'],
                type="file",
                description=file_info['description'],
                created_time=current_time,
                updated_time=current_time
            )
            assets.append(file_asset)

            # 为每个列创建资产
            for col_name in file_info['columns']:
                col_asset = Column(
                    id=f"file://{file_info['name']}#{col_name}",
                    name=col_name,
                    type="column",
                    data_type="string",  # CSV文件默认字符串类型
                    description=f"Column {col_name} in file {file_info['name']}",
                    created_time=current_time,
                    updated_time=current_time
                )
                assets.append(col_asset)

        return assets