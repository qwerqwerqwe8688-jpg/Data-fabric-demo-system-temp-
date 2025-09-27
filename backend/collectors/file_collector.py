# backend/collectors/file_collector.py
import pandas as pd
from pathlib import Path
from typing import List
import logging
from datetime import datetime
import re  # 新增
from backend.models.metadata import DataAsset, Column
from .base_collector import BaseMetadataCollector



class FileCollector(BaseMetadataCollector):
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)

    def test_connection(self) -> bool:
        return self.base_path.exists()

    def _make_safe_id(self, raw: str) -> str:
        """将任意路径/字段名映射为 URL-safe 字符串"""
        return re.sub(r'[^0-9A-Za-z._-]', '_', str(raw))

    def collect_metadata(self) -> List[DataAsset]:
        assets: List[DataAsset] = []
        now = datetime.now().isoformat()

        if not self.base_path.exists():
            logging.warning(f"Base path does not exist: {self.base_path}")
            return assets

        for pattern in ["*.csv", "*.txt", "*.json", "*.parquet"]:
            for file_path in self.base_path.rglob(pattern):
                columns = self.get_csv_columns(file_path)
                if not columns:
                    continue

                # 1. 文件资产
                safe_file_id = self._make_safe_id(file_path.stem)
                file_asset = DataAsset(
                    id=f"file.{safe_file_id}",
                    name=file_path.name,
                    type="file",
                    description=f"数据文件: {file_path}",
                    owner="文件采集器",
                    tags=[pattern.replace('*.', ''), "数据文件"],
                    created_time=now,
                    updated_time=now
                )
                assets.append(file_asset)

                # 2. 列资产
                for col_name in columns:
                    safe_col_id = self._make_safe_id(col_name)
                    col_asset = Column(
                        id=f"file.{safe_file_id}.{safe_col_id}",
                        name=col_name,
                        type="column",
                        data_type="string",
                        description=f"列: {col_name} in {file_path.name}",
                        owner="文件采集器",
                        tags=["column", "数据列"],
                        created_time=now,
                        updated_time=now
                    )
                    assets.append(col_asset)

        # 3. 立即写入图数据库
        from backend.services.graph_service import GraphService
        gs = GraphService("bolt://localhost:7687", "neo4j", "password")
        for ast in assets:
            gs.create_asset(ast)
        gs.close()
        logging.info(f"✅ FileCollector 已写入 {len(assets)} 个资产（含列）")
        return assets

    def detect_encoding(self, file_path: Path) -> str:
        """检测文件编码"""
        for enc in ('utf-8-sig', 'gbk', 'latin1'):
            try:
                with file_path.open('r', encoding=enc) as f:
                    f.read(1024)
                return enc
            except UnicodeDecodeError:
                continue
        return 'utf-8'

    def get_csv_columns(self, file_path: Path) -> List[str]:
        """安全读取 CSV 头"""
        enc = self.detect_encoding(file_path)
        try:
            df = pd.read_csv(file_path, encoding=enc, nrows=0)
            return list(df.columns)
        except Exception as e:
            logging.warning(f"无法读取列信息 {file_path}: {e}")
            return []