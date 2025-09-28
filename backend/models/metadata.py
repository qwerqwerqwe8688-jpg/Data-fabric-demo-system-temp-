# backend/models/metadata.py
from typing import List, Optional, Dict, Any
from pydantic import BaseModel

class DataAsset(BaseModel):
    id: str
    name: str
    type: str  # table, column, file, api, row, etc.
    description: Optional[str]
    owner: Optional[str]
    tags: List[str] = []
    created_time: str
    updated_time: str

class Table(DataAsset):
    database: str
    schema: str
    columns: List['Column']
    row_count: Optional[int]

class Column(DataAsset):
    data_type: str
    is_pii: bool = False
    is_primary_key: bool = False

class DataRow(DataAsset):
    """新增：数据行资产"""
    table_id: str  # 所属表的ID
    row_hash: str  # 行内容哈希，用于唯一标识
    row_data: Dict[str, Any]  # 行数据内容
    row_index: int  # 行索引位置

class LineageEdge(BaseModel):
    source_id: str
    target_id: str
    relationship: str  # UPSTREAM, DOWNSTREAM, DERIVED_FROM, TRANSFORMED_FROM, etc.
    transformation: Optional[str]  # 转换描述

class Sheet(DataAsset):
    """新增：Excel工作表资产"""
    file_id: str  # 所属文件的ID
    sheet_name: str
    row_count: int
    column_count: int

class Database(DataAsset):
    """新增：数据库资产"""
    file_path: str
    table_count: int
    connection_string: str