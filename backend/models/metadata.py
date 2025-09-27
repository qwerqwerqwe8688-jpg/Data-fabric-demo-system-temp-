# models/metadata.py
from typing import List, Optional
from pydantic import BaseModel

class DataAsset(BaseModel):
    id: str
    name: str
    type: str  # table, column, file, api, etc.
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

class LineageEdge(BaseModel):
    source_id: str
    target_id: str
    relationship: str  # UPSTREAM, DOWNSTREAM, DERIVED_FROM, etc.