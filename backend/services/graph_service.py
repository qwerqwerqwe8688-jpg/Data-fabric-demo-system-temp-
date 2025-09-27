# services/graph_service.py
from neo4j import GraphDatabase
from backend.models.metadata import *


class GraphService:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_asset(self, asset: DataAsset):
        """
        创建资产节点，并自动补 :Column 标签（当 type=='column' 时）
        保持与现有代码 100% 兼容
        """
        with self.driver.session() as session:
            # 1. 基础 DataAsset 节点必须存在
            query = """
            MERGE (a:DataAsset {id: $id})
            SET a.name = $name,
                a.type = $type,
                a.description = $description,
                a.owner = $owner,
                a.tags = $tags,
                a.created_time = $created_time,
                a.updated_time = $updated_time
            """
            # 2. 如果是列，额外打上 :Column 标签
            if asset.type == "column":
                query += " SET a:Column"
            query += " RETURN a"

            session.run(query,
                        id=asset.id,
                        name=asset.name,
                        type=asset.type,
                        description=asset.description or "",
                        owner=asset.owner or "",
                        tags=asset.tags or [],
                        created_time=asset.created_time,
                        updated_time=asset.updated_time)

    def create_lineage(self, source_id: str, target_id: str, relationship: str):
        with self.driver.session() as session:
            query = """
            MATCH (source:DataAsset {id: $source_id})
            MATCH (target:DataAsset {id: $target_id})
            MERGE (source)-[r:LINEAGE {type: $relationship}]->(target)
            RETURN r
            """
            session.run(query,
                        source_id=source_id,
                        target_id=target_id,
                        relationship=relationship)

    def search_assets(self, query: str, asset_type: str = None):
        try:
            with self.driver.session() as session:
                cypher_query = """
                MATCH (a:DataAsset)
                WHERE a.name CONTAINS $query OR a.description CONTAINS $query
                """
                params = {"query": query}

                if asset_type:
                    cypher_query += " AND a.type = $asset_type"
                    params["asset_type"] = asset_type

                cypher_query += " RETURN a LIMIT 50"

                # 修改这一行：直接传递 params 字典，而不是使用 **params
                result = session.run(cypher_query, parameters=params)

                assets = []
                for record in result:
                    asset_data = record["a"]
                    assets.append({
                        "id": asset_data["id"],
                        "name": asset_data["name"],
                        "type": asset_data["type"],
                        "description": asset_data.get("description", ""),
                        "owner": asset_data.get("owner", ""),
                        "tags": asset_data.get("tags", [])
                    })
                return assets
        except Exception as e:
            print(f"搜索出错: {e}")
            raise e

    # 在 graph_service.py 中修改 get_lineage 方法
    def get_lineage(self, asset_id: str, depth: int = 3):
        with self.driver.session() as session:
            query = """
            MATCH (start:DataAsset {id: $asset_id})
            OPTIONAL MATCH up = (otherUp)-[:DERIVED_FROM|LINEAGE]->(start)
            OPTIONAL MATCH down = (start)-[:DERIVED_FROM|LINEAGE]->(otherDown)
            RETURN start, 
                   [x IN collect(distinct otherUp) WHERE x IS NOT NULL] as ups, 
                   [x IN collect(distinct otherDown) WHERE x IS NOT NULL] as downs
            """
            result = session.run(query, asset_id=asset_id)
            return self._parse_lineage_result(result)

    # 确保 _parse_lineage_result 方法存在且正确
    def _parse_lineage_result(self, result):
        nodes, edges = {}, []

        for record in result:
            # 添加起始节点
            start_node = record["start"]
            nodes[start_node["id"]] = {
                "id": start_node["id"],
                "name": start_node["name"],
                "type": start_node["type"]
            }

            # 处理上游节点
            for up_node in record["ups"] or []:
                nodes[up_node["id"]] = {
                    "id": up_node["id"],
                    "name": up_node["name"],
                    "type": up_node["type"]
                }
                edges.append({
                    "source": up_node["id"],
                    "target": start_node["id"],
                    "relationship": "DERIVED_FROM"
                })

            # 处理下游节点
            for down_node in record["downs"] or []:
                nodes[down_node["id"]] = {
                    "id": down_node["id"],
                    "name": down_node["name"],
                    "type": down_node["type"]
                }
                edges.append({
                    "source": start_node["id"],
                    "target": down_node["id"],
                    "relationship": "DERIVED_FROM"
                })

        return {"nodes": list(nodes.values()), "edges": edges}