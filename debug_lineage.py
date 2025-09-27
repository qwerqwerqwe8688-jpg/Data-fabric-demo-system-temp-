# debug_lineage.py
from neo4j import GraphDatabase


def debug_lineage(asset_id):
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

    with driver.session() as session:
        # 检查资产是否存在
        result = session.run("MATCH (a:DataAsset {id: $asset_id}) RETURN a", asset_id=asset_id)
        asset = result.single()
        if not asset:
            print(f"❌ 资产不存在: {asset_id}")
            return

        print(f"✅ 找到资产: {asset['a']['name']} ({asset['a']['type']})")

        # 检查 DERIVED_FROM 关系
        result = session.run("""
        MATCH (a:DataAsset {id: $asset_id})
        OPTIONAL MATCH (a)-[r:DERIVED_FROM]->(target)
        OPTIONAL MATCH (source)-[r2:DERIVED_FROM]->(a)
        RETURN a, collect(distinct target) as targets, collect(distinct source) as sources
        """, asset_id=asset_id)

        record = result.single()
        if record:
            print(f"🔗 下游关系: {len(record['targets'])} 个")
            print(f"🔗 上游关系: {len(record['sources'])} 个")

            for target in record['targets']:
                if target:
                    print(f"  → {target['name']} ({target['type']})")

            for source in record['sources']:
                if source:
                    print(f"  ← {source['name']} ({source['type']})")

        # 检查 LINEAGE 关系
        result = session.run("""
        MATCH (a:DataAsset {id: $asset_id})
        OPTIONAL MATCH (a)-[r:LINEAGE]->(target)
        OPTIONAL MATCH (source)-[r2:LINEAGE]->(a)
        RETURN a, collect(distinct target) as targets, collect(distinct source) as sources
        """, asset_id=asset_id)

        record = result.single()
        if record:
            print(f"🔗 LINEAGE下游关系: {len(record['targets'])} 个")
            print(f"🔗 LINEAGE上游关系: {len(record['sources'])} 个")

    driver.close()


if __name__ == "__main__":
    debug_lineage("file.table_15.col_d")