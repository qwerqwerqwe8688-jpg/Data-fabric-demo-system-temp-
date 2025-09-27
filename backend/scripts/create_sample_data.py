import sys
import os

# 添加项目根目录到 Python 路径
current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_script_dir, '..', '..'))
sys.path.insert(0, project_root)

try:
    from backend.services.graph_service import GraphService
    from backend.models.metadata import DataAsset, LineageEdge
    print("导入成功！")
except ImportError as e:
    print(f"导入失败: {e}")
    print(f"当前 Python 路径: {sys.path}")
    sys.exit(1)


def clear_existing_data(graph_service):
    """清空现有数据"""
    try:
        with graph_service.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("✅ 已清空现有数据")
    except Exception as e:
        print(f"⚠️ 清空数据时出错: {e}")

def create_sample_data():
    graph_service = GraphService("bolt://localhost:7687", "neo4j", "password")

    # 清空现有数据
    clear_existing_data(graph_service)

    # 创建示例数据资产
    sample_assets = [
        DataAsset(
            id="table.sales.fact_sales",
            name="fact_sales",
            type="table",
            description="销售事实表",
            owner="BI团队",
            tags=["sales", "fact", "business"],
            created_time="2024-01-01T00:00:00",
            updated_time="2024-01-01T00:00:00"
        ),
        DataAsset(
            id="table.sales.dim_customer",
            name="dim_customer",
            type="table",
            description="客户维度表",
            owner="BI团队",
            tags=["customer", "dimension"],
            created_time="2024-01-01T00:00:00",
            updated_time="2024-01-01T00:00:00"
        ),
        DataAsset(
            id="table.sales.dim_product",
            name="dim_product",
            type="table",
            description="产品维度表",
            owner="BI团队",
            tags=["product", "dimension"],
            created_time="2024-01-01T00:00:00",
            updated_time="2024-01-01T00:00:00"
        )
    ]

    # 创建血缘关系
    lineage_edges = [
        LineageEdge(source_id="table.sales.dim_customer", target_id="table.sales.fact_sales", relationship="JOIN"),
        LineageEdge(source_id="table.sales.dim_product", target_id="table.sales.fact_sales", relationship="JOIN"),
        LineageEdge(source_id="file://sales_data.csv", target_id="table.sales.fact_sales", relationship="ETL"),
        LineageEdge(source_id="file://customer_info.csv", target_id="table.sales.dim_customer", relationship="ETL")
    ]

    # 插入数据
    for asset in sample_assets:
        graph_service.create_asset(asset)
        print(f"Created asset: {asset.name}")

    for edge in lineage_edges:
        graph_service.create_lineage(edge.source_id, edge.target_id, edge.relationship)
        print(f"Created lineage: {edge.source_id} -> {edge.target_id}")

if __name__ == "__main__":
    create_sample_data()