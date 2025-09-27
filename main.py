# main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from backend.services.graph_service import GraphService
from backend.models.metadata import DataAsset, LineageEdge
import os
from backend.services.lineage_discovery import *
from backend.services.policy_engine import PolicyEngine, EnhancedGraphService
from backend.services.data_quality import DataQualityChecker, generate_quality_report
from backend.services.lineage_discovery import get_lineage_graph_for_frontend
import pandas as pd
from pathlib import Path
from urllib.parse import unquote


app = FastAPI(title="数据编织原型系统")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 使用环境变量或默认值配置Neo4j连接
neo4j_host = os.getenv("NEO4J_HOST", "localhost")
graph_service = GraphService(f"bolt://{neo4j_host}:7687", "neo4j", "password")


@app.get("/")
async def root():
    return {"message": "数据编织原型系统"}


@app.post("/assets/")
async def create_asset(asset: DataAsset):
    try:
        graph_service.create_asset(asset)
        return {"status": "success", "asset_id": asset.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search/")
async def search_assets(q: str, asset_type: str = None):
    try:
        results = graph_service.search_assets(q, asset_type)
        return {"query": q, "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"搜索失败: {str(e)}")


@app.get("/assets/{asset_id}/lineage")
async def get_asset_lineage(asset_id: str, depth: int = 3):
    try:
        asset_id = unquote(asset_id)
        print(f"🔍 查询血缘关系，资产ID: {asset_id}")

        lineage = graph_service.get_lineage(asset_id, depth)
        print(f"✅ 血缘查询结果: {len(lineage.get('nodes', []))} 个节点, {len(lineage.get('edges', []))} 条边")

        return {"asset_id": asset_id, "lineage": lineage}
    except Exception as e:
        print(f"❌ 获取血缘失败: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"获取血缘失败: {str(e)}")


@app.post("/lineage/")
async def create_lineage(edge: LineageEdge):
    try:
        graph_service.create_lineage(
            edge.source_id,
            edge.target_id,
            edge.relationship
        )
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建血缘失败: {str(e)}")


@app.post("/assets/with-policy")
async def create_asset_with_policy(asset: DataAsset):
    """创建资产并自动应用策略"""
    try:
        policy = graph_service.create_asset_with_policy(asset)
        return {"status": "success", "asset_id": asset.id, "policy": policy}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/lineage/discover-sql")
async def discover_sql_lineage(sql_content: str, script_name: str = "unknown"):
    """从SQL脚本自动发现血缘关系"""
    try:
        discovery_service = AutoLineageService(graph_service.graph_service)
        result = discovery_service.discover_from_sql_script(sql_content, script_name)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"血缘发现失败: {str(e)}")


@app.post("/quality/analyze")
async def analyze_data_quality(asset_id: str, sample_data: dict):
    """分析数据质量"""
    try:
        df = pd.DataFrame(sample_data.get("rows", []))
        report = generate_quality_report(asset_id, df)
        return report
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"质量分析失败: {str(e)}")


@app.get("/policy/analyze/{asset_id}")
async def analyze_asset_policy(asset_id: str):
    """分析资产策略"""
    try:
        policy_engine = PolicyEngine()
        with graph_service.graph_service.driver.session() as session:
            result = session.run("MATCH (a:DataAsset {id: $asset_id}) RETURN a", asset_id=asset_id)
            record = result.single()
            if not record:
                raise HTTPException(status_code=404, detail="资产未找到")
            asset_data = record["a"]
            policy = policy_engine.generate_data_governance_policy(dict(asset_data))
            return policy
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"策略分析失败: {str(e)}")


@app.get("/lineage/graph")
async def lineage_graph():
    data = get_lineage_graph_for_frontend()
    return data if data else []


# -------------------------------------------------------------------------
# 新增：供 run_collectors.py 调用的后台血缘发现入口（零破环）
# -------------------------------------------------------------------------
def run_lineage_discovery():
    """
    由 run_collectors.py 在采集完成后调用，
    立即执行全策略自动血缘发现并写入 DERIVED_FROM。
    """
    from backend.services.lineage_discovery import AutoLineageService
    from pathlib import Path

    csv_root = Path(__file__).resolve().parent.parent / "data"
    sql_dir  = Path(__file__).resolve().parent.parent / "sql"

    svc = AutoLineageService(graph_service)
    print("\n开始自动血缘发现（采集后）...")
    svc.discover_all(csv_root, sql_dir)
    print("✅ 自动血缘发现完成")