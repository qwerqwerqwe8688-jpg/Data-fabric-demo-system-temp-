# scripts/run_collectors.py
import sys
import os
from pathlib import Path
from datetime import datetime

# 添加项目根目录到 Python 路径
current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = Path(os.path.abspath(os.path.join(current_script_dir, '..', '..')))
sys.path.insert(0, str(project_root))

try:
    from backend.services.graph_service import GraphService
    from backend.models.metadata import DataAsset, Column
    from backend.collectors.file_collector import FileCollector
    from backend.services.lineage_discovery import discover_lineage_auto, AutoLineageService
    from backend.services.policy_engine import PolicyEngine
    import logging

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


def collect_file_metadata_with_fallback(graph_service):
    # 增强路径配置，确保包含SQLite数据库文件
    possible_paths = [
        str(project_root / "data"),
        str(project_root / "sql"),
    ]

    # 去重并过滤存在的路径
    unique_paths = []
    for path in possible_paths:
        path_obj = Path(path)
        if path_obj.exists() and path not in unique_paths:
            unique_paths.append(path)

    print(f"将尝试以下路径: {unique_paths}")

    for path in unique_paths:
        try:
            print(f"尝试路径: {path}")
            file_collector = FileCollector(path, sample_rows=50)
            if file_collector.test_connection():
                print(f"✅ 找到文件路径: {path}")

                # 检查该路径下是否有SQLite文件
                sqlite_files = list(Path(path).rglob("*.db")) + list(Path(path).rglob("*.sqlite")) + list(
                    Path(path).rglob("*.sqlite3"))
                print(f"在该路径下找到 {len(sqlite_files)} 个SQLite文件")
                for sqlite_file in sqlite_files:
                    print(f"  - {sqlite_file}")

                assets = file_collector.collect_metadata()
                print(f"采集到 {len(assets)} 个资产")

                # 统计各类资产数量
                asset_types = {}
                for asset in assets:
                    asset_types[asset.type] = asset_types.get(asset.type, 0) + 1

                for asset_type, count in asset_types.items():
                    print(f"  - {asset_type}: {count}个")

                print(f"✅ 成功采集了 {len(assets)} 个文件资产")
                return True
            else:
                print(f"❌ 路径不存在或无法连接: {path}")
        except Exception as e:
            print(f"⚠️ 路径 {path} 采集失败: {e}")
    return False


def run_lineage_discovery(graph_service):
    """执行血缘发现"""
    try:
        print("开始血缘发现...")

        # 设置数据路径
        csv_root = project_root / "data"
        sql_dir = project_root / "sql"

        # 确保目录存在
        csv_root.mkdir(exist_ok=True)
        sql_dir.mkdir(exist_ok=True)

        print(f"CSV路径: {csv_root}")
        print(f"SQL路径: {sql_dir}")

        # 检查SQL目录内容
        if sql_dir.exists():
            sql_files = list(sql_dir.glob("*.sql")) + list(sql_dir.glob("*.db"))
            print(f"SQL目录包含 {len(sql_files)} 个文件: {[f.name for f in sql_files]}")

        # 使用AutoLineageService进行血缘发现
        from backend.services.lineage_discovery import AutoLineageService
        lineage_service = AutoLineageService(graph_service)

        # 执行所有血缘发现方法
        lineage_service.discover_all(csv_root, sql_dir)

        print("✅ 血缘发现完成")

    except Exception as e:
        print(f"❌ 血缘发现失败: {e}")
        import traceback
        traceback.print_exc()


def main():
    # 初始化图数据库服务
    graph_service = GraphService("bolt://localhost:7687", "neo4j", "password")

    print("开始元数据采集...")

    # 清空现有数据
    clear_existing_data(graph_service)

    # 采集文件元数据（带备用方案）
    file_success = collect_file_metadata_with_fallback(graph_service)

    if file_success:
        print("✅ 元数据采集完成")

        # 验证数据
        print("\n验证采集结果...")
        try:
            with graph_service.driver.session() as session:
                result = session.run("MATCH (a:DataAsset) RETURN count(a) as count")
                count = result.single()["count"]
                print(f"✅ 数据库中共有 {count} 个资产")

                # 显示资产类型分布
                result = session.run("MATCH (a:DataAsset) RETURN a.type as type, count(a) as count")
                print("资产类型分布:")
                for record in result:
                    print(f"  - {record['type']}: {record['count']}个")

        except Exception as e:
            print(f"❌ 验证失败: {e}")

        # 执行血缘发现
        run_lineage_discovery(graph_service)

        # 验证血缘关系
        print("\n验证血缘关系...")
        try:
            with graph_service.driver.session() as session:
                # 检查血缘关系数量
                result = session.run("MATCH ()-[r:DERIVED_FROM]->() RETURN count(r) as relationship_count")
                rel_count = result.single()["relationship_count"]
                print(f"✅ 发现 {rel_count} 个血缘关系")

                # 显示部分血缘关系
                result = session.run("""
                    MATCH (src)-[r:DERIVED_FROM]->(dst) 
                    RETURN src.id as source, dst.id as target, type(r) as relationship 
                    LIMIT 10
                """)
                print("血缘关系示例:")
                for record in result:
                    print(f"  - {record['source']} -> {record['target']} ({record['relationship']})")

        except Exception as e:
            print(f"❌ 血缘关系验证失败: {e}")

    else:
        print("❌ 元数据采集失败")

    # 策略分析
    print("开始策略分析...")
    policy_engine = PolicyEngine()

    # 对现有资产进行策略分析
    with graph_service.driver.session() as session:
        result = session.run("MATCH (a:DataAsset) RETURN a LIMIT 5")
        for record in result:
            asset_data = dict(record["a"])
            policy = policy_engine.analyze_asset(asset_data)
            print(f"资产 {asset_data.get('name', 'Unknown')} 策略分析: {policy['sensitivity_level']}")

    print("🎉 所有采集和分析任务完成！")
    graph_service.close()


if __name__ == "__main__":
    main()