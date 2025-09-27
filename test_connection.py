# test_connection.py
from backend.services.graph_service import GraphService


def test_neo4j_connection():
    try:
        # 测试本地连接
        graph_service = GraphService("bolt://localhost:7687", "neo4j", "password")
        with graph_service.driver.session() as session:
            result = session.run("RETURN 1 as test")
            record = result.single()
            print("✅ Neo4j 本地连接测试成功")
        return True
    except Exception as e:
        print(f"❌ Neo4j 本地连接失败: {e}")
        return False


def test_search_function():
    try:
        graph_service = GraphService("bolt://localhost:7687", "neo4j", "password")
        results = graph_service.search_assets("sales")
        print(f"✅ 搜索功能测试成功，找到 {len(results)} 个结果")
        for result in results:
            print(f"  - {result['name']} ({result['type']})")
        return True
    except Exception as e:
        print(f"❌ 搜索功能测试失败: {e}")
        return False


if __name__ == "__main__":
    print("开始测试连接...")

    if test_neo4j_connection():
        test_search_function()

