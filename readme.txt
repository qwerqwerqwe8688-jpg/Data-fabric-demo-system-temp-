# Python 3.9开发

# 启动基础设施
docker-compose up -d neo4j elasticsearch

# 启动后端服务
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# 以管理员模式进入命令行(路径需改成项目对应绝对路径)
cd /d E:\py_temp_project1\frontend

# 命令行中运行前端
npm start

# 运行生成模拟示例资产
python backend/scripts/create_sample_data.py

# 连接测试脚本
python test_connection.py

# 采集真实元数据
python backend/scripts/run_collectors.py