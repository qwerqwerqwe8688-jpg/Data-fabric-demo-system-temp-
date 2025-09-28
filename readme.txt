# Python 3.9

# 启动基础设施
docker-compose up -d neo4j elasticsearch

# 启动后端服务
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# 以管理员模式进入命令行(路径需改成项目对应绝对路径)
cd /d E:\py_temp_project1\frontend

# 命令行中运行前端
npm start

# 采集并处理真实元数据
python backend/scripts/run_collectors.py

# 访问localhost:3000进入系统主搜索页面

# 访问localhost:7474进入neo4j图数据库页面