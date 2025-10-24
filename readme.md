# Python 3.9

## 启动基础设施
    docker-compose up -d neo4j elasticsearch

## 启动后端服务
    uvicorn main:app --reload --host 0.0.0.0 --port 8000

## 管理员模式进入命令行(路径改成项目对应绝对路径)
    cd /d E:\py_temp_project1\frontend

## 命令行中运行前端
    npm start

## 采集并处理真实元数据
    python backend/scripts/run_collectors.py

访问localhost:3000进入系统主搜索页面

访问localhost:7474进入neo4j图数据库页面


## 系统核心功能
数据发现层：
多源元数据采集
MySQL数据库表结构采集
文件系统元数据采集（CSV、Excel、SQLite）
支持表、列、行级元数据采集
自动识别数据类型和结构

血缘发现层：
智能血缘发现
基于SQL解析的字段级血缘关系
基于名称相似性的血缘匹配
行级数据相似性分析
数据专业血缘分析

图数据管理层：
图数据库存储
Neo4j图数据库集成
支持复杂血缘关系查询

业务逻辑层：
可视化数据资产关系
数据治理与质量
敏感数据标记
数据质量检查

应用层：
前后端完整系统
FastAPI服务
React前端界面
ECharts血缘关系可视化


## 技术特性
容器化部署：Docker Compose一键启动
模块化设计：可扩展的采集器架构
实时处理：采集后立即写入图数据库
错误恢复：完善的异常处理和日志记录

## 未来优化方向
大规模数据采集性能提升
图数据库查询优化
内存管理和资源优化
更多数据源支持（API、NoSQL、数据湖）
血缘关系版本管理
数据变更追踪
更丰富的可视化选项
交互式血缘探索
资产详情面板优化