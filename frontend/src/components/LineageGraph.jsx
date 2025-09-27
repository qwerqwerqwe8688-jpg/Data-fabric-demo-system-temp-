import React, { useEffect, useState } from 'react';
import ReactECharts from 'echarts-for-react';  // 使用这个而不是直接导入 echarts
import axios from 'axios';

const LineageGraph = ({ assetId }) => {
    const [graphData, setGraphData] = useState(null);
    const [chartOption, setChartOption] = useState({});

    useEffect(() => {
        const fetchLineage = async () => {
            try {
                const response = await axios.get(`/api/assets/${assetId}/lineage`);
                setGraphData(response.data.lineage);
                renderGraph(response.data.lineage);
            } catch (error) {
                console.error('Failed to fetch lineage:', error);
            }
        };

        if (assetId) {
            fetchLineage();
        }
    }, [assetId]);

    const renderGraph = (lineageData) => {
        if (!lineageData || !lineageData.nodes) {
            setChartOption({
                title: {
                    text: '暂无血缘数据',
                    left: 'center',
                    top: 'center'
                }
            });
            return;
        }

        const nodes = lineageData.nodes.map(node => ({
            id: node.id,
            name: node.name,
            symbolSize: 50,
            itemStyle: {
                color: getNodeColor(node.type)
            }
        }));

        const links = lineageData.edges.map(edge => ({
            source: edge.source,
            target: edge.target,
            lineStyle: {
                color: '#aaa'
            }
        }));

        const option = {
            title: {
                text: '数据血缘图',
                left: 'center'
            },
            tooltip: {},
            animationDurationUpdate: 1500,
            animationEasingUpdate: 'quinticInOut',
            series: [{
                type: 'graph',
                layout: 'force',
                data: nodes,
                links: links,
                roam: true,
                focusNodeAdjacency: true,
                label: {
                    show: true,
                    position: 'right',
                    formatter: '{b}'
                },
                lineStyle: {
                    color: 'source',
                    curveness: 0.3
                },
                force: {
                    repulsion: 1000,
                    edgeLength: 200
                }
            }]
        };

        setChartOption(option);
    };

    const getNodeColor = (type) => {
        const colors = {
            'table': '#5470c6',
            'file': '#91cc75',
            'column': '#fac858'
        };
        return colors[type] || '#73c0de';
    };


    return (
        <ReactECharts
            option={chartOption}
            style={{ height: '600px', width: '100%' }}
            opts={{ renderer: 'canvas' }}
        />
    );
};

export default LineageGraph;