import React, { useState } from 'react';
import Search from './components/Search';
import LineageGraph from './components/LineageGraph';
import './App.css';

function App() {
  const [selectedAssetId, setSelectedAssetId] = useState(null);

  // 当在搜索结果中点击资产时显示血缘图
  const handleAssetSelect = (assetId) => {
    setSelectedAssetId(assetId);
  };

  return (
    <div className="App">
      <h1>数据编织原型系统</h1>
      <Search onAssetSelect={handleAssetSelect} />

      {selectedAssetId && (
        <div className="lineage-section">
          <h2>数据血缘关系</h2>
          <LineageGraph assetId={selectedAssetId} />
        </div>
      )}
    </div>
  );
}

export default App;