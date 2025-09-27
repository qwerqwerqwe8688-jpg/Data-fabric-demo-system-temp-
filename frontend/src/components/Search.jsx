import React, { useState } from 'react';
import axios from 'axios';

const Search = ({ onAssetSelect }) => {
    const [query, setQuery] = useState('');
    const [results, setResults] = useState([]);

    const handleSearch = async () => {
        try {
            const response = await axios.get(`/api/search/?q=${query}`);
            setResults(response.data.results);
        } catch (error) {
            console.error('Search failed:', error);
        }
    };

    const handleAssetClick = (assetId) => {
        if (onAssetSelect) {
            onAssetSelect(assetId);
        }
    };

    return (
        <div className="search-container">
            <div className="search-bar">
                <input
                    type="text"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="搜索数据资产..."
                    onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
                />
                <button onClick={handleSearch}>搜索</button>
            </div>

            <div className="results">
                {results.map(asset => (
                    <div
                        key={asset.id}
                        className="asset-card"
                        onClick={() => handleAssetClick(asset.id)}
                        style={{ cursor: 'pointer' }}
                    >
                        <h3>{asset.name}</h3>
                        <p>类型: {asset.type}</p>
                        <p>ID: {asset.id}</p>
                        <p>描述: {asset.description}</p>
                        <p>所有者: {asset.owner}</p>
                        <p>标签: {asset.tags.join(', ')}</p>
                    </div>
                ))}
            </div>
        </div>
    );
};

export default Search;