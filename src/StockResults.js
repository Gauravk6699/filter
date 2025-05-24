import React from 'react';

function StockResults({ 
  filteredStocks, 
  isLoading, 
  error, 
  processedCount, 
  apiErrorsList 
}) {
  if (isLoading) {
    return <p style={{ marginTop: '20px' }}>Loading analysis results...</p>;
  }

  if (error) {
    return <p style={{ marginTop: '20px', color: 'red' }}>Error: {error}</p>;
  }

  return (
    <div className="stock-results" style={{ marginTop: '30px' }}>
      <h3>Analysis Results</h3>
      {processedCount > 0 && <p>Total F&O Stocks Processed: {processedCount}</p>}
      
      {apiErrorsList && apiErrorsList.length > 0 && (
        <div className="api-errors" style={{ marginBottom: '20px', border: '1px solid orange', padding: '10px' }}>
          <h4>API/Processing Errors Encountered:</h4>
          <ul style={{ maxHeight: '150px', overflowY: 'auto', fontSize: '0.9em' }}>
            {apiErrorsList.map((apiError, index) => (
              <li key={index} style={{ color: 'darkorange' }}>{apiError}</li>
            ))}
          </ul>
          <p style={{ fontSize: '0.8em', color: 'gray' }}>
            Note: API errors are expected if the backend's test token date (e.g., 2025-05-17) does not match the selected dates.
            This may result in 'None' for price/OI data and empty filtered results.
            Also, symbol derivation issues or missing instrument keys will be reported here.
          </p>
        </div>
      )}

      {filteredStocks && filteredStocks.length > 0 ? (
        <div>
          <h4>Stocks with >2% Absolute Price Change (9:20 AM vs Prev. Close):</h4>
          <table border="1" cellPadding="5" style={{ width: '100%', maxWidth: '500px', textAlign: 'left' }}>
            <thead>
              <tr>
                <th>Stock Symbol</th>
                <th>Percent Change</th>
              </tr>
            </thead>
            <tbody>
              {filteredStocks.map((stock, index) => (
                <tr key={index}>
                  <td>{stock.symbol}</td>
                  <td>{stock.percent_change.toFixed(2)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        // This message will show if filteredStocks is empty, even if there were API errors for other stocks
        <p>No stocks met the >2% filter criteria based on the available data.</p>
      )}
    </div>
  );
}

export default StockResults;
