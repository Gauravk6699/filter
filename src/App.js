import React, { useState } from 'react';
import DateSelector from './DateSelector';
import StockResults from './StockResults';
import './App.css'; // Optional: for basic app styling

function App() {
  const [previousDate, setPreviousDate] = useState('');
  const [currentDate, setCurrentDate] = useState('');
  const [filteredStocks, setFilteredStocks] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [processedCount, setProcessedCount] = useState(0);
  const [apiErrorsList, setApiErrorsList] = useState([]);

  const handleFetchData = async () => {
    if (!previousDate || !currentDate) {
      setError("Please select both previous and current dates.");
      return;
    }
    setIsLoading(true);
    setError(null);
    setFilteredStocks([]);
    setProcessedCount(0);
    setApiErrorsList([]);

    const API_BASE_URL = "http://localhost:5001"; // Assuming Flask backend runs on this port
    const url = `${API_BASE_URL}/api/analyze_stocks?previous_date=${previousDate}&current_date=${currentDate}`;

    try {
      const response = await fetch(url);
      if (!response.ok) {
        // Try to parse error response from Flask if possible
        const errorData = await response.json().catch(() => null);
        throw new Error(errorData?.error || `HTTP error! Status: ${response.status}`);
      }
      const data = await response.json();
      
      setFilteredStocks(data.filtered_stocks || []);
      setProcessedCount(data.processed_stocks_count || 0);
      setApiErrorsList(data.errors_list || []);
      if (data.filtered_stocks && data.filtered_stocks.length === 0 && !data.errors_list?.length) {
        // setError("No stocks met the filter criteria or no data returned."); 
        // setError can be used, or StockResults can handle empty filteredStocks list
      }

    } catch (err) {
      console.error("Fetch error:", err);
      setError(err.message || "Failed to fetch data. Ensure the backend server is running.");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="App">
      <header className="App-header">
        <h1>F&O Stock Analyzer</h1>
      </header>
      <main>
        <DateSelector
          previousDate={previousDate}
          setPreviousDate={setPreviousDate}
          currentDate={currentDate}
          setCurrentDate={setCurrentDate}
          onFetchData={handleFetchData}
          isLoading={isLoading}
        />
        <StockResults
          filteredStocks={filteredStocks}
          isLoading={isLoading}
          error={error}
          processedCount={processedCount}
          apiErrorsList={apiErrorsList}
        />
      </main>
    </div>
  );
}

export default App;
