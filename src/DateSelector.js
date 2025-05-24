import React from 'react';

function DateSelector({ 
  previousDate, 
  setPreviousDate, 
  currentDate, 
  setCurrentDate, 
  onFetchData, 
  isLoading 
}) {
  return (
    <div className="date-selector">
      <h2>Select Dates for Analysis</h2>
      <div>
        <label htmlFor="previous-date">Previous Trading Date: </label>
        <input 
          type="date" 
          id="previous-date" 
          value={previousDate}
          onChange={(e) => setPreviousDate(e.target.value)}
          disabled={isLoading}
        />
      </div>
      <div style={{ marginTop: '10px' }}>
        <label htmlFor="current-date">Current Trading Date (for 9:20 AM data): </label>
        <input 
          type="date" 
          id="current-date" 
          value={currentDate}
          onChange={(e) => setCurrentDate(e.target.value)}
          disabled={isLoading}
        />
      </div>
      <button 
        onClick={onFetchData} 
        disabled={isLoading || !previousDate || !currentDate}
        style={{ marginTop: '15px', padding: '10px 15px' }}
      >
        {isLoading ? 'Fetching...' : 'Fetch Stock Analysis'}
      </button>
    </div>
  );
}

export default DateSelector;
