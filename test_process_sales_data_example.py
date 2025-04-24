import pandas as pd
import numpy as np
from technical_interview_solution import process_sales_batch

def test_process_sales_batch():
    """Test the process_sales_batch function with sample data."""
    sample_data = pd.DataFrame({
        'portfolio_id': [1, 1, 2, 2, 1],
        'asset_type': ['solar', 'solar', 'wind', 'wind', 'solar'],
        'MWh': [100, 150, 200, 300, 50],
        'price': [50, 55, 45, 40, 52],
        'date': pd.date_range(start='2023-01-01', periods=5),
        'year_week': ['2023-01', '2023-01', '2023-01', '2023-01', '2023-01']
    })
    
    result = process_sales_batch(sample_data)
    
    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == set(['portfolio_id', 'asset_type', 'MWh', 'price', 
                                       'sales_amount', 'transaction_count', 'year_week'])
    
    # Verify the aggregation logic
    # For portfolio_id=1, asset_type='solar' there should be 3 transactions
    solar_row = result[(result['portfolio_id'] == 1) & (result['asset_type'] == 'solar')]
    assert len(solar_row) == 1
    assert solar_row['transaction_count'].values[0] == 3
    assert solar_row['MWh'].values[0] == 300  # 100 + 150 + 50
    assert abs(solar_row['price'].values[0] - np.mean([50, 55, 52])) < 0.001
    assert abs(solar_row['sales_amount'].values[0] - (100*50 + 150*55 + 50*52)) < 0.001
    
    # For portfolio_id=2, asset_type='wind' there should be 2 transactions
    wind_row = result[(result['portfolio_id'] == 2) & (result['asset_type'] == 'wind')]
    assert len(wind_row) == 1
    assert wind_row['transaction_count'].values[0] == 2
    assert wind_row['MWh'].values[0] == 500  # 200 + 300
    assert abs(wind_row['price'].values[0] - np.mean([45, 40])) < 0.001
    assert abs(wind_row['sales_amount'].values[0] - (200*45 + 300*40)) < 0.001
    
    # Verify the total number of rows in the result
    assert len(result) == 2  # One for each unique portfolio_id and asset_type combination