import pandas as pd
import numpy as np
from technical_interview_solution import process_sales_batch

def test_process_sales_batch():
    """Test the process_sales_batch function"""
    # Feel Free to modify the sample data.
    sample_data = pd.DataFrame({
        'portfolio_id': [1, 1, 2, 2, 1],
        'asset_type': ['solar', 'solar', 'wind', 'wind', 'solar'],
        'MWh': [100, 150, 200, 300, 50],
        'price': [50, 55, 45, 40, 52],
        'date': pd.date_range(start='2023-01-01', periods=5),
        'year_week': ['2023-01', '2023-01', '2023-01', '2023-01', '2023-01']
    })
    
    result = process_sales_batch(sample_data)