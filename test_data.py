import pandas as pd
import numpy as np
from typing import List, Dict, Any

def create_sample_data():
    """
    Create three sample DataFrames to simulate different data sources.
    In a real scenario, these would be much larger datasets.
    """
    hours_in_four_weeks = 4 * 7 * 24
    
    sales_data = pd.DataFrame({
        'date': pd.date_range(start='2023-01-01', periods=hours_in_four_weeks, freq='h'),
        'portfolio_id': np.random.choice(['A001', 'B002', 'C003', 'D004'], size=hours_in_four_weeks),
        'asset_type': np.random.choice(['Wind', 'Solar', 'Gas'], size=hours_in_four_weeks),
        'MWh': np.random.randint(1, 50, size=hours_in_four_weeks),
        'price': np.random.uniform(10, 200, size=hours_in_four_weeks).round(2),
    })
    
    # Data source 2: Asset Data
    asset_data = pd.DataFrame({
        'portfolio_id': np.random.choice(['A001', 'B002', 'C003', 'D004'], size=12),
        'geography': np.random.choice(['North', 'South', 'East', 'West'], size=12),
        'asset_id': ['Asset_{:02d}'.format(i) for i in range(1, 13)],
        'ISO': np.random.choice(['North', 'South', 'East', 'West'], size=12),
        'operational_date': pd.to_datetime(
            np.random.choice(
                pd.date_range(
                    start=pd.Timestamp.now() - pd.DateOffset(years=10),
                    end=pd.Timestamp.now()
                ),
                size=12,
                replace=False
            )
        ),
    })
    asset_data['operational_date'] = asset_data['operational_date'].dt.date
    
    # Add timezone information based on geography
    timezone_map = {
        'North': 'US/Eastern',
        'South': 'US/Central',
        'East': 'US/Eastern',
        'West': 'US/Pacific'
    }
    asset_data['timezone'] = asset_data['geography'].map(timezone_map)
    
    return sales_data, asset_data

def get_weekly_sales_data(sales_data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Simulates the process of getting weekly sales data.
    
    Parameters:
    -----------
    sales_data : pd.DataFrame
        Raw sales transaction data with date column in hourly format
        
    Returns:
    --------
    Dict[str, pd.DataFrame]
        Dictionary with keys as week identifiers (YYYY-WW) and values as processed DataFrames
    """

    sales_data['year_week'] = sales_data['date'].dt.strftime('%Y-%U')
    weeks = sales_data['year_week'].unique()
    
    weekly_results = {}
    for week in weeks:
        print(f"Fetching data for week: {week}")
        
        weekly_data = sales_data[sales_data['year_week'] == week].copy()

        weekly_results[week] = weekly_data
    
    return weekly_results
