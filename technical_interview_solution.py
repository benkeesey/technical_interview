"""
Technical Interview: Data Batching and Transformation Exercise

1. Implement the `process_sales_batch()` function according to the specifications in its docstring.
   This function should process a batch of sales data, performing aggregation and calculations.

2. For both `combine_sales_with_asset_data()` and `create_consolidated_weekly_report()` functions:
   - Explain the purpose of the function and its parameters to us.
   - Write a docstring and type hints for these functions.

3. Create a new function called `run_pipeline()` that:
   - Processes the sales data in appropriate batches (e.g., by week)
   - Combines each processed batch with asset data
   - Generates a consolidated report from all processed weeks
   - Returns the complete results dictionary


4. (Bonus) Implement a simple test using `pytest` for process_sales_batch() to validate its functionality.
"""

import pandas as pd
import numpy as np
from typing import Dict


def process_sales_batch(sales_data: pd.DataFrame) -> pd.DataFrame:
    """
    Process a single weekly batch of sales data.
    
    For each batch:
    1. Group by portfolio_id and asset_type
    2. Calculate total sales amount (MWh * price)
    3. Calculate average price per product
    4. Count number of transactions
    5. Retain the year_week information for each group

    Parameters:
    -----------
    sales_data : pd.DataFrame
        Weekly batch of sales data containing columns:
        - portfolio_id: Identifier for the portfolio
        - asset_type: Type of asset
        - MWh: Sum of Energy volume in MegaWatt hours
        - price: Average Price per MWh
        - sales_amount: Total sales amount (MWh * price)
        - transaction_count: Transaction number of date
        - year_week: Week and year of the transaction

    Returns:
    --------
    pd.DataFrame: A DataFrame with aggregated sales metrics
    """
    result = sales_data.copy()
    
    result['sales_amount'] = result['MWh'] * result['price']
    
    # Group by portfolio_id and asset_type
    grouped = result.groupby(['portfolio_id', 'asset_type']).agg({
        'MWh': 'sum',
        'price': 'mean',
        'sales_amount': 'sum',
        'date': 'count',
        'year_week': 'first'
    }).rename(columns={'date': 'transaction_count'}).reset_index()
    
    return grouped

def combine_sales_with_asset_data(
    sales_results: pd.DataFrame, 
    asset_data: pd.DataFrame, 
) -> pd.DataFrame:
    """
    Combines the processed sales data with asset information.
    
    For this function:
    1. Aggregate asset data by portfolio_id to get portfolio-level metrics
    2. Calculate portfolio statistics (asset count, primary geography, ISO regions, timezones)
    3. Calculate portfolio age based on operational dates
    4. Merge sales results with asset portfolio summary
    5. Calculate per-asset and efficiency metrics

    Parameters:
    -----------
    sales_results : pd.DataFrame
        Processed sales data with aggregated metrics by portfolio_id and asset_type
    asset_data : pd.DataFrame
        Asset information including:
        - portfolio_id: Identifier for the portfolio
        - asset_id: Unique identifier for each asset
        - geography: Location of the asset
        - ISO: Independent System Operator region
        - operational_date: Date when the asset became operational
        - timezone: Timezone where the asset is located
        
    Returns:
    --------
    pd.DataFrame
        Combined dataframe with sales metrics and asset details
    """
    
    # Agg asset data by portfolio_id to get portfolio-level metrics
    asset_portfolio_summary = asset_data.groupby('portfolio_id').agg({
        'asset_id': 'count',
        'geography': lambda x: x.mode().iloc[0] if not x.mode().empty else None,
        'ISO': lambda x: list(x.unique()),
        'operational_date': ['min', 'max'],
        'timezone': lambda x: list(x.unique())
    })
    
    asset_portfolio_summary.columns = [
        'asset_count', 'primary_geography', 'iso_regions', 
        'oldest_asset_date', 'newest_asset_date', 'timezones'
    ]
    asset_portfolio_summary = asset_portfolio_summary.reset_index()
    
    # Calculate portfolio age in years. 365.25 is used to account for leap years.
    today = pd.Timestamp.now().date()
    asset_portfolio_summary['portfolio_age_years'] = asset_portfolio_summary['oldest_asset_date'].apply(
        lambda x: (today - x).days / 365.25
    ).round(1)
    
    combined_data = pd.merge(
        sales_results,
        asset_portfolio_summary,
        on='portfolio_id',
        how='left'
    )
    
    combined_data['mwh_per_asset'] = combined_data['MWh'] / combined_data['asset_count']
    combined_data['revenue_per_asset'] = combined_data['sales_amount'] / combined_data['asset_count']
    combined_data['revenue_per_mwh'] = combined_data['sales_amount'] / combined_data['MWh']

    
    return combined_data


def create_consolidated_weekly_report(
        combined_weekly_data: Dict[str, pd.DataFrame]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Creates a consolidated report by combining all batches of processed weekly data. 
    
    For this function:
    1. Concatenate all weekly results into a single dataset
    2. Create pivot tables to compare metrics across weeks
    3. Calculate summary statistics across all weeks
    4. Calculate average weekly metrics and efficiency indicators
    5. Package results into different report views

    Parameters:
    -----------
    weekly_data : Dict[str, pd.DataFrame]
        Dictionary containing processed data
        for each week of data after having been processed by the
        `process_sales_batch` and `combine_sales_with_asset_data` funcation.
        Keys: Week identifiers (e.g., '2023-01', '2023-02')
        Values: DataFrames with the processed and combined data for each week
        
    Returns:
    --------
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        - weekly_comparison: Pivot table comparing metrics across weeks
        - summary: Overall summary across all weeks
        - all_data: Complete consolidated dataset
    """  
    all_weeks_data = pd.concat(combined_weekly_data.values(), ignore_index=True)
    
    weekly_comparison = pd.pivot_table(
        all_weeks_data, 
        values=['MWh', 'sales_amount', 'price', 'revenue_per_mwh'],
        index=['portfolio_id', 'asset_type'],
        columns='year_week'
    )
    
    weekly_comparison = weekly_comparison.reset_index()
    
    summary = all_weeks_data.groupby(['portfolio_id', 'asset_type']).agg({
        'MWh': 'sum',
        'sales_amount': 'sum',
        'transaction_count': 'sum',
        'price': 'mean',
        'asset_count': 'first', # These values should be the same across weeks
        'primary_geography': 'first',
        'portfolio_age_years': 'first'
    }).reset_index()
    
    summary['avg_weekly_revenue'] = summary['sales_amount'] / len(combined_weekly_data)
    summary['avg_revenue_per_mwh'] = summary['sales_amount'] / summary['MWh']
    
    return weekly_comparison, summary, all_weeks_data


def run_pipeline(
    weekly_results: Dict[str, pd.DataFrame],
    asset_data: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Runs the entire sales data processing pipeline.
    
    This function:
    1. Processes sales data in batches by week
       (simulating a batched API call).
    2. Combines each processed batch with asset data
    3. Generates a consolidated report from all processed weeks and returns its results.
    
    Parameters:
    -----------
    weekly_results : Dict[str, pd.DataFrame]
        Dict containing weekly sales data as if from a batched API call.
    asset_data : pd.DataFrame
        DataFrame containing asset data to be combined with sales data.
        
    Returns:
    --------
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        - weekly_comparison: Pivot table comparing metrics across weeks
        - summary: Overall summary across all weeks
        - all_weeks_data: Complete consolidated dataset
    """
    processed_weekly_results = {}
    for week in weekly_results:
        processed_weekly_data = process_sales_batch(weekly_results[week])
        combined_weekly_results = combine_sales_with_asset_data(processed_weekly_data, asset_data)
        processed_weekly_results[week] = combined_weekly_results
        print(f"Combined with Asset data for week: {week}")

    print(f"Processed data for {len(processed_weekly_results)} weeks")
    weekly_comparison, summary, all_weeks_data = create_consolidated_weekly_report(processed_weekly_results)
    return weekly_comparison, summary, all_weeks_data



