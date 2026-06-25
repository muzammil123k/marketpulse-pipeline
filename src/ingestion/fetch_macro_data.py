import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Securely fetch the API key
FRED_API_KEY = os.getenv('FRED_API_KEY')

if not FRED_API_KEY:
    raise ValueError("❌ FRED_API_KEY is missing! Check your .env file.")

def fetch_bitemporal_fred_data(series_id, series_name):
    """
    Fetches macroeconomic data from the FRED API with strict knowledge-time guardrails.
    Includes a fallback for series that do not support ALFRED vintage history.
    """
    print(f"Fetching {series_name} ({series_id})...")
    
    url = "https://api.stlouisfed.org/fred/series/observations"
    
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'realtime_start': '1776-07-04' # Ask for all historical revisions
    }
    
    response = requests.get(url, params=params)
    
    # FALLBACK: If the series (like EFFR) doesn't have a revision history
    if response.status_code == 400:
        print(f"  ↳ ⚠️ {series_name} has no vintage history. Falling back to standard pull...")
        del params['realtime_start'] # Remove the ALFRED parameter
        response = requests.get(url, params=params)
        
    response.raise_for_status()
    data = response.json()
    
    # Extract the observations
    observations = data.get('observations', [])
    df = pd.DataFrame(observations)
    
    # Filter out "." (FRED uses this for missing data)
    df = df[df['value'] != '.']
    
    # If we had to use the fallback, the API sets 'realtime_start' to today's date.
    # To prevent time-travel leakage, we force the publish date to equal the effective date.
    if 'realtime_start' not in params:
        df['realtime_start'] = df['date']
        
    # Sort by the actual date, then by when the world found out
    df.sort_values(by=['date', 'realtime_start'], inplace=True)
    
    # KEEP ONLY THE INITIAL RELEASE: Drop all future revisions
    df.drop_duplicates(subset=['date'], keep='first', inplace=True)
    
    # Keep only the columns we need
    df = df[['date', 'realtime_start', 'value']].copy()
    
    # Rename them to match our architectural standard
    df.rename(columns={
        'date': 'effective_date',       
        'realtime_start': 'published_at', 
        'value': f'{series_name.lower()}_value'
    }, inplace=True)
    
    return df

if __name__ == "__main__":
    try:
        # 1. Fetch Effective Federal Funds Rate (Daily Interest Rate)
        effr_df = fetch_bitemporal_fred_data('EFFR', 'EFFR')
        
        # 2. Fetch Consumer Price Index (Monthly Inflation)
        cpi_df = fetch_bitemporal_fred_data('CPIAUCSL', 'CPI')
        
        # Save locally to simulate our Azure Blob Storage drop
        effr_df.to_parquet('effr_historical.parquet', index=False)
        cpi_df.to_parquet('cpi_historical.parquet', index=False)
        
        print("\n✅ Success! Bitemporal data saved to Parquet.")
        print(f"EFFR Rows: {len(effr_df)} | CPI Rows: {len(cpi_df)}")
        print("\nSample CPI Data (Notice the gap between effective and published dates):")
        print(cpi_df.tail())
        
    except Exception as e:
        print(f"❌ Pipeline Failed: {e}")