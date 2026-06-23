import os
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# 1. Load Environment Variables
load_dotenv()
connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
container_name = "bronze"

def fetch_and_upload_data():
    print("🚀 Starting Historical Data Ingestion...")
    
    # 2. Define our expanded basket of 25 stocks
    tickers = [
        "AAPL", "MSFT", "TSLA", "NVDA", "AMZN", 
        "GOOGL", "META", "BRK-B", "JNJ", "JPM", 
        "V", "PG", "UNH", "HD", "MA", 
        "DIS", "NFLX", "PYPL", "INTC", "CSCO", 
        "PEP", "KO", "PFE", "ABBV", "BAC"
    ]
    
    # 3. Fetch 5 years of historical OHLCV data
    print(f" Fetching 5 years of data for {len(tickers)} tickers...")
    
    # yfinance will automatically download these in parallel
    df = yf.download(tickers, period="5y", group_by="ticker", auto_adjust=True)
    
    # Clean up the headers so 'Ticker' is a clean column
    df = df.stack(level=0, future_stack=True).rename_axis(['Date', 'Ticker']).reset_index()
    
    # 4. Save locally as Parquet
    local_file_name = "historical_stock_data_25.parquet"
    df.to_parquet(local_file_name, engine='pyarrow')
    print(f" Saved locally as {local_file_name} ({len(df)} rows)")

    # 5. Upload to Azure Data Lake (Bronze Container)
    print("☁️ Uploading to Azure Data Lake Gen2...")
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

        with open(local_file_name, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        
        print(f" Success! {local_file_name} successfully uploaded to the '{container_name}' container.")
        
    except Exception as e:
        print(f" Azure Upload Failed: {e}")

if __name__ == "__main__":
    fetch_and_upload_data()