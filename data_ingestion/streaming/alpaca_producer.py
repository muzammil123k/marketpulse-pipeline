import os
import json
import time
#from datetime import datetime
from datetime import datetime, timezone
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest
from azure.eventhub import EventHubProducerClient, EventData

# =====================================================================
# 1. LOAD & DEBUG ENVIRONMENT
# =====================================================================
load_dotenv()

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
EH_CONN_STR = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")
EH_NAME = os.getenv("EVENTHUB_NAME")

print("🔍 --- DETAILED CONNECTION STRING DIAGNOSTIC ---")
if EH_CONN_STR:
    print(f"Total Character Length:   {len(EH_CONN_STR)}")
    print(f"Starts with 'Endpoint='?  {EH_CONN_STR.startswith('Endpoint=')}")
    
    # Clean check without backslashes inside the f-string
    has_quote = EH_CONN_STR.endswith('"') or EH_CONN_STR.endswith("'")
    print(f"Ends with a Quote?        {has_quote}")
    print(f"First 30 characters:      {EH_CONN_STR[:30]}")
else:
    print("Event Hub Conn Str is completely empty (None).")
print("------------------------------------------------\n")

# Guard rails to stop execution cleanly if keys are missing
if not EH_CONN_STR or not EH_NAME:
    print("❌ ERROR: Missing Azure Event Hub configurations in your .env file!")
    print("Please fix your .env file before proceeding.")
    exit(1)

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    print("❌ ERROR: Missing Alpaca API credentials in your .env file!")
    print("Please fix your .env file before proceeding.")
    exit(1)

# =====================================================================
# 2. INITIALIZE CLIENTS
# =====================================================================
print("🔌 Initializing platform clients...")
alpaca_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
producer_client = EventHubProducerClient.from_connection_string(conn_str=EH_CONN_STR, eventhub_name=EH_NAME)
print("✅ Clients successfully initialized!\n")

# Use a subset of your tech stocks for real-time tracking
TICKERS = ["AAPL", "MSFT", "TSLA", "NVDA"]

def stream_ticks():
    print(f"📡 Starting Alpaca to Azure Event Hub Streamer for: {TICKERS}")
    print("Press Ctrl+C to terminate the stream.\n")
    
    try:
        while True:
            request_params = StockLatestQuoteRequest(symbol_or_symbols=TICKERS)
            latest_quotes = alpaca_client.get_stock_latest_quote(request_params)
            
            event_batch = producer_client.create_batch()
            
            for ticker, quote in latest_quotes.items():
                tick_payload = {
                    "ticker": ticker,
                    "timestamp": quote.timestamp.isoformat() if hasattr(quote, 'timestamp') else datetime.now(timezone.utc).isoformat(),
                    "bid_price": quote.bid_price,
                    "bid_size": quote.bid_size,
                    "ask_price": quote.ask_price,
                    "ask_size": quote.ask_size,
                    "ingest_timestamp": datetime.now(timezone.utc).isoformat()
                }
                
                json_data = json.dumps(tick_payload)
                event_data = EventData(json_data)
                
                try:
                    event_batch.add(event_data)
                    print(f"⚡ Packed tick: {ticker} | Bid: {quote.bid_price} | Ask: {quote.ask_price}")
                except ValueError:
                    producer_client.send_batch(event_batch)
                    event_batch = producer_client.create_batch()
                    event_batch.add(event_data)
            
            if len(event_batch) > 0:
                producer_client.send_batch(event_batch)
                print("📤 Batch successfully transmitted to Azure Event Hub.\n")
            
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n🛑 Stream stopped manually by user.")
    except Exception as e:
        print(f"\n❌ Streaming Error encountered: {e}")
    finally:
        producer_client.close()
        print("🔒 Event Hub client connection closed safely.")

if __name__ == "__main__":
    stream_ticks()