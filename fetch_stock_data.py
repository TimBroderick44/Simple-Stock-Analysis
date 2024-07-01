import requests
import pandas as pd
import boto3
from dotenv import load_dotenv
from io import StringIO
import os

load_dotenv()

def fetch_full_minute_data(stock_symbol, api_key):
    ## FULL = 1min data for the last 20 days
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={stock_symbol}&interval=1min&outputsize=full&apikey={api_key}'
    response = requests.get(url)
    # Get the time series data or an empty dictionary if it doesn't exist
    time_series = response.json().get('Time Series (1min)', {})
    
    # Transform the data into a list of dictionaries
    data = [
        {
            'stock_symbol': stock_symbol,
            'timestamp': timestamp,
            'open': float(values['1. open']),
            'high': float(values['2. high']),
            'low': float(values['3. low']),
            'close': float(values['4. close']),
            'volume': int(values['5. volume'])
        }
        # For each key-value pair (timestamp: values) in the time_series dictionary, do the above transformation.
        for timestamp, values in time_series.items()
    ]
    
    return data

def save_to_s3(df, bucket_name, file_name):
    ## StringIO => In-memory file-like object
    ## This is needed to pass the CSV data to boto3
    ## Don't need to use Pandas but allows for easier conversation to CSV and with StringIO can create an in-memory CSV
    
    # Create one in-memory file-like object
    csv_buffer = StringIO()
    # The path is the in-memory object and don't want an index. 
    df.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, file_name).put(Body=csv_buffer.getvalue())

api_key = os.getenv('ALPHA_VANTAGE_API_KEY')

stock_symbols = ['AAPL', 'MSFT']
bucket_name = 'simple-stock-analysis'

for stock_symbol in stock_symbols:
    data = fetch_full_minute_data(stock_symbol, api_key)
    df = pd.DataFrame(data)
    file_name = f'{stock_symbol}_minute_data.csv'
    save_to_s3(df, bucket_name, file_name)
