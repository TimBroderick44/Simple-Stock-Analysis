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
    time_series = response.json().get('Time Series (1min)', {})
    
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
        for timestamp, values in time_series.items()
    ]
    
    return data

def save_to_s3(df, bucket_name, file_name):
    ## StringIO => In-memory file-like object
    ## This is needed to pass the CSV data to boto3
    csv_buffer = StringIO()
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
