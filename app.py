from flask import Flask, jsonify, render_template, request, abort
import pandas as pd
import boto3
from io import BytesIO

app = Flask(__name__)

def read_from_s3(bucket_name, file_name):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    return pd.read_csv(BytesIO(obj['Body'].read()))

bucket_name = 'simple-stock-analysis'

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/daily_avg', methods=['GET'])
def get_daily_avg():
    df = read_from_s3(bucket_name, 'daily_avg.csv')
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/hourly_max', methods=['GET'])
def get_hourly_max():
    df = read_from_s3(bucket_name, 'hourly_max.csv')
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/daily_volume', methods=['GET'])
def get_daily_volume():
    df = read_from_s3(bucket_name, 'daily_volume.csv')
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/moving_avg_close', methods=['GET'])
def get_moving_avg_close():
    df = read_from_s3(bucket_name, 'moving_avg_close.csv')
    return jsonify(df.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
