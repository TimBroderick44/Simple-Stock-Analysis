from flask import Flask, jsonify, render_template
import pandas as pd

app = Flask(__name__)

def read_from_local(file_name):
    return pd.read_csv(file_name)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/daily_avg', methods=['GET'])
def get_daily_avg():
    df = read_from_local('./daily_avg.csv')
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/hourly_max', methods=['GET'])
def get_hourly_max():
    df = read_from_local('./hourly_max.csv')
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/daily_volume', methods=['GET'])
def get_daily_volume():
    df = read_from_local('./daily_volume.csv')
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/moving_avg_close', methods=['GET'])
def get_moving_avg_close():
    df = read_from_local('./moving_avg_close.csv')
    return jsonify(df.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
