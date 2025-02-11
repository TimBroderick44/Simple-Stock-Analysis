from flask import Flask, jsonify, request, send_from_directory
from flask_jwt_extended import JWTManager, create_access_token, jwt_required
from passlib.hash import pbkdf2_sha256
import boto3
from io import BytesIO
import pandas as pd
import logging
from flask_cors import CORS
import secrets
from werkzeug.exceptions import Unauthorized

app = Flask(__name__, static_folder='build', static_url_path='')
CORS(app, resources={r"/*": {"origins": "*"}}) 

JWT_SECRET_KEY = secrets.token_urlsafe(32)
app.config['JWT_SECRET_KEY'] = JWT_SECRET_KEY

jwt = JWTManager(app)

dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
table = dynamodb.Table('users')

## Why do we need BytesIO?
# obj creates a stream of bytes; however pandas.read_csv expects a file-like object
# BytesIO creates a wrapper around the stream of bytes so that it appears as a file-like object
# Essentially, we are creating a virtual file in memory that pandas can read from

def read_from_s3(bucket_name, file_name):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    return pd.read_csv(BytesIO(obj['Body'].read()))

bucket_name = 'simple-stock-analysis'

logging.basicConfig(level=logging.DEBUG)

def verify_password(password, password_hash):
    #pbkdf2 = resistant to brute force attacks as it allows for a number of iterations and salt
    #sha256 = cryptographic hash function that produces a 64-character hash value (used in SSL certificates and bitcoin) 
    return pbkdf2_sha256.verify(password, password_hash)

@app.route('/login', methods=['POST'])
def login():
    username = request.json.get('username', None)
    password = request.json.get('password', None)

    response = table.get_item(Key={'username': username})
    user = response.get('Item')

    if user and verify_password(password, user['password_hash']):
        access_token = create_access_token(identity=username)
        return jsonify(access_token=access_token), 200
    else:
        return jsonify({'message': 'Invalid credentials'}), 401

@app.route('/logout', methods=['POST'])
def logout():
    return jsonify({'message': 'Successfully logged out'}), 200

@app.route('/api/daily_avg', methods=['GET'])
def get_daily_avg():
    df = read_from_s3(bucket_name, 'daily_avg.csv')
    # orient='records' creates a dictionary where each row is a record
    # Instead of records we can use index, columns, or values
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

@app.errorhandler(Unauthorized)
def handle_unauthorized(e):
    return jsonify({'message': 'Token has expired or is invalid'}), 401

@app.route('/')
def serve_react_app():
    return send_from_directory(app.static_folder, 'index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
