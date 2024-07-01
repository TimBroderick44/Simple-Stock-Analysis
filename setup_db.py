import boto3
from passlib.hash import pbkdf2_sha256

dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')

def create_table():
    try:
        table = dynamodb.Table('users')
        table.meta.client.describe_table(TableName='users')
        print("Table 'users' already exists.")
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        table = dynamodb.create_table(
            TableName='users',
            KeySchema=[
                {'AttributeName': 'username', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'username', 'AttributeType': 'S'}
            ],
            # ReadCapacityUnits and WriteCapacityUnits as on the free tier
            # This means that the table can only handle 2 read and 2 write requests per second
            ProvisionedThroughput={
                'ReadCapacityUnits': 2,
                'WriteCapacityUnits': 2
            }
        )
        table.meta.client.get_waiter('table_exists').wait(TableName='users')
        print("Table 'users' created successfully.")

def hash_password(password):
    # SHA256 with PBKDF2:
    # - SHA256 is a cryptographic hash function that takes an input and produces a fixed-size string of bytes
    # - PBKDF2 takes SHA256 and adds a salt to make it computationally intensive to brute force
    
    # in the config generated, the third part is the salt. 
    password_hash = pbkdf2_sha256.hash(password)
    salt = password_hash.split('$')[3] 
    return password_hash, salt

def setup_users():
    table = dynamodb.Table('users')
    users = [
        {'username': 'user1', 'password': 'userpassword'},
        {'username': 'Tim', 'password': 'Broderick!'},
        {'username': 'Masa', 'password': 'Murakami!'}
    ]
    for user in users:
        password_hash, salt = hash_password(user['password'])
        table.put_item(
            Item={
                'username': user['username'],
                'password_hash': password_hash,
                'salt': salt
            }
        )
    print("Users inserted successfully.")

if __name__ == "__main__":
    create_table()
    setup_users()
