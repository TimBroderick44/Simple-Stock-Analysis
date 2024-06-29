import boto3
from passlib.hash import pbkdf2_sha256

dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')

def create_table_if_not_exists():
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
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        table.meta.client.get_waiter('table_exists').wait(TableName='users')
        print("Table 'users' created successfully.")

def hash_password(password):
    salt = pbkdf2_sha256.genconfig().split('$')[2]  # Generate a salt
    password_hash = pbkdf2_sha256.hash(password)
    return password_hash, salt

def setup_users():
    table = dynamodb.Table('users')
    users = [
        {'username': 'user1', 'password': 'userpassword'},
        {'username': 'Tim', 'password': 'Legend!'},
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
    create_table_if_not_exists()
    setup_users()
