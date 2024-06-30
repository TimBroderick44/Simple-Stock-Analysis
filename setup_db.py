import boto3
from passlib.hash import pbkdf2_sha256

dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')

def create_table_if_not_exists():
    # Flipped so that creation is not first but the check is first
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
        
# SHA256 with PBKDF2:
# - SHA256 is a cryptographic hash function that takes an input and produces a fixed-size string of bytes
# - PBKDF2 takes SHA256 and adds a salt to make it computationally intensive to brute force

def hash_password(password):
    # in the config generated, the second part is the salt. 
    # e.g. $pbkdf2-sha256$29000$O3lE1P/Eb6R/CZH2qO6vKA$6TziYke8al6DhrKZmhBN19tsJQZ3U64KZGRk/WUKBC8
    salt = pbkdf2_sha256.genconfig().split('$')[2] 
    password_hash = pbkdf2_sha256.hash(password)
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
    create_table_if_not_exists()
    setup_users()
