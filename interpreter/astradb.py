import os
import openai
import requests
import json
import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

session = None
def astradb_upsert(messages):
    global session
    keyspace = "open_interpreter"
    table = "message_history"
    astradb_api_token = os.environ.get('ASTRA_DB_TOKEN')
    dbid = os.environ.get('ASTRA_DBID')
    if dbid == None:
        dbid = get_or_create_db(astradb_api_token)
    if session == None:
        session = connect(astradb_api_token, dbid)
        create_table(session, astradb_api_token, dbid, keyspace, table)
    upsert_messages(session, messages, keyspace, table)

def get_or_create_db(astradb_api_token):
    if astradb_api_token == None:
        astradb_api_token= input("ASTRA_DB_TOKEN (you can also set this as an env var): ")

    url = f"https://api.astra.datastax.com/v2/databases/"
    headers = {
        "Authorization": f"Bearer {astradb_api_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers).json()

    if 'errors' in response:
        # Handle the errors
        errors = response['errors']
        if errors[0]['message']:
            if errors[0]['message'] == 'JWT not valid':
                print(
                    "Invalid token, your token should start with `ASTRACS:`")
        astradb_api_token = None
        return  get_or_create_db()
    else:
        parsed_data = [{"id": item["id"], "name": item["info"]["name"], "status": item["status"]} for item in response]
        dbid = input(f"you have the following active dbs: \n{parsed_data} \nPlease enter the dbid of the db you want to use: ")
        return dbid



def connect(astradb_api_token, dbid):
    # connect to Astra
    url = get_astra_bundle_url(astradb_api_token, dbid)
    if url:
        # Download the secure connect bundle and extract it
        r = requests.get(url)
        bundlepath = f'/tmp/{dbid}.zip'
        with open(bundlepath, 'wb') as f:
            f.write(r.content)
        # Connect to the cluster
        cloud_config = {
            'secure_connect_bundle': bundlepath
        }
        auth_provider = PlainTextAuthProvider('token', astradb_api_token)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect()
        return session

def get_astra_bundle_url(astradb_api_token, dbid):
    # Define the URL
    url = f"https://api.astra.datastax.com/v2/databases/{dbid}/secureBundleURL"

    # Define the headers
    headers = {
        "Authorization": f"Bearer {astradb_api_token}",
        "Content-Type": "application/json"
    }

    # Define the payload (if any)
    payload = {}

    # Make the POST request
    response = requests.post(url, headers=headers, data=json.dumps(payload)).json()

    # Print the response
    if 'errors' in response:
        # Handle the errors
        errors = response['errors']
        if errors[0]['message']:
            if errors[0]['message'] == 'JWT not valid':
                print(
                    "Invalid token or db, your token should start with `ASTRACS:`")
        return False
    else:
        return response['downloadURL']

def make_keyspace(dbid, astra_api_token, keyspace):
    url = f"https://api.astra.datastax.com/v2/databases/{dbid}/keyspaces/{keyspace}"
    headers = {
        "Authorization": f"Bearer {astra_api_token}",
        "Content-Type": "application/json"
    }
    payload = {}

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code != 201:
        print(f"Error creating keyspace: {response.text}")


def create_table(session, astra_api_token, dbid, keyspace, table):
   try:
        make_keyspace(dbid, astra_api_token, keyspace)
        query_string = f"""create table if not exists {keyspace}.{table} (
                session_id text,
                user_name text,
                content text,
                created_at timestamp,
                embedding VECTOR<float,1536>,
                PRIMARY KEY ((user_name), session_id, created_at)
            ) WITH CLUSTERING ORDER BY (session_id ASC, created_at DESC);
        """
        session.execute(query_string)

        statement = SimpleStatement(
            f"CREATE CUSTOM INDEX IF NOT EXISTS ON {keyspace}.{table} (embedding) USING 'StorageAttachedIndex';"
            , consistency_level=ConsistencyLevel.QUORUM
        )
        session.execute(statement)
   except Exception as e:
       print(f"Exception creating table or index: {e}")



def upsert_messages(session, messages, keyspace, table):
    try:
        queryString = f"""
            insert into {keyspace}.{table} 
            (session_id, user_name, content, created_at, embedding) 
            VALUES (%s, %s, %s, %s, %s);
        """
        statement = SimpleStatement(queryString, consistency_level=ConsistencyLevel.QUORUM)

        openai.api_key = os.environ.get('OPENAI_API_KEY')

        for message in messages:
            response = openai.Embedding.create(model="text-embedding-ada-002", input=json.dumps(message))
            embedding = response["data"][0]["embedding"]
            session.execute(statement, (
                "session",
                "user_name",
                json.dumps(message),
                message["timestamp"],
                embedding
                )
            )
    except Exception as e:
        print(f"Exception inserting into table: {e}")
