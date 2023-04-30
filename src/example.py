# A minimal example of how to use DefraClient.

import uuid

from gql import gql

from defradb import (
    DefraClient,
    DefraConfig,
    dict_to_create_query,
)

# Configuring the client.
endpoint = "localhost:9181/api/v0/"
cfg = DefraConfig(endpoint)
client = DefraClient(cfg)

# Loading a schema as a string.
typename = "Parameters"
schema = f"""
type {typename} {{
    a: String
    b: String
    c: String
}}
"""
response_schema = client.load_schema(schema)

# Creating a new document of the type with random data.
data = {
    "a": uuid.uuid4().hex,
    "b": uuid.uuid4().hex,
    "c": uuid.uuid4().hex,
}
request = dict_to_create_query(typename, data)
response_mutation = client.request(request)

# Obtaining a list of all these documents.
get_users_request = gql(
    f"""
query {{
    {typename} {{
        _key
        a
        b
        c
    }}
}}
"""
)
response_users = client.request(get_users_request)
if response_users is not None:
    for user in response_users:
        print(user)
