# import botocore 
# import botocore.session 
# from aws_secretsmanager_caching import SecretCache, SecretCacheConfig 

# client = botocore.session.get_session().create_client('secretsmanager')
# cache_config = SecretCacheConfig()
# cache = SecretCache( config = cache_config, client = client)

# secret = cache.get_secret_string('mysecret')

import boto3
import base64
from botocore.exceptions import ClientError


def get_client_secrets():

    secret_name = "gcn-client-credentials"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
         print(e.response['Error']['Message'])
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret