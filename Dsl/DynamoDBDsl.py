from botocore.exceptions import ClientError
from pyspark.sql import *
import boto3
from boto3.dynamodb.conditions import Key, Attr


# Get the service resource.
# dynamodb = boto3.resource('dynamodb', region_name='eu-central-1', endpoint_url="https://dynamodb.eu-central-1.amazonaws.com")
# client = boto3.client('dynamodb')


class DynamoDBDsl:

    def getAuxTablePath(auxTableName):
        dynamodb = boto3.resource('dynamodb', region_name='eu-central-1',
                                  endpoint_url="https://dynamodb.eu-central-1.amazonaws.com")
        table = dynamodb.Table("coptero_state")
        try:
            response = table.get_item(
                Key={
                    'process': 'rod_master_table',
                    'module': auxTableName
                }
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return response['Item']['info']
