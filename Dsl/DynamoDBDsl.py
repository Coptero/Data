from pyspark.sql import *
import boto3
from boto3.dynamodb.conditions import Key, Attr

# Get the service resource.
dynamodb = boto3.resource('dynamodb')
client = boto3.client('dynamodb')

class DynamoDBDsl:


    def getAuxTablePath(auxTableName):
        table = dynamodb.Table("coptero_state")
        key_to_get = {}
        key_to_get.update({'process', Attr('rod_master_table')})
        key_to_get.update({'module', Attr(auxTableName)})
        response = table.get_item(Key=key_to_get)
        # items = response['items']
        # return items
        return response['info']
