import json
import boto3

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

TABLE_NAME = 'ProcessedSalesData'

def lambda_handler(event, context):

    table = dynamodb.Table(TABLE_NAME)

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # Read file from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(response['Body'].read())

        # Prepare item for DynamoDB
        item = {
            'order_id': data['order_id'],
            'amount': data['amount'],
            'product': data['product'],
            'ingested_at': data['ingested_at']
        }

        # Store in DynamoDB
        table.put_item(Item=item)

    return {
        'statusCode': 200,
        'body': 'Data processed successfully'
    }
