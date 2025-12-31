import json
import boto3
import uuid
from datetime import datetime

s3 = boto3.client('s3')

RAW_BUCKET = 'raw-data-event-pipeline-bucket'

def lambda_handler(event, context):
    try:
        # Handle both API Gateway and test events
        if 'body' in event:
            if isinstance(event['body'], str):
                body = json.loads(event['body'])
            else:
                body = event['body']
        else:
            body = event

        # Add timestamp
        body['ingested_at'] = datetime.utcnow().isoformat()

        # Create unique filename
        file_name = f"raw-data/{uuid.uuid4()}.json"

        # Upload to S3
        s3.put_object(
            Bucket=RAW_BUCKET,
            Key=file_name,
            Body=json.dumps(body)
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data ingested successfully',
                'file': file_name
            })
        }

    except Exception as e:
        print("ERROR:", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }
