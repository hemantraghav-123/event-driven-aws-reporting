import json
import boto3
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
sns = boto3.client('sns')

TABLE_NAME = 'ProcessedSalesData'
REPORT_BUCKET = 'daily-report-event-pipeline-bucket'
SNS_TOPIC_ARN = 'REPLACE_WITH_SNS_ARN'

def lambda_handler(event, context):

    table = dynamodb.Table(TABLE_NAME)

    # Scan table
    response = table.scan()
    items = response['Items']

    total_orders = len(items)
    total_revenue = sum(int(item['amount']) for item in items)

    report = {
        'date': datetime.utcnow().strftime('%Y-%m-%d'),
        'total_orders': total_orders,
        'total_revenue': total_revenue
    }

    # Save report to S3
    file_name = f"reports/report-{report['date']}.json"

    s3.put_object(
        Bucket=REPORT_BUCKET,
        Key=file_name,
        Body=json.dumps(report)
    )

    # Send Email Notification
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="Daily Sales Report Generated",
        Message=f"Report generated and stored in S3: {file_name}"
    )

    return {
        'statusCode': 200,
        'body': 'Report generated successfully'
    }
