import json
import boto3
from datetime import datetime, timezone
import os

# Initialize a boto3 client for Timestream
timestream_write = boto3.client('timestream-write')

# DATABASE_NAME = 'MyDatabase'
# TABLE_NAME = 'MyTable'
DATABASE_NAME = os.environ['TIMESTREAM_DATABASE_NAME']
TABLE_NAME = os.environ['TIMESTREAM_TABLE_NAME']
print(f"Live Stream Database Name: ${DATABASE_NAME}")
print(f"Live Stream Table Name ${TABLE_NAME}")

def handler(event, context):
    print(event)  # Debug print to show event structure
    
    # Extract temperature and humidity directly from the event
    temperature = event.get('temperature')
    humidity = event.get('humidity')
    
    # Generate current timestamp in milliseconds since epoch (UTC)
    timestamp_str = str(int(datetime.now(timezone.utc).timestamp() * 1000))

    # Prepare records for Timestream
    records = [
        {
            'Dimensions': [{'Name': 'sensor', 'Value': 'sensor1'}],
            'MeasureName': 'temperature',
            'MeasureValue': str(temperature),
            'MeasureValueType': 'DOUBLE',
            'Time': timestamp_str,
            'TimeUnit': 'MILLISECONDS'
        },
        {
            'Dimensions': [{'Name': 'sensor', 'Value': 'sensor1'}],
            'MeasureName': 'humidity',
            'MeasureValue': str(humidity),
            'MeasureValueType': 'DOUBLE',
            'Time': timestamp_str,
            'TimeUnit': 'MILLISECONDS'
        }
    ]

    # Attempt to write records to Timestream
    try:
        result = timestream_write.write_records(
            DatabaseName=DATABASE_NAME,
            TableName=TABLE_NAME,
            Records=records
        )
        print("Data written to Timestream successfully:", result)
    except Exception as e:
        print("Unexpected error:", e)

    return {
        'statusCode': 200,
        'body': json.dumps('Data handling completed')
    }
