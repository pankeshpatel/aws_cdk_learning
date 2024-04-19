import json
import boto3
from datetime import datetime

# Initialize a boto3 client for Timestream
timestream_write = boto3.client('timestream-write')

DATABASE_NAME = 'MyDatabase'
TABLE_NAME = 'MyTable'

def handler(event, context):
    # Extract temperature and humidity directly from the event
    # Example event: {'temperature': 22.5, 'humidity': 60, 'timestamp': '2024-04-10T12:00:00Z'}
    temperature = event.get('temperature')
    humidity = event.get('humidity')

    # Format the timestamp for Timestream
    # Convert ISO 8601 timestamp to milliseconds since epoch if necessary
    if isinstance(timestamp, str):
        timestamp = str(int(datetime.fromisoformat(timestamp.replace('Z', '+00:00')).timestamp() * 1000))

    # Prepare records for Timestream
    records = [
        {
            'Dimensions': [{'Name': 'sensor', 'Value': 'sensor1'}],  # Static dimension, adjust as needed
            'MeasureName': 'temperature',
            'MeasureValue': str(temperature),
            'MeasureValueType': 'DOUBLE',
            'Time': timestamp,
            'TimeUnit': 'MILLISECONDS'
        },
        {
            'Dimensions': [{'Name': 'sensor', 'Value': 'sensor1'}],  # Static dimension, adjust as needed
            'MeasureName': 'humidity',
            'MeasureValue': str(humidity),
            'MeasureValueType': 'DOUBLE',
            'Time': timestamp,
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
        print(f"Data written to Timestream successfully: {result}")
        return {
            'statusCode': 200,
            'body': json.dumps('Data successfully written to Timestream')
        }
    except Exception as e:
        print(f"Error writing to Timestream: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps(f"Error writing to Timestream: {e}")
        }
