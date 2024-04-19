import json
import boto3
from datetime import datetime, timezone

# Initialize a boto3 client for Timestream
timestream_write = boto3.client('timestream-write')

DATABASE_NAME = 'MyDatabase'
TABLE_NAME = 'MyTable'

def handler(event, context):
    print(event)  # Debug print to show event structure
    # Extract temperature and humidity directly from the event
    temperature = event.get('temperature')
    humidity = event.get('humidity')
    # event_timestamp = event.get('timestamp', None)

    # # Check if event provides a timestamp and convert it to milliseconds since epoch
    # if event_timestamp:
    #     # Parse the provided ISO 8601 timestamp
    #     try:
    #         # Remove 'Z' and convert
    #         timestamp = int(datetime.fromisoformat(event_timestamp.replace('Z', '+00:00')).timestamp() * 1000)
    #     except ValueError:
    #         # Fallback if timestamp is invalid
    #         timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
    # else:
    #     # Generate current timestamp if none provided
    #     timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

    # timestamp_str = str(timestamp)  # Convert to string for Timestream

    # Prepare records for Timestream
    records = [
        {
            'Dimensions': [{'Name': 'sensor', 'Value': 'sensor1'}],
            'MeasureName': 'temperature',
            'MeasureValue': str(temperature),
            'MeasureValueType': 'DOUBLE'
            # 'Time': timestamp_str,
            # 'TimeUnit': 'MILLISECONDS'
        },
        {
            'Dimensions': [{'Name': 'sensor', 'Value': 'sensor1'}],
            'MeasureName': 'humidity',
            'MeasureValue': str(humidity),
            'MeasureValueType': 'DOUBLE'
            # 'Time': timestamp_str,
            # 'TimeUnit': 'MILLISECONDS'
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

