import json
import boto3
from datetime import datetime, timezone

# Initialize a boto3 client for Timestream
timestream_write = boto3.client('timestream-write')

DATABASE_NAME = 'MyDatabase'
TABLE_NAME = 'MyTable'

def handler(event, context):

    print(event)
    # Extract temperature and humidity directly from the event
    # Example event: {'temperature': 22.5, 'humidity': 60, 'timestamp': '2024-04-10T12:00:00Z'}
    temperature = event.get('temperature')
    humidity = event.get('humidity')
    # timestamp = datetime.now(timezone.utc).timestamp() * 1000  # Current time in UTC in milliseconds
    timestamp = str(int(datetime.now(timezone.utc).timestamp() * 1000))
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
        print("Data written to Timestream successfully:", result)
    except timestream_write.exceptions.RejectedRecordsException as e:
        print("Error writing to Timestream:", e)
        # Extracting details of the rejected records
        for rejected_record in e.response['Error']['RejectedRecords']:
            print("Rejected Record Reason:", rejected_record['Reason'])
            print("Rejected Record:", json.dumps(rejected_record['Record'], indent=4))
    except Exception as e:
        print("Unexpected error:", e)

    return {
        'statusCode': 200,
        'body': json.dumps('Data handling completed')
    }
