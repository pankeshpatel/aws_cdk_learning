import json
import boto3
from datetime import datetime, timezone
import os

# Initialize a boto3 client for Timestream
timestream_client = boto3.client('timestream-write')

TIMESTREAM_DATABASE_NAME=os.environ['TIMESTREAM_DATABASE_NAME']
TIMESTREAM_ELECTRIC_OUTDOOR_TABLE_NAME=os.environ['TIMESTREAM_ELECTRIC_OUTDOOR_TABLE_NAME']

def handler(event, context):

    print(f"Event: {event}")
    print(f"Context", {context})
    timestamp_str = str(int(datetime.now(timezone.utc).timestamp() * 1000))
    data_json = json.dumps(event)
    record = {
        'Dimensions': [
            {'Name': 'Company', 'Value': 'EO'}
        ],
        'MeasureName': 'payload_data',
        'MeasureValue': data_json,
        'MeasureValueType': 'VARCHAR',
        'Time': str(timestamp_str)
    }

    # Write records to Timestream
    try:
        result = timestream_client.write_records(DatabaseName=TIMESTREAM_DATABASE_NAME, 
                                                TableName=TIMESTREAM_ELECTRIC_OUTDOOR_TABLE_NAME,
                                                Records=[record])
        print("Write records status:", result['ResponseMetadata']['HTTPStatusCode'])
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise e