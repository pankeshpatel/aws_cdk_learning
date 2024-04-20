import json
import boto3
from datetime import datetime, timezone
import os


# Initialize a boto3 client for Timestream
timestream_client = boto3.client('timestream-write')

TIMESTREAM_DATABASE_NAME=os.environ['TIMESTREAM_DATABASE_NAME']
TIMESTREAM_ENERGYSTATE_TABLE_NAME=os.environ['TIMESTREAM_ENERGYSTATE_TABLE_NAME']
TIMESTREAM_LIGHTSTATE_TABLE_NAME=os.environ['TIMESTREAM_LIGHTSTATE_TABLE_NAME']

def extract_state(topic):
    parts = topic.split('/')
    return parts[-1]

def extract_canopyID(topic):
    parts = topic.split('/')
    return parts[2]

def handler(event, context):

    print(f"Event: {event}")
    # Get the topic string
    topic_string = event['topic']
    canopyID = extract_canopyID(topic_string)
    # Get the state from the topic string 
    state = extract_state(topic_string)
    print("The state is:", state)

    if state == 'energy-state':
        data = {
            'canopy_soc': (str(event.get('canopy_soc', 0)), 'DOUBLE'),
            'canopy_charging': (str(event.get('canopy_charging', False)), 'VARCHAR'),
            'canopy_powering': (str(event.get('canopy_powering', False)), 'VARCHAR'),
            'vehicle_soc': (str(event.get('vehicle_soc', 0)), 'DOUBLE'),
            'vehicle_charging': (str(event.get('vehicle_charging', False)), 'VARCHAR')
        }

        timestamp_str = str(int(datetime.now(timezone.utc).timestamp() * 1000))

        # Prepare a single record with multiple measurements
        records = [{
            'Dimensions': [{'Name': 'canopy', 'Value': canopyID}],
            'Time': timestamp_str,
            'MeasureName': 'canopy_data',
            'MeasureValueType': 'MULTI',
            'MeasureValues': [
                {'Name': 'canopy_soc', 'Value': data['canopy_soc'][0], 'Type': data['canopy_soc'][1]},
                {'Name': 'canopy_charging', 'Value': data['canopy_charging'][0], 'Type': data['canopy_charging'][1]},
                {'Name': 'canopy_powering', 'Value': data['canopy_powering'][0], 'Type': data['canopy_powering'][1]},
                {'Name': 'vehicle_soc', 'Value': data['vehicle_soc'][0], 'Type': data['vehicle_soc'][1]},
                {'Name': 'vehicle_charging', 'Value': data['vehicle_charging'][0], 'Type': data['vehicle_charging'][1]}
            ]
        }]

        # Write records to Timestream
        try:
            result = timestream_client.write_records(DatabaseName=TIMESTREAM_DATABASE_NAME, 
                                                    TableName=TIMESTREAM_ENERGYSTATE_TABLE_NAME,
                                                    Records=records)
            print("Write records status:", result['ResponseMetadata']['HTTPStatusCode'])
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            raise e

        return {
            'statusCode': 200,
            'body': f"Successfully processed record for topic {topic_string}"
        }