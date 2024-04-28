import base64
import json
import boto3
from datetime import datetime, timezone
import os
import time

# Initialize a boto3 client for Timestream
timestream_client = boto3.client('timestream-write')

TIMESTREAM_DATABASE_NAME=os.environ['TIMESTREAM_DATABASE_NAME']
TIMESTREAM_ELECTRIC_OUTDOOR_TABLE_NAME=os.environ['TIMESTREAM_ELECTRIC_OUTDOOR_TABLE_NAME']

INTERVAL = 1 # Seconds


def prepare_common_attributes(TYPE_OF_DATA, CANOPY_ID):
    common_attributes = {
        'Dimensions': [
            {'Name': "Canopy_ID", 'Value': CANOPY_ID}
        ],
        'MeasureName': TYPE_OF_DATA,
        'MeasureValueType': 'MULTI'
    }
    return common_attributes


def prepare_record(current_time):
    record = {
        'Time': str(current_time),
        'MeasureValues': []
    }
    return record


def prepare_measure(measure_name, measure_value, measure_type):
    measure = {
        'Name': measure_name,
        'Value': str(measure_value),
        'Type': measure_type
    }
    return measure


def write_records(records, common_attributes):
    try:
        result = timestream_client.write_records(DatabaseName=TIMESTREAM_DATABASE_NAME,
                                            TableName=TIMESTREAM_ELECTRIC_OUTDOOR_TABLE_NAME,
                                            CommonAttributes=common_attributes,
                                            Records=records)
        status = result['ResponseMetadata']['HTTPStatusCode']
        print("Processed %d records. WriteRecords HTTPStatusCode: %s" %
              (len(records), status))
    except Exception as err:
        print("Error:", err)


def handler(event, context):
    print(f"Event: {event}")
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record["kinesis"]["data"])
        data = json.loads(payload)
        print("Received record:", data)
        
        CANOPY_ID = data['canopy_id']

        if data['message_type'] == "energy-state":
            print("Inside")
            TYPE_OF_DATA = data['message_type']
            common_attributes = prepare_common_attributes(TYPE_OF_DATA, CANOPY_ID)
            records = []
            current_time = int(time.time() * 1000)
            canopy_soc = data["canopy_soc"]
            canopy_charging = data["canopy_charging"]
            canopy_powering = data["canopy_powering"]
            vehicle_soc = data["vehicle_soc"]
            vehicle_charging = data["vehicle_charging"]
    
    
            record = prepare_record(current_time)
            record['MeasureValues'].append(prepare_measure('canopy_soc', canopy_soc, "DOUBLE"))
            record['MeasureValues'].append(prepare_measure('canopy_charging', canopy_charging, "BOOLEAN"))
            record['MeasureValues'].append(prepare_measure('canopy_powering', canopy_powering, "BOOLEAN"))
            record['MeasureValues'].append(prepare_measure('vehicle_soc', vehicle_soc, "DOUBLE"))
            record['MeasureValues'].append(prepare_measure('vehicle_charging', vehicle_charging, "BOOLEAN"))
    
            records.append(record)
            write_records(records, common_attributes)
            records = []
    
    
        elif data['message_type'] == "light-state":
            print("Inide elif : Ligts")
            TYPE_OF_DATA = data['message_type']
            common_attributes = prepare_common_attributes(TYPE_OF_DATA, CANOPY_ID)
            records = []
    
            for light in data["lights"]:
                light_id = light["id"]
                light_intensity = light["intensity"]
                light_color = light["color"]
                current_time = int(time.time() * 1000)
                record = prepare_record(current_time)
                record['MeasureValues'].append(prepare_measure('light_id', light_id, "VARCHAR"))
                record['MeasureValues'].append(prepare_measure('light_intensity', light_intensity, "DOUBLE"))
                record['MeasureValues'].append(prepare_measure('light_color_rgb', light_color, "VARCHAR"))
    
                records.append(record)
                write_records(records, common_attributes)
                records = []
    return 'Finished processing records'
