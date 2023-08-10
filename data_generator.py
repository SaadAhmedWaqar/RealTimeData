import numpy as np
import time
from datetime import datetime
from faker import Faker
import pandas as pd
import boto3
import json

fake = Faker()
mac_addresses = [
    '00:16:3E:AB:CD:01',
    '08:23:45:EF:67:29',
    '1A:BC:2D:5E:9F:38',
    '5C:D4:E5:6F:A2:B1',
    'D0:72:34:81:C9:FA',
    '40:AA:BC:23:DD:52',
    '7A:8F:12:6E:40:39',
    '5F:6D:3A:4B:EF:0C',
    'B3:9A:E7:84:C1:5D',
    'E0:21:F8:35:CD:6B',
    '11:22:33:44:55:66',
    'F0:E1:D2:C3:B4:A5',
    '0A:3B:9F:85:27:ED',
    '87:6C:FA:09:1B:2D',
    '56:C8:A0:BD:4E:F7'
]

start_time = int(time.time())
end_time = time.time() + 185
number_of_devices = 5

areas = ['N', 'S', 'E', 'W']

def get_device_ids_areas(number_of_devices):
    # Universal Unique Identifier
    return [(mac_addresses[x], areas[x % 4]) for x in range(number_of_devices)]

def convert_date_to_human_readable(unix_time):
    return datetime.fromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')

def get_temperature(previous_temperature = 22):
    if previous_temperature % 3 == 0:
        return previous_temperature + np.random.randint(-2,6)
    else:
        return previous_temperature + np.random.randint(-3,3)

kinesis = boto3.client('kinesis', region_name='us-east-1')


stream_name  = os.environ.get('stream_name')
stream_ARN = os.environ.get('stream_ARN')

def put_records_to_stream(record):
    
    record_bytes = str(record).encode('utf-8')
    json_data = json.dumps(record)
    print(json_data)
    
    response = kinesis.put_record(
    StreamName =stream_name,
    Data = json_data,
    PartitionKey = record['id'],
    StreamARN = stream_ARN
    )           
    
    return response
    

    


def main():
    ids_areas = get_device_ids_areas(number_of_devices)
    dict_temp = {}
    default_temp = 22
    for each_time in range(int(start_time), int(end_time), 1):
    #for each_id, area in ids_areas:
        
        #for each_time in range(int(start_time), int(end_time), 1):
        for each_id, area in ids_areas:
            dict_temp[each_id] = dict_temp.get(each_id, default_temp)
            time_stamp = convert_date_to_human_readable(each_time)
            dict_temp[each_id]  = get_temperature(dict_temp.get(each_id, 22))
 
            if dict_temp[each_id] <=5:
                dict_temp[each_id] += 4
            if dict_temp[each_id] >= 80:
                dict_temp[each_id] -= 3
 
 
 
            data = {
                    'id': each_id,
                    'area': area,
                    'time': time_stamp,
                    'temperature': dict_temp.get(each_id, 22)
                    }
                    
            #json_data = json.dumps(data)
            res = put_records_to_stream (data)
            print(res)
        print(res)
                    
        time.sleep(1)
            
            
        
        
def lambda_handler(event, context):
    
    
    main()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
