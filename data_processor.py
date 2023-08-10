import json
import boto3
import os

kinesis_client = boto3.client('kinesis')
sns_client = boto3.client('sns')
sns_ARN = os.environ.get('sns_ARN')


threshold = int (os.environ.get('threshold'))

device_tempratures = {}
consecutive_readings = int (os.environ.get('consecutive_readings'))

def extract_data (data):
    extracted_dict = json.loads (data)
    mac = extracted_dict ['id']
    time = extracted_dict ['time']
    temperature  =  int (extracted_dict ['temperature'])
    process_data (mac, time, temperature)
    

def process_data (mac, time, temperature):
    # initializing the high temp count for all IDs
    if mac not in device_tempratures:
        device_tempratures [mac] = 0

    if temperature > threshold:
        device_tempratures [mac] += 1
        print (device_tempratures)
    else:
        device_tempratures [mac] = 0
        
        
    if device_tempratures [mac] >= consecutive_readings:
        print (f'this {mac} bitch is on fire')
        generate_alarm(mac,temperature)
        device_tempratures [mac] = 0
            

def generate_alarm(mac,temperature):
   
    message = f'Temperature threshold reached for device {mac}. Temperature: {temperature}°C'
    response = sns_client.publish(
        TopicArn=sns_ARN,
        Message=message,
        Subject="High Temperature Alarm"
    )
    print(f"Alarm triggered for device {mac}. SNS MessageId: {response['MessageId']}")


def get_records():
  
    
    stream_name  = os.environ.get('stream_name')
    stream_ARN = os.environ.get('stream_ARN')
    iterator_type = os.environ.get('iterator_type')
    
    get_shard_iterator = kinesis_client.get_shard_iterator(
        StreamName=stream_name,
        ShardId='shardId-000000000000', 
        ShardIteratorType=iterator_type
    )

    shard_iterator = get_shard_iterator ['ShardIterator']
    response = kinesis_client.get_records(
        ShardIterator=shard_iterator,
        Limit=1,
        StreamARN=stream_ARN 
    )
    
    
    data = response["Records"]
   
    records = response['Records']
    while "NextShardIterator" in response:
        
        data = response["Records"]
        if len(data) < 1:
            pass
        else:
            data = data[0]["Data"]
            extract_data (data)
            #print (data)
        response = kinesis_client.get_records( ShardIterator=response["NextShardIterator"],Limit = 1)
    

def lambda_handler(event, context):
 
    print ('Lambda Initiated \n')
    get_records()
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
