from __future__ import print_function
import pika
import ssl
import boto3
import json
import base64 
import os

client = boto3.client('secretsmanager')
brokerArn = os.environ['BrokerArn']
response = client.get_secret_value(
  SecretId=os.environ['SecretManagerArn']
)
queue_name = os.environ ['queue_name']
userDetails = json.loads(response['SecretString'])
credentials = pika.PlainCredentials(userDetails['user'], userDetails['pass'])
context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)

msgNofiler = os.environ ['msgNofiler']
replayMaxNo = int(os.environ ['replayMaxNo'])
replayNo = 0

brokerHost = "{}.mq.{}.amazonaws.com".format(brokerArn.split(':')[-1],os.environ['AWS_REGION'])
print(brokerHost)
cp = pika.ConnectionParameters(
  port=5671,
  host=brokerHost,
  credentials=credentials,
  ssl_options=pika.SSLOptions(context)
)
connection = pika.BlockingConnection(cp)
sent_channel = connection.channel() # 收发消息使用不同的 channel  ：separate the send / consume channel
consume_channel = connection.channel() # 收发消息使用不同的 channel  ：separate the send / consume channel

def pdf_process_function(properties,msg):
    global replayNo
    print(" replay processing")
    #print(properties)
    print(" [x] Received " + str(properties)+str(msg)) #[x] Received <BasicProperties(['message_id=1671019886', 'timestamp=1671019886'])>b'"replay message! #0"'
    
    #过滤消息. filter the massage
    message_id = properties.message_id
    print (f'message_id is : {message_id}, filter is {msgNofiler}')
    if properties.type== 'replayed' or int(message_id) < int(msgNofiler) :
        print(f"!!! ignore the msg with message_id {message_id}, {properties.type}")
        return
    
    #为消息添加 replay 标识
    mprop = properties
    mprop.type = str('replayed')
    
    #将消息回放至 生产队列 replay message back to the production exchange
    print(f"###replay the msg with message_id {message_id}")
    sent_channel.basic_publish(exchange="pd.fanout", routing_key="OrderProcessing", body=(msg), properties=mprop)
    replayNo += 1
    print(f"=== replay msg no：{replayNo}, max replay no is: {replayMaxNo}");
    if replayNo >= replayMaxNo :
        connection.close()
        return {
            'statusCode': 200
        }
        exit() #完成消息回放，退出
    return

# create a function which is called on incoming messages
def callback(ch, method, properties, body):
    pdf_process_function(properties,body)

def lambda_handler(event, context):
    print(event)
    print(f'queue_name is {queue_name}')
    
    argu = {'x-queue-mode':'lazy','x-queue-type':'classic'}
    consume_channel.queue_declare(queue=queue_name,durable=1,arguments=argu)
    
    # set up subscription on the queue
    consume_channel.basic_consume('',
        callback,
        auto_ack=True)
    consume_channel.start_consuming()
    connection.close()
    return {
        'statusCode': 200
    }
