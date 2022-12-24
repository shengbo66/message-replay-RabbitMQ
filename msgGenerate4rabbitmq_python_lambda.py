from __future__ import print_function
import pika
import ssl
import boto3
import json
import base64 
import os
import datetime
import time

client = boto3.client('secretsmanager')
brokerArn = os.environ['BrokerArn']
response = client.get_secret_value(
  SecretId=os.environ['SecretManagerArn']
)

userDetails = json.loads(response['SecretString'])
credentials = pika.PlainCredentials(userDetails['user'], userDetails['pass'])
context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)

brokerHost = "{}.mq.{}.amazonaws.com".format(brokerArn.split(':')[-1],os.environ['AWS_REGION'])
print(brokerHost)
cp = pika.ConnectionParameters(
  port=5671,
  host=brokerHost,
  credentials=credentials,
  ssl_options=pika.SSLOptions(context)
)


def lambda_handler(event, context):
    print(event)

    connection = pika.BlockingConnection(cp)
    channel = connection.channel()
    dt = datetime.datetime.now()
    ts = dt.timestamp()
    basic_no= int(ts)
    # 发送一组测试消息，以便能够在 replay 队列中有消息缓存,需要发送的消息数量来自环境变量 msgNo
    msgNo = os.environ ['msgNo']
    for i in range(int(msgNo)):
      print (f"Msg # {i}")
      dt = datetime.datetime.now()
      ts = dt.timestamp()
      print(dt) # datetime.datetime(2019, 9, 11, 11, 20, 6, 681320)
      print(ts) # 1568172006.68132
      #pika.spec.BasicProperties(content_type=None, content_encoding=None, headers=None, delivery_mode=None, priority=None, correlation_id=None, reply_to=None, expiration=None, message_id=None, timestamp=None, type=None, user_id=None, app_id=None, cluster_id=None)
      mypro = pika.spec.BasicProperties(timestamp=int(ts),message_id=str(basic_no+i))
      #msgbody=f{‘replay msg #'} + {i}‘
      msgbody='replay message! #'
      msgbody = msgbody + str(i)
      channel.basic_publish(exchange="pd.fanout", routing_key="OrderProcessing", body=json.dumps(msgbody),properties=mypro)
      #time.sleep（0.1）

    connection.close()
