# message-replay-RabbitMQ



## Purpose

For the solutions want to migrate from RocketMQ to RabbitMQ. There is a message relay feature supported in RocketMQ while not in RabbitMQ. This project provide a low code message replay solution for RabbitMQ.

### 
Key features

1. use message_id in the properties for filtering
2. you can set the max number to be fileter, when the max number reached, the lambda function will be finished.

## Solution Design

[Image: Image.jpg]
Use the fan-out exchange to copy(route) the same message to cc_que1 and bak_cc_que1. The fan-out exchange will keep the messages are delivered to both two queues.
Setup the shovel plugin between source queue: bak_cc_que1 and the target:Operation (fan_out exchange). the message will be automatically sync between bak_cc_que1 and Replay(queue)
The message will be deleted from cc_que1 after consumed. If there is something wrong happened in the consuming micro-service. you can replay the message from replay queue. start the message replay lambda , it will consume the message in from the replay queue. The messages which match the filters will be tagged and put to the pd exchange again. The consuming micro-service can process the replay message without any code logic change. 


## Setup Env

### Exchange & Queues setup


[Image: Image.jpg]

[Image: Image.jpg]
[Image: Image.jpg][Image: Image.jpg]
### Shovel config

go to “Admin” → “Shovel Management”
[Image: Image.jpg]add a new shovel


[Image: Image.jpg]


### Testing

produce 1000 msg, message_id start from 1671028898
[Image: Image.jpg]consume the message in cc_queue1 (production queue)
[Image: Image.jpg]replay message_id from 1671029198, there should be 700 message within the scope. but we set replayMaxNo : 500;  it means only first 500 messages will be replayed.

