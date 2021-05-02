#!/usr/bin/env python
import sys
import pika
from tortoise_mq import TortoiseMQ


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.basic_qos(prefetch_count=1)
channel.queue_declare('task_queue', durable=True)

# there is either a default message or the one entered on the command line
message = ' '.join(sys.argv[1:]) or 'Default Message'

tmq = TortoiseMQ()
tmq.tmq_produce(message, channel)
print(" [x] Sent %r" % message)


