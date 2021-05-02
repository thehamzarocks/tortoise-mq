#!/usr/bin/env python
import pika
import sys
from tortoise_mq import TortoiseMQ

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=True)
channel.basic_qos(prefetch_count=1)


"""
pick up the messages matching the es query and push them
back onto the queue
"""


tmq = TortoiseMQ()
tmq.tmq_retrigger(channel)




"""
# there is either a default message or the one entered on the command line
message = ' '.join(sys.argv[1:]) or 'Default Message'

channel.basic_publish(exchange='',
                routing_key='task_queue',
                body=message,
                properties=pika.BasicProperties(
                        delivery_mode=2
                        )
        )


print(" [x] Sent %r" % message)
"""

connection.close()
