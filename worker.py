#!/usr/bin/env python
import pika
import time
from tortoise_mq import TortoiseMQ 

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.basic_qos(prefetch_count=1)
channel.queue_declare('task_queue', durable=True)

# the workers are initially plagued with bad memories
# this can be fixed by giving them good work
have_happy_memories = False

tmq = TortoiseMQ()

"""
this gets executed whenever a message is consumed.
the consumer sleeps according to the number of '.'s in the message
if the message contains "work" and the worker doesn't have happy
memories, they fail to sleep.
You can give the worker happy memories by passing "food" in the message
"""
def callback(ch, method, properties, body):
	global have_happy_memories
	print(" [x] Received %r" % body)

	if(not have_happy_memories and body.decode('ascii').find('work') != -1):
		print(" [x] Oh no, more work!")
		"""  we notify tortoisemq that an error occured processing the 
			message for the given queue.
			the method will log the error and acknowledge completion
		"""
		return {'status': 'error', 'error_message': 'Received work, crash!'}

	if(not have_happy_memories and body.decode('ascii').find('food') != -1):
		print(" [x] Yay we have food now!")
		have_happy_memories = True
		return  {'status': 'success', 'error_message': ''}

	
	time.sleep(body.count(b'.'))
	print(" [x] Done")
	return  {'status': 'success', 'error_message': ''}


tmq.tmq_basic_consume(channel=channel, callback=callback)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

