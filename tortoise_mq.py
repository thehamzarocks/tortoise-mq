#!/usr/bin/env python
import pika
import json
from datetime import datetime
from elasticsearch import Elasticsearch
es = Elasticsearch()


class TortoiseMQ:
	"""
	Wraps the body with the tmq parameters like status, id, error.
	Sends to the default queue on the default channel
	Messages sent this way need to be consumed by tmq_consume
	"""
	def tmq_produce(self, message, channel):
		tmq_message = {
			'id': 'unassigned',
			'queue_name': 'task_queue',
			'error_count': 0,
			'status': 'pending',
			'message': message,
			'errorMessage': ''
		}

		channel.basic_publish(exchange='',
					routing_key='task_queue',
					body=json.dumps(tmq_message),
					properties=pika.BasicProperties(
						delivery_mode=2
						)
				)

	def __tmq_callback_wrapper(self, channel, method, properties, tmq_message, original_callback):
		tmq_message = json.loads(tmq_message.decode('ascii'))
		print("Executing callback wrapper")
		tmq_response_obj = original_callback(channel, method, properties, tmq_message['message'].encode('utf-8'))
		channel.basic_ack(delivery_tag=method.delivery_tag)
		if(tmq_response_obj['status'] == 'success'):
			# update existing log with success if present
			if(tmq_message['id'] != 'unassigned'):
				tmq_message['status'] = 'success'
				print("The id is %r" % tmq_message['id'])
				es.update(index='tmq-logs', id=tmq_message['id'], body={"doc": tmq_message})
				print("success")

		if tmq_response_obj['status'] == 'error':
			self.__tmq_log_error(tmq_message, tmq_response_obj['error_message'], channel, method)



	"""
	Use this method to consume messages when using TortoiseMQ
	This abstracts away the wrappers added by TortoiseMQ so you can
	focus on processing the actual message
	Unlike RabbitMQ, the callback provided to TortoiseMQ needs to return
	a response object of the form {'status': 'success', 'error_message': ''}
	Returning this object sends an acknowledgement to the channel so no
	separate acknowledgement is needed
	"""
	def tmq_basic_consume(self, channel, callback):
		channel.basic_consume(queue='task_queue', on_message_callback=lambda ch, method, properties, body: self.__tmq_callback_wrapper(ch, method, properties, body, callback))



	def __tmq_log_error(self, tmq_message, error_message, channel, method):
		print(" [x] TortoiseMQ: Error processing message %r" % tmq_message)
		""" log the error into elastic along with the required metadata
		"""
		tmq_message['errorMessage'] = error_message
		tmq_message['status'] = 'error'
		# if the id has been assigned then we need to update an existing entry
		if tmq_message['id'] != 'unassigned':
			print("Updating existing error count %r" % tmq_message['error_count'])
			tmq_message['error_count'] = tmq_message['error_count'] + 1
			es.update(index='tmq-logs', id=tmq_message['id'], body={"doc": tmq_message})
			return 	

		# otherwise add a new entry to the logs
		tmq_message['error_count'] = 1
		insert_response = es.index(index='tmq-logs', body=tmq_message)
		print(insert_response)
		tmq_message['id'] = insert_response['_id']
		es.update(index='tmq-logs', id=insert_response['_id'], body={"doc": tmq_message})
		return

	
	def __publish_tmq_message(self, tmq_message, channel):
		channel.basic_publish(exchange='',
			routing_key='task_queue',
			body=json.dumps(tmq_message),
			properties=pika.BasicProperties(
				delivery_mode=2
				)
		)



		



	def tmq_retrigger(self, channel):
		connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
		channel = connection.channel()

		channel.queue_declare(queue='task_queue', durable=True)
		channel.basic_qos(prefetch_count=1) 
		res = es.search(index="tmq-logs", body={"query": {"bool": { "must": [{"match": {"errorMessage": "work crash!"}}, { "match": { "status": "error!" }}]}}})
		print(res)
		[self.__publish_tmq_message(tmq_message['_source'], channel=channel) for tmq_message in res['hits']['hits']]
		connection.close()


