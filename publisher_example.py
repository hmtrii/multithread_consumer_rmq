#!/usr/bin/env python
import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='topic_job', exchange_type='topic')

# routing_key = sys.argv[1] if len(sys.argv) > 2 else 'anonymous.info'
routing_key = "test.threaded_consumer"
message = ' '.join(sys.argv[1:]) or 'Hello World!'
channel.basic_publish(
    exchange='topic_job', routing_key=routing_key, body=message)
print(" [x] Sent %r:%r" % (routing_key, message))
connection.close()