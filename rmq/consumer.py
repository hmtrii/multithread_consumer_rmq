import threading
import functools

import pika
from pika.exchange_type import ExchangeType


class Consumner:

    def __init__(self, rmq_host, rmq_port, rmq_username, rmq_password, exchange_name, queue_name, routing_key, num_threads, job):
        self._rmq_host = rmq_host
        self._rmq_port = rmq_port
        self._rmq_username = rmq_username
        self._rmq_password = rmq_password
        self._exchange_name = exchange_name
        self._queue_name = queue_name
        self._routing_key = routing_key
        self._num_threads = num_threads
        self._job = job
        self._hearbeat = 5
    
    @property
    def credentials(self):
        return pika.PlainCredentials(self._rmq_username, self._rmq_password)
    
    @property
    def connection_params(self):
        return pika.ConnectionParameters(
            self._rmq_host, self._rmq_port,
            credentials=self.credentials, heartbeat=self._hearbeat)

    @property
    def connection(self):
        return pika.BlockingConnection(self.connection_params)

    def do_work(self, ch, delivery_tag, body):
        thread_id = threading.get_ident()
        print("Thread id: {} Delivery tag: {} Message body: {}".format(thread_id,
                    delivery_tag, body))
        ### Call custom job here
        self._job(body)
        cb = functools.partial(self.ack_message, ch, delivery_tag)
        ch.connection.add_callback_threadsafe(cb)
    
    def on_message(self, ch, method_frame, _header_frame, body, args):
        thrds = args
        delivery_tag = method_frame.delivery_tag
        t = threading.Thread(target=self.do_work, args=(ch, delivery_tag, body))
        t.start()
        thrds.append(t)
    
    def ack_message(self, ch, delivery_tag):
        """Note that `ch` must be the same pika channel instance via which
        the message being ACKed was retrieved (AMQP protocol constraint).
        """
        thread_id = threading.get_ident()
        print("Thread id: {} Delivery tag: {} Message body: {}".format(thread_id,
                    delivery_tag, "send ack message"))
        if ch.is_open:
            ch.basic_ack(delivery_tag)
        else:
            # Channel is already closed, so we can"t ACK this message;
            # log and/or do something that makes sense for your app in this case.
            pass
    
    def run(self):
        while True:
            try:
                threads = []
                on_message_callback = functools.partial(self.on_message, args=(threads))
                channel = self.connection.channel()
                channel.exchange_declare(
                    exchange=self._exchange_name,
                    exchange_type=ExchangeType.topic,
                )
                channel.queue_declare(queue=self._queue_name, exclusive=False)
                channel.queue_bind(
                    queue=self._queue_name,
                    exchange=self._exchange_name, 
                    routing_key=self._routing_key)
                channel.basic_qos(prefetch_count=self._num_threads)
                channel.basic_consume(on_message_callback=on_message_callback, queue="")

                try:
                    channel.start_consuming()
                except KeyboardInterrupt:
                    channel.stop_consuming()

                for thread in threads:
                    thread.join()

                self.connection.close()
            except pika.exceptions.ConnectionClosedByBroker:
                # Uncomment this to make the example not attempt recovery
                # from server-initiated connection closure, including
                # when the node is stopped cleanly
                #
                # break
                continue
            # Do not recover on channel errors
            except pika.exceptions.AMQPChannelError as err:
                print("Caught a channel error: {}, stopping...".format(err))
                break
            # Recover on all other connection errors
            except pika.exceptions.AMQPConnectionError:
                print("Connection was closed, retrying...")
                continue