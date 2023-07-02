import json
from threading import Thread

from src.worker.woker import Worker
from rmq.consumer import Consumner


def get_configs():
    with open('./configs.json') as file:
        configs = json.load(file)
    return configs


class Controller:

    def __init__(self):
        self._configs = get_configs()
        self._worker = Worker()
        self._consumer = Consumner(
            self._configs['rmq_host'],
            self._configs['rmq_port'],
            self._configs['rmq_username'],
            self._configs['rmq_password'],
            self._configs['exchange_name'],
            self._configs['routing_key'],
            self._configs['num_threads'], 
            self._worker.work
        )

    def run(self):
        consume_thread = Thread(target=self._consumer.run, daemon=True)
        consume_thread.start()
        consume_thread.join()
