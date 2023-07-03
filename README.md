# AN EXAMPLE RABBITMQ MULTITHREAD CONSUMER
This repo assume a consumer that will consume messages from a queue and the worker will process them in a multithread way.

After the job is complete, the consumer send an ack message back to RabbitMQ.

The worker is an simple: ```src/worker/worker.py```.

## DEMO
### Pre-requisites:
- Python
- [Pika](https://github.com/pika/pika)
- Running RabbitMQ server

### How to run:
Configures in ```configs.json```

Consumer:

```python app.py```

Publisher: there is an example publisher in file: ```publisher_example.py```

```python publisher_example.py```
