import time


class Worker:

    def work(self, body):
        print("Worker is working: ", body)
        time.sleep(10)
