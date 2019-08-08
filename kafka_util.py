import json
from threading import Thread
import logging
import time
import traceback
from kafka import KafkaClient, KafkaProducer, KafkaConsumer

# logging.basicConfig(level=logging.DEBUG)


class BFKafkaProducer(Thread):

    def __init__(self, server):
        super(BFKafkaProducer, self).__init__()
        self._producer = KafkaProducer(bootstrap_servers=server, value_serializer=lambda item: json.dumps(item).encode(encoding="utf-8"))

    def run(self) -> None:
        count = 0
        while True:
            topic = "kfk_test"
            msg_dict = {"name": "who", "password": 234}
            count += 1
            msg_dict["count"] = count
            result = self.put(topic, msg_dict)
            time.sleep(1)
            print(f"put {msg_dict} result: {result}")

    def put(self, topic, msg, partition=None):
        try:
            future = self._producer.send(topic, msg, partition=partition)
        # sumary = self._producer.metrics()
        # print(f"sumary:{sumary}")
            return future.get(10)
        except:
            traceback.print_exc()
            return None


class BFKafkaClient(object):
    def __init__(self):
        pass


class BFKafkaConsumer(Thread):
    def __init__(self, server, topic):
        super(BFKafkaConsumer, self).__init__()
        self._consumer = KafkaConsumer(topic, bootstrap_servers=server, group_id="test", auto_offset_reset="latest"
                                       , enable_auto_commit=True, auto_commit_interval_ms=3000
                                       , consumer_timeout_ms=10000, value_deserializer=json.loads)

    def run(self) -> None:
        topic = "kfk_test"
        self.subscribe(topic)

    def subscribe(self, topic):
        self._consumer.subscribe(topic)
        partions = self._consumer.partitions_for_topic(topic)
        print("partions:", partions, type(partions))
        while True:
            msg = self._consumer.poll(timeout_ms=50)
            time.sleep(5)
            for key, item in msg.items():
                for sub_item in item:
                    print(f"partion:{key.partition}, value:{sub_item.value}")
            #print(f"msg:{msg} types:{type(msg)}")

    def consumer(self):
        for item in self._consumer:
            print(f"items:{item.value}")
        # metrics = self._consumer.metrics()
        # print(f"summary:{metrics}")


if __name__ == "__main__":
    # server = ["10.160.34.113:2181", "10.160.34.114:2181", "10.160.34.147:2181"]
    server = ["10.160.34.113:9092", "10.160.34.114:9092", "10.160.34.147:9092"]
    # server = ["10.160.34.113:9092"]
    producer = BFKafkaProducer(server)
    topic = "kfk_test"
    msg_dict = {"name": "who", "password": 234}
    partition = 0
    consumer1 = BFKafkaConsumer(server, topic)
    consumer2 = BFKafkaConsumer(server, topic)
    consumer3 = BFKafkaConsumer(server, topic)

    # producer.start()
    consumer1.start()
    consumer2.start()
    consumer3.start()
    # producer.join()
    consumer1.join()
    consumer2.join()
    consumer3.join()

    count = 0
    while True:
        count += 1
        msg_dict["count"] = count
        msg = json.dumps(msg_dict).encode(encoding="utf-8")
        result = producer.put(topic, msg, partition)
        print(f"kafka producer data:{msg_dict} result:{result}")
        consumer1.subscribe(topic)
        # consumer.consumer()
