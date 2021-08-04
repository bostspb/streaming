from kafka import KafkaProducer
import sys

def upload_orders_to_kafka(host, topic, orders):
    producer = KafkaProducer(bootstrap_servers=host)
    orders_topic_name = topic
    for order in orders:
        producer.send(orders_topic_name, bytes(order, 'utf-8'))


def read_orders(orders_file_path):
    with open(orders_file_path, 'r') as f:
        orders_json = f.readlines()

    return orders_json


if __name__ == '__main__':
    args = sys.argv[1:]

    # arg 1 - orders file path
    orders_file_path = args[0]
    print("Orders file path: {}".format(orders_file_path))

    # arg 2 - kafka host
    kafka_host = args[1]
    print("Kafka host: {}".format(kafka_host))

    # arg 3 - kafka target orders topic
    kafka_target_topic = args[2]
    print("Kafka topic: {}".format(kafka_target_topic))

    # read orders json file
    orders = read_orders(orders_file_path)
    # upload orders to kafka
    upload_orders_to_kafka(kafka_host, kafka_target_topic, orders)
