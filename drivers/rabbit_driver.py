import ast

import pika

from Aux_funcs import fix_db


def callback(ch, method, properties, body):
    body = body.decode('utf-8')
    body = ast.literal_eval(body)
    fix_db(body)


class RabbitDriver:
    def __init__(self):
        rabbit_connection = pika.BlockingConnection(pika.ConnectionParameters(host='some-rabbit', port=5672))
        self.channel = rabbit_connection.channel()
        self.channel.queue_declare(queue='load')
        self.routing_key = 'load'
        self.channel.basic_consume(queue='load', on_message_callback=callback, auto_ack=True)


rabbit_driver = RabbitDriver()
