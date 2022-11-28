import ast

from Aux_funcs import fix_db
from drivers.rabbit_driver import rabbit_driver


def main():
    print(' [*] Waiting for messages.')
    for method_frame, properties, body in rabbit_driver.channel.consume(rabbit_driver.routing_key):
        body = body.decode('utf-8')
        body = ast.literal_eval(body)
        fix_db(body)
        rabbit_driver.channel.basic_ack(method_frame.delivery_tag)


if __name__ == '__main__':
    main()
