import pika
import sys
import os

def main():
    # 1. Connect to RabbitMQ
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # 2. Declare the queue (good practice in case consumer starts before producer)
    channel.queue_declare(queue='hello')

    # 3. Define what to do when a message arrives
    def callback(ch, method, properties, body):
        print(f" [x] Received {body.decode()}")

    # 4. Tell RabbitMQ to use our callback function for the 'hello' queue
    channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)