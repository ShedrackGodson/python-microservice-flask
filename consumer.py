import pika
from dotenv import load_dotenv
import os

load_dotenv('.env')

params = pika.URLParameters(os.getenv('AMQP_URL'))

connection = pika.BlockingConnection(params)

channel = connection.channel()

channel.queue_declare(queue='main')


def callback(ch, method, properties, body):
    print("Received in main")
    print(body)


channel.basic_consume(queue='main', on_message_callback=callback, auto_ack=True)

print("Started consuming")

channel.start_consuming()

channel.close()
