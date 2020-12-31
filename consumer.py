import pika

params = pika.URLParameters("amqps://ygfxrvwk:pRe_uyBY9BgrDQMbVyYJpn8qOosuVLHx@llama.rmq.cloudamqp.com/ygfxrvwk")

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
