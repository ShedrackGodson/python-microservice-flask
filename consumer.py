import json

import pika
from dotenv import load_dotenv
import os

from main import Product, db

load_dotenv('.env')

params = pika.URLParameters(os.getenv('AMQP_URL'))

connection = pika.BlockingConnection(params)

channel = connection.channel()

channel.queue_declare(queue='main')


def callback(ch, method, properties, body):
    print("Received in main")
    data = json.loads(body)
    print(data)

    if properties.content_type == "product_created":
        product = Product(id=data["id"], title=data["title"], image=data["image"])
        db.session.add(product)
        db.session.commit()
        print("Product Created")

    elif properties.content_type == "product_updated":
        product = Product.query.get(data['id'])
        product.title = data['title']
        product.image = data['image']
        db.session.commit()
        print("Product Updated")

    elif properties.content_type == "product_deleted":
        product = Product.query.get(data)  # Because here ID is only received
        print(product)
        db.session.delete(product)
        db.session.commit()
        print("Product Deleted")


channel.basic_consume(queue='main', on_message_callback=callback, auto_ack=True)

print("Started consuming")

channel.start_consuming()

channel.close()
