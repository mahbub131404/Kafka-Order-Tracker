from confluent_kafka import Consumer, Producer
import json
import time
from db import init_db, update_order_status

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "delivery-service-group",
    "auto.offset.reset": "earliest",
}

producer_config = {"bootstrap.servers": "localhost:9092"}

consumer = Consumer(consumer_config)
producer = Producer(producer_config)

init_db()
consumer.subscribe(["orders"])

print("[delivery-service] Waiting for ORDER_PAID events...")


def send_event(event_type, order):
    payload = {
        "event_type": event_type,
        "order_id": order["order_id"],
        "user": order.get("user"),
        "item": order.get("item"),
        "quantity": order.get("quantity"),
        "amount": order.get("amount"),
        "status": order.get("status"),
        "source": "delivery-service",
    }
    value = json.dumps(payload).encode("utf-8")
    key = payload["order_id"].encode("utf-8")
    producer.produce(topic="orders", key=key, value=value)
    producer.flush()


try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("[delivery-service] Consumer error:", msg.error())
            continue

        event = json.loads(msg.value().decode("utf-8"))

        if event.get("event_type") == "ORDER_PAID":
            order_id = event["order_id"]
            print(f"[delivery-service] Received ORDER_PAID for {order_id}. Assigning courier...")

            # OUT_FOR_DELIVERY
            update_order_status(order_id, "OUT_FOR_DELIVERY")
            event["status"] = "OUT_FOR_DELIVERY"
            send_event("ORDER_OUT_FOR_DELIVERY", event)
            print(f"[delivery-service] Order {order_id} OUT_FOR_DELIVERY")

            # simulate delivery time
            time.sleep(5)

            # DELIVERED
            update_order_status(order_id, "DELIVERED")
            event["status"] = "DELIVERED"
            send_event("ORDER_DELIVERED", event)
            print(f"[delivery-service] Order {order_id} DELIVERED")

except KeyboardInterrupt:
    print("[delivery-service] Stopping...")
finally:
    consumer.close()
