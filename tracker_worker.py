from confluent_kafka import Consumer
import json

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "tracker-service-group",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_config)
consumer.subscribe(["orders"])

print("[tracker] Listening for order events...")


try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("[tracker] Consumer error:", msg.error())
            continue

        event = json.loads(msg.value().decode("utf-8"))
        etype = event.get("event_type")
        oid = event.get("order_id")
        status = event.get("status")
        user = event.get("user")
        item = event.get("item")
        qty = event.get("quantity")

        if etype == "ORDER_PAID":
            print(f"[tracker] ✅ {user} paid for {qty} x {item} (order {oid}).")
        elif etype == "ORDER_OUT_FOR_DELIVERY":
            print(f"[tracker] 🚚 Order {oid} is out for delivery.")
        elif etype == "ORDER_DELIVERED":
            print(f"[tracker] 🎉 Order {oid} has been delivered to {user}.")
        else:
            print(f"[tracker] ℹ️ {etype} for order {oid} (status={status}).")

except KeyboardInterrupt:
    print("[tracker] Stopping...")
finally:
    consumer.close()
