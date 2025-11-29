from flask import Flask, render_template, request, redirect, url_for, flash
from confluent_kafka import Producer
import uuid
import json
from db import init_db, create_order, get_all_orders

producer_config = {"bootstrap.servers": "localhost:9092"}
producer = Producer(producer_config)

app = Flask(__name__)
app.secret_key = "dev-secret-change-me"

# Create DB tables if not exists
init_db()


@app.route("/", methods=["GET"])
def index():
    orders = get_all_orders()
    return render_template("index.html", orders=orders)


@app.route("/orders", methods=["POST"])
def create_order_route():
    user = request.form.get("user") or "anonymous"
    item = request.form.get("item") or "mushroom pizza"
    quantity = int(request.form.get("quantity") or 1)
    amount = float(request.form.get("amount") or 0.0)

    order_id = str(uuid.uuid4())
    status = "PAID"  # payment success (simulated)

    # Save to DB
    create_order(order_id, user, item, quantity, amount, status)

    # Build event and send to Kafka
    event = {
        "event_type": "ORDER_PAID",
        "order_id": order_id,
        "user": user,
        "item": item,
        "quantity": quantity,
        "amount": amount,
        "status": status,
        "source": "payment-service",
    }
    value = json.dumps(event).encode("utf-8")
    producer.produce(topic="orders", key=order_id.encode("utf-8"), value=value)
    producer.flush()

    flash(f"Order {order_id} created and payment successful. Sent to Kafka.")
    return redirect(url_for("index"))


if __name__ == "__main__":
    # Run web server on http://127.0.0.1:5000
    app.run(debug=True)
