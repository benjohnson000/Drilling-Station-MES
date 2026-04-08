"""
Flask dashboard for the Drilling Station MES prototype.

Algorithm:
1. Start the database, PLC interface, and MES controller.
2. Render a dashboard showing KPIs, machine state, orders, cycles, RFID activity, and recent events.
3. Allow the user to create manual or random orders from the web UI.
"""

from __future__ import annotations

import atexit

from flask import Flask, redirect, render_template, request, url_for

from model import MESDatabase
from opcua_interface import OPCUAInterface
from MES_controller import MESController


app = Flask(__name__)

db = MESDatabase("mes_prototype.db")
plc = OPCUAInterface()
controller = MESController(db, plc)


def parse_raw_rfid(raw_data: str) -> dict:
    try:
        values = [int(x.strip()) for x in raw_data.split(",") if x.strip() != ""]
        return plc.decode_rfid_payload(values)
    except Exception:
        return {
            "order_id": "-",
            "task_code": "-",
            "status_text": "Invalid",
            "quality_text": "Invalid",
            "pallet_id": "-",
        }


@atexit.register
def shutdown() -> None:
    try:
        controller.stop()
    except Exception:
        pass

    try:
        db.close()
    except Exception:
        pass


@app.route("/")
def index():
    kpis = db.get_kpis()
    machine_state = controller.get_live_snapshot()

    orders = db.list_orders()
    recent_events = db.get_recent_events(20)
    recent_cycles = db.get_recent_cycles(15)
    recent_rfid_events = db.get_recent_rfid_events(15)

    decoded_rfid_events = []
    for event in recent_rfid_events:
        decoded = parse_raw_rfid(event["raw_data"])
        decoded_rfid_events.append(
            {
                "timestamp": event["timestamp"],
                "order_id": event["order_id"],
                "operation": event["operation"],
                "addr_tag": event["addr_tag"],
                "length": event["length"],
                "raw_data": event["raw_data"],
                "decoded": decoded,
            }
        )

    plc_mode = "Simulation" if plc.simulate else "Physical PLC"

    return render_template(
        "index.html",
        kpis=kpis,
        machine_state=machine_state,
        orders=orders,
        recent_events=recent_events,
        recent_cycles=recent_cycles,
        decoded_rfid_events=decoded_rfid_events,
        plc_mode=plc_mode,
    )


@app.route("/create_order", methods=["POST"])
def create_order():
    task_code = int(request.form.get("task_code", "1"))
    quantity = int(request.form.get("quantity", "1"))

    db.create_order(task_code=task_code, quantity=quantity)
    db.add_event(
        "order_create",
        f"Created manual order: task_code={task_code}, quantity={quantity}",
    )

    return redirect(url_for("index"))


@app.route("/create_random_order", methods=["POST"])
def create_random_order():
    order_id = db.generate_random_order()
    db.add_event("order_create", f"Created random order {order_id}")
    return redirect(url_for("index"))


@app.route("/generate_random_orders", methods=["POST"])
def generate_random_orders():
    count = int(request.form.get("count", "3"))
    db.generate_random_orders(count)
    db.add_event("order_create", f"Generated {count} random orders")
    return redirect(url_for("index"))


if __name__ == "__main__":
    controller.start()
    app.run(debug=False, use_reloader=False, port=5000)