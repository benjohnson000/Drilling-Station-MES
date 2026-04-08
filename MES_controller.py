"""
Controller / service layer for the MES.

Algorithm:
1. Poll the PLC state continuously.
2. Track machine state, cycle times, downtime, and RFID events.
3. When the PLC is waiting for an application command, read RFID and pull the next order.
4. Send the order task code and start the cycle.
5. When the PLC signals appDone, write RFID completion data, release the pallet, and complete one unit on the order.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Dict, Any, Optional

from model import MESDatabase
from opcua_interface import OPCUAInterface


def now_dt() -> datetime:
    return datetime.now()


def now_str() -> str:
    return now_dt().strftime("%Y-%m-%d %H:%M:%S")


class MESController:
    def __init__(self, db: MESDatabase, plc: OPCUAInterface) -> None:
        self.db = db
        self.plc = plc

        self.running = False
        self.thread: Optional[threading.Thread] = None

        self.current_order_id: Optional[int] = None
        self.current_task_code: Optional[int] = None
        self.current_cycle_start: Optional[datetime] = None

        self.last_state_label = "Starting"
        self.last_downtime_start: Optional[datetime] = None
        self.last_snapshot: Dict[str, Any] = {}
        self.last_log_time = 0.0

######################### Control #########################

    def start(self) -> None:
        self.plc.connect()
        self.plc.reset_outputs()

        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

        self.db.add_event("system", "MES controller started")

    def stop(self) -> None:
        self.running = False

        if self.thread is not None:
            self.thread.join(timeout=2.0)

        self.plc.disconnect()
        self.db.add_event("system", "MES controller stopped")

    def get_live_snapshot(self) -> Dict[str, Any]:
        snapshot = dict(self.last_snapshot) if self.last_snapshot else self.plc.read_snapshot()
        snapshot["active_order_id"] = self.current_order_id
        snapshot["state_label"] = self.last_state_label
        return snapshot

######################### Main Loop #########################

    def _loop(self) -> None:
        while self.running:
            try:
                snapshot = self.plc.read_snapshot()
                snapshot["active_order_id"] = self.current_order_id

                state_label = self._derive_state_label(snapshot)
                snapshot["state_label"] = state_label

                self._track_downtime(state_label)
                self._process_orders(snapshot)
                self._log_state_periodically(snapshot)

                self.last_snapshot = snapshot
                self.last_state_label = state_label

            except Exception as exc:
                self.db.add_event("error", f"MES loop error: {exc}")

            time.sleep(0.25)

######################### State Helpers #########################

    def _derive_state_label(self, snapshot: Dict[str, Any]) -> str:
        if not snapshot.get("plc_connected"):
            return "PLC Offline"

        operating_mode = str(snapshot.get("operating_mode"))
        if operating_mode not in ("8", "Run", "SimaticOperatingState.Run"):
            return f"PLC Not In Run ({operating_mode})"

        if snapshot.get("app_run"):
            return "Running"

        if snapshot.get("await_app"):
            return "Waiting For Order"

        if self.current_order_id is not None and not snapshot.get("await_app") and not snapshot.get("app_run"):
            return "Cycle Complete"

        return "Idle"

    def _track_downtime(self, state_label: str) -> None:
        downtime_states = {"Idle", "PLC Offline"}
        now = now_dt()

        if state_label in downtime_states:
            if self.last_downtime_start is None:
                self.last_downtime_start = now
                self.db.add_event("downtime_start", f"Downtime started: {state_label}")
        else:
            if self.last_downtime_start is not None:
                seconds = (now - self.last_downtime_start).total_seconds()
                self.db.add_event("downtime_end", f"Downtime ended after {seconds:.1f} s")
                self.last_downtime_start = None

######################### Order Processing #########################

    def _process_orders(self, snapshot: Dict[str, Any]) -> None:
        if self._should_start_order(snapshot):
            self._start_next_order()
            return

        if self._should_complete_order(snapshot):
            self._complete_current_order()

    def _should_start_order(self, snapshot: Dict[str, Any]) -> bool:
        return bool(snapshot.get("await_app")) and self.current_order_id is None

    def _should_complete_order(self, snapshot: Dict[str, Any]) -> bool:
        return bool(snapshot.get("app_done")) and self.current_order_id is not None

    def _start_next_order(self) -> None:
        order = self.db.get_next_pending_order()
        if order is None or order.status == "Completed":
            return

        # Read RFID when the carrier arrives
        try:
            rfid_data = self.plc.read_rfid_tag(addr_tag=0, length=8)
            self.db.add_rfid_event(
                order_id=order.order_id,
                operation="read",
                addr_tag=0,
                length=len(rfid_data),
                raw_data=rfid_data,
            )
            decoded = self.plc.decode_rfid_payload(rfid_data)

            self.db.add_event(
                "rfid_read",
                (
                    f"RFID read for order {order.order_id}: "
                    f"pallet_id={decoded['pallet_id']}, "
                    f"tag_order_id={decoded['order_id']}, "
                    f"task_code={decoded['task_code']}, "
                    f"status={decoded['status_text']}, "
                    f"quality={decoded['quality_text']}"
                ),
            )
        except Exception as exc:
            self.db.add_event("rfid_read_error", f"RFID read failed: {exc}")

        self.current_order_id = order.order_id
        self.current_task_code = order.task_code
        self.current_cycle_start = now_dt()

        self.db.start_order_if_needed(order.order_id)
        self.plc.set_task_code(order.task_code)
        self.plc.set_app_run(True)

        self.db.add_event(
            "order_start",
            f"Started order {order.order_id} with task code {order.task_code}",
        )

    def _complete_current_order(self) -> None:
        if self.current_order_id is None or self.current_task_code is None or self.current_cycle_start is None:
            return

        cycle_end = now_dt()
        cycle_seconds = (cycle_end - self.current_cycle_start).total_seconds()

        # Write RFID completion data before releasing the pallet
        try:
            pallet_id = self.current_order_id  # simple prototype choice; can be replaced later

            payload = self.plc.build_rfid_payload(
                order_id=self.current_order_id,
                task_code=self.current_task_code,
                status_code=1,
                quality_code=1,
                pallet_id=pallet_id,
            )
            self.plc.write_rfid_tag(payload, addr_tag=0)

            self.db.add_rfid_event(
                order_id=self.current_order_id,
                operation="write",
                addr_tag=0,
                length=len(payload),
                raw_data=payload,
            )
            decoded = self.plc.decode_rfid_payload(payload)

            self.db.add_event(
                "rfid_write",
                (
                    f"RFID written for order {self.current_order_id}: "
                    f"pallet_id={decoded['pallet_id']}, "
                    f"order_id={decoded['order_id']}, "
                    f"task_code={decoded['task_code']}, "
                    f"status={decoded['status_text']}, "
                    f"quality={decoded['quality_text']}"
                ),
            )
        except Exception as exc:
            self.db.add_event("rfid_write_error", f"RFID write failed: {exc}")

        # Release the pallet
        self.plc.set_release(True)

        self.db.add_cycle(
            order_id=self.current_order_id,
            task_code=self.current_task_code,
            cycle_start=self.current_cycle_start.strftime("%Y-%m-%d %H:%M:%S"),
            cycle_end=cycle_end.strftime("%Y-%m-%d %H:%M:%S"),
            cycle_seconds=cycle_seconds,
            result="Completed",
        )

        self.db.mark_one_part_completed(self.current_order_id)
        self.db.add_event(
            "order_complete",
            f"Completed one part for order {self.current_order_id}; cycle {cycle_seconds:.2f} s",
        )

        # Reset controller state
        finished_order_id = self.current_order_id
        self.current_order_id = None
        self.current_task_code = None
        self.current_cycle_start = None

        # Release is just a momentary command in the prototype
        time.sleep(0.2)
        self.plc.set_release(False)

        self.db.add_event("release", f"Pallet released for order {finished_order_id}")

######################### Logging #########################

    def _log_state_periodically(self, snapshot: Dict[str, Any]) -> None:
        now = time.time()
        if now - self.last_log_time >= 1.0:
            self.db.log_machine_state(snapshot)
            self.last_log_time = now