"""
All OPC UA communication stays in this file.

Algorithm:
1. Connect to the PLC over OPC UA.
2. Read the important machine handshake tags.
3. Write the tags needed by the MES.
4. Support a simple simulation mode so the prototype can run without the lab PLC.
"""

from __future__ import annotations

import time
from typing import Any

from opcua import Client, ua

SIMULATE_PLC = False # Simulate PLC here to test outside of lab!!
ENDPOINT = "opc.tcp://172.21.3.1:4840"

# PLC CONTROL TAGS
NODE_TASK_CODE = 'ns=3;s="abstractMachine"."taskCode"'
NODE_AWAIT_APP = 'ns=3;s="abstractMachine"."awaitApp"'
NODE_APP_RUN = 'ns=3;s="abstractMachine"."appRun"'
NODE_APP_DONE = 'ns=3;s="abstractMachine"."appDone"'
NODE_RELEASE = 'ns=3;s="abstractMachine"."release"'
NODE_OPERATING_MODE = 'ns=3;s=OperatingMode'

# RFID TAGS
NODE_RFID_READ_EXECUTE = 'ns=3;s="LIOLink_RF200_ReadTag_DB"."execute"'
NODE_RFID_WRITE_EXECUTE = 'ns=3;s="LIOLink_RF200_WriteTag_DB"."execute"'
NODE_RFID_ADDR_TAG = 'ns=3;s="rfidControl"."addrTag"'
NODE_RFID_LENGTH = 'ns=3;s="rfidControl"."length"'
NODE_RFID_READ_DATA = 'ns=3;s="identData"."readData"'
NODE_RFID_WRITE_DATA = 'ns=3;s="identData"."writeData"'

class OPCUAInterface:
    def __init__(self, endpoint: str = ENDPOINT, simulate: bool = SIMULATE_PLC) -> None:
        self.endpoint = endpoint
        self.simulate = simulate
        self.client = None if simulate else Client(endpoint)
        self.connected = False

        self.sim_state = {
            "task_code": 0,
            "await_app": False,
            "app_run": False,
            "app_done": False,
            "release": False,
            "operating_mode": "Run",
            "carrier_present": False,
            "cycle_started_at": None,
            "carrier_interval": 6.0,
            "drill_time": 4.0,
            "last_carrier_change": time.time(),
            "rfid_data": [1, 0, 2, 0, 0, 0, 0, 0] + [0] * 24,
        }

######################### Connection #########################

    def connect(self) -> None:
        if self.simulate:
            self.connected = True
            return

        self.client.connect()
        self.connected = True

    def disconnect(self) -> None:
        if self.simulate:
            self.connected = False
            return

        if self.connected:
            self.client.disconnect()
            self.connected = False

######################### Read/Write Helpers #########################

    def _read_node(self, node_id: str) -> Any:
        return self.client.get_node(node_id).get_value()

    def _write_node(self, node_id: str, value: Any, variant_type: ua.VariantType) -> None:
        self.client.get_node(node_id).set_value(
            ua.DataValue(ua.Variant(value, variant_type))
        )

    def _write_bool(self, node_id: str, value: bool) -> None:
        if self.simulate:
            return
        self._write_node(node_id, bool(value), ua.VariantType.Boolean)

    def _write_byte(self, node_id: str, value: int) -> None:
        if self.simulate:
            return
        self._write_node(node_id, int(value), ua.VariantType.Byte)

    def _write_word(self, node_id: str, value: int) -> None:
        if self.simulate:
            return
        self._write_node(node_id, int(value), ua.VariantType.UInt16)

    def _write_byte_array(self, node_id: str, values: list[int]) -> None:
        if self.simulate:
            return
        self._write_node(node_id, values, ua.VariantType.Byte)

    def _pulse_bool(self, node_id: str, pulse_time: float = 0.2) -> None:
        if self.simulate:
            return

        self._write_bool(node_id, False)
        time.sleep(0.05)
        self._write_bool(node_id, True)
        time.sleep(pulse_time)
        self._write_bool(node_id, False)

######################### Read PLC Tags #########################

    def read_snapshot(self) -> dict[str, Any]:
        if self.simulate:
            self._tick_simulation()
            return {
                "plc_connected": self.connected,
                "operating_mode": self.sim_state["operating_mode"],
                "task_code": self.sim_state["task_code"],
                "await_app": self.sim_state["await_app"],
                "app_run": self.sim_state["app_run"],
                "app_done": self.sim_state["app_done"],
                "release": self.sim_state["release"],
            }

        return {
            "plc_connected": self.connected,
            "operating_mode": str(self._read_node(NODE_OPERATING_MODE)),
            "task_code": int(self._read_node(NODE_TASK_CODE)),
            "await_app": bool(self._read_node(NODE_AWAIT_APP)),
            "app_run": bool(self._read_node(NODE_APP_RUN)),
            "app_done": bool(self._read_node(NODE_APP_DONE)),
            "release": bool(self._read_node(NODE_RELEASE)),
        }
    
    def _get_snapshot_value(self, key: str) -> Any:
        return self.read_snapshot()[key]

    def get_await_app(self) -> bool:
        return bool(self._get_snapshot_value("await_app"))

    def get_app_run(self) -> bool:
        return bool(self._get_snapshot_value("app_run"))

    def get_app_done(self) -> bool:
        return bool(self._get_snapshot_value("app_done"))

    def get_release(self) -> bool:
        return bool(self._get_snapshot_value("release"))

    def get_task_code(self) -> int:
        return int(self._get_snapshot_value("task_code"))

    def get_operating_mode(self) -> str:
        return str(self._get_snapshot_value("operating_mode"))

######################### Write PLC Tags #########################

    def set_task_code(self, value: int) -> None:
        if self.simulate:
            self.sim_state["task_code"] = int(value)
            return

        self._write_byte(NODE_TASK_CODE, value)

    def set_app_run(self, value: bool) -> None:
        if self.simulate:
            self.sim_state["app_run"] = bool(value)
            if value:
                self.sim_state["cycle_started_at"] = time.time()
                self.sim_state["app_done"] = False
            return

        self._write_bool(NODE_APP_RUN, value)

    def set_release(self, value: bool) -> None:
        if self.simulate:
            self.sim_state["release"] = bool(value)

            if value:
                self.sim_state["await_app"] = False
                self.sim_state["app_done"] = False
                self.sim_state["carrier_present"] = False
                self.sim_state["task_code"] = 0
                self.sim_state["last_carrier_change"] = time.time()

            return

        self._write_bool(NODE_RELEASE, value)

    def reset_outputs(self) -> None:
        self.set_app_run(False)
        self.set_release(False)

######################### RFID #########################

    def configure_rfid(self, addr_tag: int = 0, length: int = 32) -> None:
        if self.simulate:
            return

        self._write_word(NODE_RFID_ADDR_TAG, addr_tag)
        self._write_word(NODE_RFID_LENGTH, length)

    def read_rfid_tag(self, addr_tag: int = 0, length: int = 32) -> list[int]:
        if self.simulate:
            return self.sim_state["rfid_data"][:length]

        self.configure_rfid(addr_tag, length)
        self._pulse_bool(NODE_RFID_READ_EXECUTE)
        time.sleep(0.5)

        data = self._read_node(NODE_RFID_READ_DATA)
        return list(data)[:length]

    def write_rfid_tag(self, data: list[int], addr_tag: int = 0) -> None:
        if len(data) > 32:
            raise ValueError("RFID data cannot be longer than 32 bytes")

        padded_data = data + [0] * (32 - len(data))

        if self.simulate:
            self.sim_state["rfid_data"] = padded_data
            return

        self.configure_rfid(addr_tag, len(data))
        self._write_byte_array(NODE_RFID_WRITE_DATA, padded_data)
        self._pulse_bool(NODE_RFID_WRITE_EXECUTE)

    def build_rfid_payload(
        self,
        order_id: int,
        task_code: int,
        status_code: int = 0,
        quality_code: int = 0,
        pallet_id: int = 0,
    ) -> list[int]:
        payload = [0] * 32

        # order_id: bytes 0-1
        payload[0] = order_id & 0xFF
        payload[1] = (order_id >> 8) & 0xFF

        # task/status/quality: bytes 2-4
        payload[2] = task_code
        payload[3] = status_code
        payload[4] = quality_code

        # pallet_id: bytes 5-6
        payload[5] = pallet_id & 0xFF
        payload[6] = (pallet_id >> 8) & 0xFF

        return payload

    def decode_rfid_payload(self, data: list[int]) -> dict[str, int | str]:
        padded = list(data) + [0] * max(0, 32 - len(data))

        order_id = padded[0] | (padded[1] << 8)
        task_code = padded[2]
        status_code = padded[3]
        quality_code = padded[4]
        pallet_id = padded[5] | (padded[6] << 8)

        status_text_map = {
            0: "Unknown",
            1: "Complete",
            2: "In Progress",
            3: "Failed",
        }

        quality_text_map = {
            0: "Unknown",
            1: "Pass",
            2: "Fail",
        }

        return {
            "order_id": order_id,
            "task_code": task_code,
            "status_code": status_code,
            "status_text": status_text_map.get(status_code, f"Code {status_code}"),
            "quality_code": quality_code,
            "quality_text": quality_text_map.get(quality_code, f"Code {quality_code}"),
            "pallet_id": pallet_id,
        }
    
######################### Simulation Help #########################

    def _tick_simulation(self) -> None:
        now = time.time()

        if not self.sim_state["carrier_present"]:
            if now - self.sim_state["last_carrier_change"] >= self.sim_state["carrier_interval"]:
                self.sim_state["carrier_present"] = True
                self.sim_state["await_app"] = True
                self.sim_state["release"] = False

        if self.sim_state["app_run"] and self.sim_state["cycle_started_at"] is not None:
            if now - self.sim_state["cycle_started_at"] >= self.sim_state["drill_time"]:
                self.sim_state["app_done"] = True
                self.sim_state["app_run"] = False