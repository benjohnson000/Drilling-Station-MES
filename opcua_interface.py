from opcua import Client, ua

ENDPOINT = "opc.tcp://192.168.0.1:4840"

NODE_TASK_CODE = 'ns=3;s="abstractMachine"."taskCode"'
NODE_AWAIT_APP = 'ns=3;s="abstractMachine"."awaitApp"'
NODE_APP_RUN   = 'ns=3;s="abstractMachine"."appRun"'
NODE_APP_DONE  = 'ns=3;s="abstractMachine"."appDone"'
NODE_RELEASE   = 'ns=3;s="abstractMachine"."release"'


class OPCUAInterface:
    def __init__(self, endpoint=ENDPOINT):
        self.client = Client(endpoint)

    def establish_connection(self):
        plc = OPCUAInterface()
        try:
            plc.connect()
            print("Connected to PLC")
            plc.print_status()
        except Exception as e:
            print("Error:", e)
        finally:
            plc.disconnect()
            print("Disconnected")

    def connect(self):
        self.client.connect()

    def disconnect(self):
        self.client.disconnect()

    def read_value(self, node_id):
        return self.client.get_node(node_id).get_value()

    def write_bool(self, node_id, value):
        self.client.get_node(node_id).set_value(
            ua.DataValue(ua.Variant(value, ua.VariantType.Boolean))
        )

    def write_byte(self, node_id, value):
        self.client.get_node(node_id).set_value(
            ua.DataValue(ua.Variant(value, ua.VariantType.Byte))
        )

    def get_await_app(self):
        return self.read_value(NODE_AWAIT_APP)

    def get_app_done(self):
        return self.read_value(NODE_APP_DONE)

    def get_task_code(self):
        return self.read_value(NODE_TASK_CODE)

    def get_app_run(self):
        return self.read_value(NODE_APP_RUN)

    def get_release(self):
        return self.read_value(NODE_RELEASE)

    def set_task_code(self, value):
        self.write_byte(NODE_TASK_CODE, value)

    def set_app_run(self, value):
        self.write_bool(NODE_APP_RUN, value)

    def set_release(self, value):
        self.write_bool(NODE_RELEASE, value)

    def print_status(self):
        print(f"awaitApp = {self.get_await_app()}")
        print(f"taskCode = {self.get_task_code()}")
        print(f"appRun   = {self.get_app_run()}")
        print(f"appDone  = {self.get_app_done()}")
        print(f"release  = {self.get_release()}")