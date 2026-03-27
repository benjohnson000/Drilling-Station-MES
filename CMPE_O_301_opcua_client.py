from opcua import Client
	
class SubHandler:
	def datachange_notification(self, node, val, data):
		print("Data change event at ", node, ". New value = ", val, sep='')

if __name__ == "__main__":

	client = Client("opc.tcp://172.21.3.1:4840")
	
	try:
		client.connect()
		print("connected")

		# connect to client
		uri = "http://ubc.ca/cmpe_o_301"
		idx = client.get_namespace_index(uri)

		await_app_node = client.get_node('ns=3;s="abstractMachine"."awaitApp"')
		tag_value = await_app_node.get_value()

		
		# create the subhandler and subscribe
		handler = SubHandler()
		sub = client.create_subscription(500, handler)

		sub.subscribe_data_change(await_app_node)
		
		while True:
			print(f"{tag_value}")
			pass
			
	finally:
		client.disconnect()
		print("disconnected")