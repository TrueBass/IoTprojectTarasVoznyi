import asyncio
from asyncua import Client, ua


class Machine:
    def __init__(self, client, node_name):
        self.client = client
        self.node_name = node_name
        self.production_status = None
        self.workorder_id = None
        self.production_rate = None
        self.good_count = None
        self.bad_count = None
        self.temperature = None
        self.device_error = None

    def __str__(self):
        return (f"""
{str(self.node_name)[7:]}:
Production Status: {self.production_status}
Work order ID: {self.workorder_id}
Production Rate: {self.production_rate}
Good Count: {self.good_count}
Bad Count: {self.bad_count}
Temperature: {self.temperature}
Device Error : {self.device_error}""")

    async def insert_data(self):
        self.production_status = await self.client.get_node(f"{self.node_name}/ProductionStatus").get_value()
        self.workorder_id = await self.client.get_node(f"{self.node_name}/WorkorderId").get_value()
        self.production_rate = await self.client.get_node(f"{self.node_name}/ProductionRate").get_value()
        self.good_count = await self.client.get_node(f"{self.node_name}/GoodCount").get_value()
        self.bad_count = await self.client.get_node(f"{self.node_name}/BadCount").get_value()
        self.temperature = await self.client.get_node(f"{self.node_name}/Temperature").get_value()
        self.device_error = await self.client.get_node(f"{self.node_name}/DeviceError").get_value()
        self.device_error = [int(num) for num in bin(self.device_error)[2:].zfill(4)]

    def data(self):
        return {
            "device_name": str(self.node_name)[7:],
            "production_status": int(self.production_status),
            "workorder_id": self.workorder_id,
            "production_rate": self.production_rate,
            "good_count": self.good_count,
            "bad_count": self.bad_count,
            "temperature": self.temperature,
            "device_error": self.device_error
        }

    def get_errors(self):
        return self.device_error

    async def emergency_stop_trigger(self):
        emergency_stop_method = self.client.get_node(f"{self.node_name}/EmergencyStop")
        await self.node_name.call_method(emergency_stop_method)