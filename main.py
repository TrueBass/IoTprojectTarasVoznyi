from machine_h import Machine, asyncio
from asyncua import Client, ua
from azure.iot.device import IoTHubDeviceClient, Message

import json
import time
# prod = await devices_list[0].get_child("ProductionRate")
# await client.set_values([client.get_node(prod)], [ua.DataValue(ua.Variant(int(10), ua.VariantType.Int32))])


def send_messages_device(device: IoTHubDeviceClient, machine: Machine):
    data = machine.data()
    data = json.dumps(data)
    message = Message(data)
    device.send_message(message)


async def reported_device_twin(client, machine):
    reported_properties = {machine.get_name(): {"ProductionRate": machine.production_rate, "Errors": machine.device_error}}
    client.patch_twin_reported_properties(reported_properties)


async def main():
    #   OPC UA server connection
    client = Client("opc.tcp://localhost:4840/")
    await client.connect()
    #   IoT device connection
    connection_string = "HostName=iot-hub-taras-ul.azure-devices.net;DeviceId=testDevice;SharedAccessKey=K54t01Bf5sfFuVFBHiEz3GPNUOZ6AbfDuAIoTL9f1p4="
    iot_device_client = IoTHubDeviceClient.create_from_connection_string(connection_string)
    iot_device_client.connect()

    machines_list = []
    old_errors = [[0,0,0,0],[0,0,0,0]]

    try:
        while True:
            # receiving data from server
            devices_list = await client.get_objects_node().get_children()
            devices_list = devices_list[1:]

            for device in devices_list:
                machine = Machine(client, device)
                await machine.insert_data()
                machines_list.append(machine)

            # sending data to IoT
            for machine in machines_list:
                send_messages_device(iot_device_client, machine)

            # sending
            for i in range(len(machines_list)):
                if machines_list[i].get_errors() != [0,0,0,0] and machines_list[i].get_errors() != old_errors[i]:
                    machines_list[i].device_error_state = True
                    send_messages_device(iot_device_client, machines_list[i])
                    old_errors[i] = machines_list[i].get_errors()
                if machines_list[i].get_errors() == [0,0,0,0]:
                    old_errors[i] = machines_list[i].get_errors()

            machines_list.clear()
            time.sleep(1)
    except KeyboardInterrupt:
        print("KeyboardInterrupt")

    await client.disconnect()
    iot_device_client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
