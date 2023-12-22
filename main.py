from machine_h import Machine, asyncio
from asyncua import Client, ua
from azure.iot.device import IoTHubDeviceClient, Message

import json
import time
# prod = await devices_list[0].get_child("ProductionRate")
#await client.set_values([client.get_node(prod)], [ua.DataValue(ua.Variant(int(10), ua.VariantType.Int32))])


def send_messages_device(device: IoTHubDeviceClient, machine: Machine):
    data = machine.data()
    data = json.dumps(data)

    message = Message(data.encode('utf-8'))

    device.send_message(message)


async def main():
    #  OPC UA server connection
    client = Client("opc.tcp://localhost:4840/")
    await client.connect()
    #  get all devices
    devices_list = await client.get_objects_node().get_children()
    devices_list = devices_list[1:]

    for device in devices_list:
        machine = Machine(client, device)
        await machine.insert_data()
        print(machine)

    connection_string = "HostName=iot-hub-taras-ul.azure-devices.net;DeviceId=testDevice;SharedAccessKey=K54t01Bf5sfFuVFBHiEz3GPNUOZ6AbfDuAIoTL9f1p4="

    iot_device_client = IoTHubDeviceClient.create_from_connection_string(connection_string)

    iot_device_client.connect()

    num_of_messages = 10

    while num_of_messages > 0:
        devices_list = await client.get_objects_node().get_children()
        devices_list = devices_list[1:]

        for device in devices_list:
            machine = Machine(client, device)
            await machine.insert_data()
            send_messages_device(iot_device_client, machine)

        time.sleep(1)
        num_of_messages -= 1

if __name__ == '__main__':
    asyncio.run(main())
