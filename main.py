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


async def main():
    #   OPC UA server connection
    client = Client("opc.tcp://localhost:4840/")
    await client.connect()
    #   IoT device connection
    connection_string = "HostName=iot-hub-taras-ul.azure-devices.net;DeviceId=testDevice;SharedAccessKey=K54t01Bf5sfFuVFBHiEz3GPNUOZ6AbfDuAIoTL9f1p4="
    iot_device_client = IoTHubDeviceClient.create_from_connection_string(connection_string)
    iot_device_client.connect()

    # num_of_messages = 10
    machines_list = []
    old_errors = [[0,0,0,0],[0,0,0,0]]

    while True:
        devices_list = await client.get_objects_node().get_children()
        devices_list = devices_list[1:]

        for device in devices_list:
            machine = Machine(client, device)
            await machine.insert_data()
            machines_list.append(machine)

        for i in range(len(machines_list)):
            if machines_list[i].get_errors() != [0,0,0,0] and machines_list[i].get_errors() != old_errors[i]:
                send_messages_device(iot_device_client, machines_list[i])
                old_errors[i] = machines_list[i].get_errors()
            if machines_list[i].get_errors() == [0,0,0,0]:
                old_errors[i] = machines_list[i].get_errors()

        # if all(machine.get_errors() == [0,0,0,0] for machine in machines_list):
        #     old_errors.clear()
        # print(machines_list)
        #
        # machines_list = list(filter(lambda m: m.get_errors() != [0, 0, 0, 0], machines_list))
        #
        # if len(machines_list) != 0:
        #     for machine in machines_list:
        #         if machine.get_errors() not in old_errors:
        #             old_errors.append(machine.get_errors())
        #             send_messages_device(iot_device_client, machine)

        machines_list.clear()
        time.sleep(1)

if __name__ == '__main__':
    asyncio.run(main())
