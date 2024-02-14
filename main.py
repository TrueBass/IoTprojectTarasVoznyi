from machine_h import Machine, asyncio
from asyncua import Client, ua
from azure.iot.device import IoTHubModuleClient, IoTHubDeviceClient, Message, IoTHubModuleClient, MethodResponse, MethodRequest
import json
import time
import asyncio
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


async def production_rate_comparison(twin_patch, machines_list):
    for machine in machines_list:
        name = machine.get_name()
        if name in twin_patch.keys() and twin_patch[name] is not None and twin_patch[name]["ProductionRate"] != machine.production_rate:
            await machine.reduce_production_rate()


async def desired_twin_receive(client, machines_list):

    def twin_patch_handler(twin_patch):
        try:
            print("Twin patch received")
            print(twin_patch)
            asyncio.run(production_rate_comparison(twin_patch, machines_list))
        except Exception as e:
            print(f"{str(e)}")

    try:
        client.on_twin_desired_properties_patch_received = twin_patch_handler
    except Exception as e:
        print(f"{str(e)}")


async def emergency_stop_async(opc_client, device_name):
    e_stop_method = opc_client.get_node(f"ns=2;s={device_name}/EmergencyStop")
    node = opc_client.get_node(f"ns=2;s={device_name}")
    await node.call_method(e_stop_method)
    print("Emergency stop called. Success")


async def res_errors_status_async(opc_client, device_name):
    res_errors_method = opc_client.get_node(f"ns=2;s={device_name}/ResetErrorStatus")
    node = opc_client.get_node(f"ns=2;s={device_name}")
    await node.call_method(res_errors_method)
    print("Reset error status called. Success")


async def take_direct_method(iot_client, opc_client):
    def handle_direct_method(request):
        try:
            print(f"Direct Method called: {request.name}")
            print(f"Request: {request}")
            print(f"Payload: {request.payload}")

            if request.name == "emergency_stop":
                device_name = request.payload["DeviceName"]
                asyncio.run(emergency_stop_async(opc_client, device_name))

            elif request.name == "reset_err_status":
                device_name = request.payload["DeviceName"]
                asyncio.run(res_errors_status_async(opc_client, device_name))

            response_payload = "Method has been executed successfully"
            response = MethodResponse.create_from_method_request(request, 200, payload=response_payload)
            print(f"Response: {response}")
            print(f"Payload: {response.payload}")
            iot_client.send_method_response(response)
            return response
        except Exception as e:
            print(f"Exception caught in handle_method: {str(e)}")

    try:
        iot_client.on_method_request_received = handle_direct_method
    except:
        pass


async def main():
    #   OPC UA server connection
    client = Client("opc.tcp://localhost:4840/")
    await client.connect()
    #   IoT device connection
    connection_string = "HostName=iot-hub-taras-ul.azure-devices.net;DeviceId=testDevice;SharedAccessKey=K54t01Bf5sfFuVFBHiEz3GPNUOZ6AbfDuAIoTL9f1p4="
    iot_device_client = IoTHubDeviceClient.create_from_connection_string(connection_string)
    iot_device_client.connect()

    twin = iot_device_client.get_twin()['reported']
    del twin["$version"]
    for key, value in twin.items():
        twin[key] = None
    iot_device_client.patch_twin_reported_properties(twin)

    machines_list = []
    old_errors = [[0, 0, 0, 0], [0, 0, 0, 0]]

    try:
        while True:
            # receiving data from server
            devices_list = await client.get_objects_node().get_children()
            devices_list = devices_list[1:]

            for device in devices_list:
                machine = Machine(client, device)
                await machine.insert_data()
                machines_list.append(machine)

            await desired_twin_receive(iot_device_client, machines_list)

            # sending data to IoT
            for machine in machines_list:
                if machine.device_error_state != True:
                    send_messages_device(iot_device_client, machine)

            # sending machines w/errors
            for i in range(len(machines_list)):
                if machines_list[i].get_errors() != [0, 0, 0, 0] and machines_list[i].get_errors() != old_errors[i]:
                    machines_list[i].device_error_state = True
                    send_messages_device(iot_device_client, machines_list[i])
                    old_errors[i] = machines_list[i].get_errors()
                if machines_list[i].get_errors() == [0, 0, 0, 0]:
                    machines_list[i].device_error_state = False
                    old_errors[i] = machines_list[i].get_errors()

            await take_direct_method(iot_device_client, client)

            machines_list.clear()
            time.sleep(5)
    except KeyboardInterrupt:
        print("KeyboardInterrupt")

    await client.disconnect()
    iot_device_client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
