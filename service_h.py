import asyncio
from azure.iot.hub import IoTHubRegistryManager
from azure.iot.hub.models import Twin, TwinProperties, CloudToDeviceMethod
from azure.storage.blob import BlobServiceClient
import json
import time
import sys


async def receive_twin_reported(iot_manager_client, device):
    twin = iot_manager_client.get_twin(device)
    rep = twin.properties.reported
    print("Twin reported:")
    print(rep)
    return rep


async def twin_desired(iot_manager_client, device, reported):
    desired_twin = {}

    del reported["$metadata"]
    del reported["$version"]

    for key, value in reported.items():
        desired_twin[key] = {"ProductionRate": value["ProductionRate"]}

    twin = iot_manager_client.get_twin(device)
    twin_patch = Twin(properties=TwinProperties(desired=desired_twin))
    twin = iot_manager_client.update_twin(device, twin_patch, twin.etag)


async def clear_desired_twin(iot_manager, device_id):
    twin = iot_manager.get_twin(device_id)
    des = twin.properties.desired
    del des["$metadata"]
    del des["$version"]
    for key, value in des.items():
        des[key] = None

    twin_patch = Twin(properties=TwinProperties(desired=des))
    twin = iot_manager.update_twin(device_id, twin_patch, twin.etag)


async def clear_blob_storage(connection_str):
    blob_dev_err_name = 'device-err'
    blob_temperature_name = 'kpi-production'
    blob_kpi_name = 'temperature-info'
    blob_service = BlobServiceClient.from_connection_string(connection_str)
    try:
        blob_service.delete_container(blob_dev_err_name)
    except:
        print("No blob named device-err")

    try:
        blob_service.delete_container(blob_temperature_name)
    except:
        print("No blob named temperature-info")

    try:
        blob_service.delete_container(blob_kpi_name)
    except:
        print("No blob named kpi-production")

    print("Blobs are cleared")


async def c2d_emergency_stop(iot_manager, device):
    cd = CloudToDeviceMethod(method_name="emergency_stop", payload={"DeviceName": device})
    iot_manager.invoke_device_method("test_device", cd)


async def c2d_error_status_reset(iot_manager, device):
    cd = CloudToDeviceMethod(method_name="reset_err_status", payload={"DeviceName": device})
    iot_manager.invoke_device_method("test_device", cd)


async def read_blobs(iot_manager, device_id, connection_str, date_err, date_kpi):
    device_error_blob = 'device-err'
    temperature_blob = 'temperature-info'
    kpi_blob = 'kpi-production'
    blob_service_client = BlobServiceClient.from_connection_string(connection_str)

    ret_date_err = date_err
    ret_date_kpi = date_kpi

    # Work with device errors
    try:
        dev_err_container_client = blob_service_client.get_container_client(device_error_blob)

        device_errors_json_list = []

        for blob in dev_err_container_client.list_blobs():
            data = dev_err_container_client.download_blob(blob).readall()
            data = data.decode("utf-8")
            device_errors_json_list.append(data.split('\r\n'))

        dic_device_error_list = []

        for i in range(len(device_errors_json_list)):
            for j in range(len(device_errors_json_list[i])):
                dic_device_error_list.append(json.loads(device_errors_json_list[i][j]))

        print(f"ERR JSON: " + str(device_errors_json_list))
        print(f"DIC ERR: " + str(dic_device_error_list))

        for i in range(len(dic_device_error_list)):
            if dic_device_error_list[i]["windowEndTime"] > date_err:
                ret_date_err = dic_device_error_list[i]["windowEndTime"]
                device_name = dic_device_error_list[i]["DeviceName"]
                await c2d_emergency_stop(iot_manager, device_name)
    except:
        pass

    try:
        temperature_container_client = blob_service_client.get_container_client(temperature_blob)

        temperature_json_list = []

        for blob in temperature_container_client.list_blobs():
            data = temperature_container_client.download_blob(blob).readall()
            data = data.decode("utf-8")
            temperature_json_list.append(data.split('\r\n'))

        dic_temperature_list = []

        for i in range(len(temperature_json_list)):
            for j in range(len(temperature_json_list[i])):
                dic_temperature_list.append(json.loads(temperature_json_list[i][j]))

        print(f"TEMP JSON: " + str(temperature_json_list))
        print(f"DIC TEMP: " + str(dic_temperature_list))
    except:
        pass

    try:
        kpi_container_client = blob_service_client.get_container_client(kpi_blob)

        kpi_json_list = []

        for blob in kpi_container_client.list_blobs():
            data = kpi_container_client.download_blob(blob).readall()
            data = data.decode("utf-8")
            kpi_json_list.append(data.split('\r\n'))

        dic_kpi_list = []

        for i in range(len(kpi_json_list)):
            for j in range(len(kpi_json_list[i])):
                dic_kpi_list.append(json.loads(kpi_json_list[i][j]))

        print(f"KPI JSON: " + str(kpi_json_list))
        print(f"DIC KPI: " + str(dic_kpi_list))

        for i in range(len(dic_kpi_list)):
            if dic_kpi_list[i]["windEndTime"] > date_kpi:

                ret_date_kpi = dic_kpi_list[i]["windEndTime"]

                if dic_kpi_list[i]["KPI"] < 90:
                    print("---------kpi------------")
                    twin = iot_manager.get_twin(device_id)
                    desired = twin.properties.desired

                    dev_name = "Device" + str(dic_kpi_list[i]["DeviceName"])[-1]
                    prod_rate = desired[dev_name]["ProductionRate"] - 10

                    update_twin = {dev_name: {"ProductionRate": prod_rate}}

                    twin_patch = Twin(properties=TwinProperties(desired=update_twin))
                    twin = iot_manager.update_twin(device_id, twin_patch, twin.etag)
    except:
        pass

    return ret_date_err, ret_date_kpi


async def main():
    # Connection to IoTHub
    CONNECTION_STRING_MANAGER = "HostName=iot-hub-taras-ul.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=HT848nTL1b2y/hZMpxrOMg8wbw763NeBoAIoTLoXfTw="
    DEVICE_ID = "testDevice"

    iot_registry_manager = IoTHubRegistryManager(CONNECTION_STRING_MANAGER)

    # Clear the desired twin
    await clear_desired_twin(iot_registry_manager, DEVICE_ID)

    STORAGE_CONNECTION_STRING = 'DefaultEndpointsProtocol=https;AccountName=mystorage123443211234;AccountKey=DQ4+xyv3tmuUUB+v8ANmK+jSkanNwZXj9bSxCTdR9HMvO6tL7kW7XkUrUFgR5mG3idZLbAGJ8Va3+AStJTgjhQ==;EndpointSuffix=core.windows.net'

    # Clear the blob storage
    await clear_blob_storage(STORAGE_CONNECTION_STRING)

    old_date_err = ""
    old_date_kpi = ""

    try:
        while True:
            # receive twin reported
            twin_reported = await receive_twin_reported(iot_registry_manager, DEVICE_ID)

            # sending the twin desired
            await twin_desired(iot_registry_manager, DEVICE_ID, twin_reported)

            # reading blob storage and actualization date of new blobs
            new_date_err, new_date_kpi = await read_blobs(iot_registry_manager, DEVICE_ID, STORAGE_CONNECTION_STRING, old_date_err, old_date_kpi)

            # actualization
            old_date_err = new_date_err
            old_date_kpi = new_date_kpi

            time.sleep(1)
    except Exception as e:
        print("Progam is stopeed")
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
