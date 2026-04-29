import sys
from utils.db import get_connection
from utils.modbus_utils import inverter_modbus_read_and_store
from utils.modbus_utils import emi_modbus_read_and_store
from utils.modbus_utils import meter_modbus_read_and_store
from utils.modbus_utils import power_meter_modbus_read_and_store

if __name__ == '__main__':
    # Get power_plant_id from command line arguments
    if len(sys.argv) < 2 or not sys.argv[1].strip():
        print("Warning: power_plant_id parameter is required. Please provide it as the first argument.")
        sys.exit(1)

    power_plant_id = sys.argv[1].strip()
    print(f'Using power_plant_id: {power_plant_id}')

    conn = get_connection()
    if not conn:
        print("Database connection failed.")
        sys.exit(1)

    try:
        cur = conn.cursor()
        sql = """
            SELECT 
                power_plant_id, 
                device_type_id, 
                serial_number, 
                host, 
                port, 
                device_id, 
                inverter_id, 
                slave_id, 
                registers, 
                sensor_id, 
                meter_id, 
                power_meter_id
            FROM modbus_device_table 
            WHERE power_plant_id = %s
        """
        cur.execute(sql, (power_plant_id,))
        devices = cur.fetchall()
        cur.close()

        if not devices:
            print(f"No devices found for power_plant_id={power_plant_id}")
            conn.close()
            sys.exit(0)

        import asyncio
        import time
        import datetime


        async def process_devices():
            for device in devices:
                (
                    pp_id,
                    device_type_id,
                    serial_number,
                    host,
                    port,
                    device_id,
                    inverter_id,
                    slave_id, 
                    registers, 
                    sensor_id, 
                    meter_id, 
                    power_meter_id
                ) = device

                print(f"Processing device: {serial_number}")

                # Call the inverter_modbus_read_and_store function for each device
                if device_type_id == 1:
                    await inverter_modbus_read_and_store(host, port, device_id, pp_id, inverter_id, serial_number)
                elif device_type_id == 2:
                    await emi_modbus_read_and_store(host, port, device_id, pp_id, sensor_id, serial_number)
                elif device_type_id == 3:
                    await meter_modbus_read_and_store(host, port, device_id, pp_id, meter_id, serial_number)
                elif device_type_id == 4:
                    await power_meter_modbus_read_and_store(host, port, device_id, pp_id, power_meter_id, serial_number)
                else:
                    print(f"Unknown device_type_id: {device_type_id} for device {serial_number}")
                #await inverter_modbus_read_and_store(host, port, device_id, pp_id, inverter_id)

        asyncio.run(process_devices())
        print("Waiting for 7 minutes before exiting...")
        time.sleep(7 * 60)
        print("Finished processing all devices.")
        print("Current time:", datetime.datetime.now())

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()
