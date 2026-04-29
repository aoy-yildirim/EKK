from huawei_solar_tools.devices.inverter import Inverter
from huawei_solar_tools.devices.emi import EMI
from huawei_solar_tools.devices.meter import Meter
from huawei_solar_tools.devices.power_meter import PowerMeter


from utils.db import get_connection, insert_modbus_log
from utils.logger import get_logger, CLIENT_IP

#
# INVERTER MODBUS FUNCTIONS
#
async def inverter_modbus_read_and_store(host, port, device_id, pp_id, inverter_id,serial_number):
    logger = get_logger()
    try:
        inverter = Inverter(host=host, port=port, device_id=device_id)
        try:
            await inverter.connect()
            logger.info(f'Successfully connected to inverter at {host}:{port} (device_id={device_id})', extra={'clientip': CLIENT_IP})
            print(f'Successfully connected to inverter at {host}:{port} (device_id={device_id})')
        except Exception as e:
            logger.error(f'Error connecting to inverter: {e}', extra={'clientip': CLIENT_IP})
            print(f'Error connecting to inverter: {e}')
            conn_log = get_connection()
            if conn_log:
                insert_modbus_log(conn_log, f'Error connecting to inverter: {e}', CLIENT_IP)
                conn_log.close()
            return
        try:
            result = await inverter.read_all_registers()
            print('Modbus read result:', result[0:3])  # Print first 3 items for brevity
        except Exception as e:
            logger.error(f'Error reading all registers: {e}', extra={'clientip': CLIENT_IP})
            print(f'Error reading all registers: {e}')
            conn_log = get_connection()
            if conn_log:
                insert_modbus_log(conn_log, f'Error reading all registers: {e}', CLIENT_IP)
                conn_log.close()
            return
        conn = get_connection()
        if conn:
            insert_inverter_data(conn, result, pp_id, inverter_id, serial_number)
            conn.close()
    except Exception as e:
        logger.error(f'Error with modbus connection or reading: {e}', extra={'clientip': CLIENT_IP})
        print(f'Error with modbus connection or reading: {e}')
        conn_log = get_connection()
        if conn_log:
            insert_modbus_log(conn_log, f'Error with modbus connection or reading: {e}', CLIENT_IP)
            conn_log.close()

#---------------------------
def insert_inverter_data(conn, data,pp_id, inverter_id, serial_number):
    try:
        cur = conn.cursor()
        # Convert list of tuples to dict (ignore the third element in each tuple)
        if isinstance(data, list):
            data_dict = {k: v for k, v, *_ in data}
        else:
            data_dict = data

        # Add power_plant_id, inverter_id and serial_number to the data_dict for insertion
        data_dict['power_plant_id'] = pp_id
        data_dict['inverter_id'] = inverter_id
        data_dict['serial_number'] = serial_number

        columns = ', '.join(data_dict.keys())
        values = ', '.join(['%s'] * len(data_dict))
        sql = f"INSERT INTO modbus_inverter_data ({columns}) VALUES ({values})"
        #print(f'Executing SQL: {sql} with values {list(data_dict.values())}')
        cur.execute(sql, list(data_dict.values()))
        conn.commit()
        cur.close()
        print('Inserted inverter data into modbus_inverter_data.', pp_id, inverter_id, serial_number)
    except Exception as e:
        from utils.logger import get_logger, CLIENT_IP
        logger = get_logger()
        logger.error(f'Error inserting inverter data: {e}', extra={'clientip': CLIENT_IP})

#---------------------------
#
# EMI MODBUS FUNCTIONS - sensor 
#

async def emi_modbus_read_and_store(host, port, device_id, pp_id, sensor_id, serial_number):
    print(f'EMI modbus read and store for {host}:{port} (device_id={device_id})')

    logger = get_logger()
    try:
        emi = EMI(host=host, port=port, slave_id=device_id)
        try:
            await emi.connect()
            logger.info(f'Successfully connected to sensor at {host}:{port} (device_id={device_id})', extra={'clientip': CLIENT_IP})
            print(f'Successfully connected to sensor at {host}:{port} (device_id={device_id})')
        except Exception as e:
            logger.error(f'Error connecting to sensor: {e}', extra={'clientip': CLIENT_IP})
            print(f'Error connecting to sesnor: {e}')
            conn_log = get_connection()
            if conn_log:
                insert_modbus_log(conn_log, f'Error connecting to sensor: {e}', CLIENT_IP)
                conn_log.close()
            return
        try:
            result = await emi.read_all_registers()
            print('Modbus sensor read result:', result[0:3])  # Print first 3 items for brevity
        except Exception as e:
            logger.error(f'Error reading all sensor registers: {e}', extra={'clientip': CLIENT_IP})
            print(f'Error reading all sensor registers: {e}')
            conn_log = get_connection()
            if conn_log:
                insert_modbus_log(conn_log, f'Error reading all sensor registers: {e}', CLIENT_IP)
                conn_log.close()
            return
        conn = get_connection()
        if conn:
            insert_emi_data(conn, result, pp_id, sensor_id, serial_number)
            conn.close()
    except Exception as e:
        logger.error(f'Error with modbus connection or reading sensor: {e}', extra={'clientip': CLIENT_IP})
        print(f'Error with modbus connection or reading sensor: {e}')
        conn_log = get_connection()
        if conn_log:
            insert_modbus_log(conn_log, f'Error with modbus connection or reading sensor: {e}', CLIENT_IP)
            conn_log.close()

#---------------------------
def insert_emi_data(conn, data, pp_id, sensor_id, serial_number):
    try:
        cur = conn.cursor()
        # Convert list of tuples to dict (ignore the third element in each tuple)
        if isinstance(data, list):
            data_dict = {k: v for k, v, *_ in data}
        else:
            data_dict = data

        # Add power_plant_id, sensor_id and serial_number to the data_dict for insertion
        data_dict['power_plant_id'] = pp_id
        data_dict['sensor_id'] = sensor_id
        data_dict['serial_number'] = serial_number

        columns = ', '.join(data_dict.keys())
        values = ', '.join(['%s'] * len(data_dict))
        sql = f"INSERT INTO modbus_sensor_data ({columns}) VALUES ({values})"
        #print(f'Executing SQL: {sql} with values {list(data_dict.values())}')
        cur.execute(sql, list(data_dict.values()))
        conn.commit()
        cur.close()
        print('Inserted sensor data into modbus_sensor_data.', pp_id, sensor_id, serial_number)
    except Exception as e:
        from utils.logger import get_logger, CLIENT_IP
        logger = get_logger()
        logger.error(f'Error inserting sensor data: {e}', extra={'clientip': CLIENT_IP})



#---------------------------
#
# POWER_METER MODBUS FUNCTIONS
#

async def power_meter_modbus_read_and_store(host, port, device_id, pp_id, power_meter_id, serial_number):
    logger = get_logger()
    try:
        # If PowerMeter expects device_id instead of slave_id, change the keyword accordingly.
        power_meter = PowerMeter(host=host, port=port, slave_id=device_id)
        try:
            await power_meter.connect()
            logger.info(f'Successfully connected to power meter at {host}:{port} (device_id={device_id})', extra={'clientip': CLIENT_IP})
            print(f'Successfully connected to power meter at {host}:{port} (device_id={device_id})')
        except Exception as e:
            logger.error(f'Error connecting to power meter: {e}', extra={'clientip': CLIENT_IP})
            print(f'Error connecting to power meter: {e}')
            conn_log = get_connection()
            if conn_log:
                insert_modbus_log(conn_log, f'Error connecting to power meter: {e}', CLIENT_IP)
                conn_log.close()
            return

        try:
            result = await power_meter.read_all_registers()
            print('Modbus power meter read result:', result[0:3])  # Print first 3 items for brevity
        except Exception as e:
            logger.error(f'Error reading all power meter registers: {e}', extra={'clientip': CLIENT_IP})
            print(f'Error reading all power meter registers: {e}')
            conn_log = get_connection()
            if conn_log:
                insert_modbus_log(conn_log, f'Error reading all power meter registers: {e}', CLIENT_IP)
                conn_log.close()
            return

        conn = get_connection()
        if conn:
            insert_power_meter_data(conn, result, pp_id, power_meter_id, serial_number)
            conn.close()
    except Exception as e:
        logger.error(f'Error with modbus connection or reading power meter: {e}', extra={'clientip': CLIENT_IP})
        print(f'Error with modbus connection or reading power meter: {e}')
        conn_log = get_connection()
        if conn_log:
            insert_modbus_log(conn_log, f'Error with modbus connection or reading power meter: {e}', CLIENT_IP)
            conn_log.close()


def insert_power_meter_data(conn, data, pp_id, meter_id, serial_number):
    try:
        cur = conn.cursor()
        # Convert list of tuples to dict (ignore the third element in each tuple)
        if isinstance(data, list):
            data_dict = {k: v for k, v, *_ in data}
        else:
            data_dict = data

        # Add power_plant_id, meter_id and serial_number to the data_dict for insertion
        data_dict['power_plant_id'] = pp_id
        data_dict['meter_id'] = meter_id
        data_dict['serial_number'] = serial_number

        columns = ', '.join(data_dict.keys())
        values = ', '.join(['%s'] * len(data_dict))
        sql = f"INSERT INTO modbus_meter_data ({columns}) VALUES ({values})"
        #print(f'Executing SQL: {sql} with values {list(data_dict.values())}')
        cur.execute(sql, list(data_dict.values()))
        conn.commit()
        cur.close()
        print('Inserted power meter data into modbus_meter_data.', pp_id, meter_id, serial_number)
    except Exception as e:
        logger = get_logger()
        logger.error(f'Error inserting power meter data: {e}', extra={'clientip': CLIENT_IP})


#---------------------------
#
# METER MODBUS FUNCTIONS
#

async def meter_modbus_read_and_store(host, port, device_id, pp_id, meter_id, serial_number):
    print(f'Meter modbus read and store not implemented for {host}:{port} (device_id={device_id})')
