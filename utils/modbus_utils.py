import json
import traceback
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Tuple

from psycopg.types.json import Json
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException

from utils.db import get_connection, insert_modbus_log
from utils.logger import CLIENT_IP, get_logger


logger = get_logger()


@dataclass
class EkkDevice:
    id: int
    site_id: int
    serial_number: str
    name: str
    brand: str
    model: str
    status: int
    modbus_host: str
    modbus_port: int
    modbus_unit_id: int
    web_base_url: Optional[str]


@dataclass
class RegisterMapRow:
    id: int
    quantity: str
    modbus_register: int
    number_of_modbus_registers: int
    modbus_data_type: str
    modbus_units: Optional[str]
    web_page_tag: Optional[str]
    scale_multiplier: Decimal


def get_ekk_device(site_id: int) -> Optional[EkkDevice]:
    conn = get_connection()
    if not conn:
        return None

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id,
                site_id,
                serial_number,
                name,
                brand,
                model,
                status,
                host(modbus_host) AS modbus_host,
                modbus_port,
                modbus_unit_id,
                web_base_url
            FROM public.ekk_device
            WHERE site_id = %s
              AND status = 1
            ORDER BY id
            LIMIT 1
            """,
            (site_id,),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return EkkDevice(**row)
    finally:
        conn.close()


def get_ekk_register_map(ekk_device_id: int) -> List[RegisterMapRow]:
    conn = get_connection()
    if not conn:
        return []

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id,
                quantity,
                modbus_register,
                number_of_modbus_registers,
                modbus_data_type,
                modbus_units,
                web_page_tag,
                scale_multiplier
            FROM public.ekk_device_register_map
            WHERE ekk_device_id = %s
              AND is_enabled = true
            ORDER BY modbus_register
            """,
            (ekk_device_id,),
        )
        rows = [RegisterMapRow(**row) for row in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()


def group_registers(registers: Iterable[RegisterMapRow], max_chunk_size: int = 100) -> List[Tuple[int, int]]:
    ordered = sorted(registers, key=lambda item: item.modbus_register)
    chunks: List[Tuple[int, int]] = []
    if not ordered:
        return chunks

    start = ordered[0].modbus_register
    end = start + ordered[0].number_of_modbus_registers - 1

    for item in ordered[1:]:
        item_end = item.modbus_register + item.number_of_modbus_registers - 1
        contiguous = item.modbus_register <= end + 1
        fits = (item_end - start + 1) <= max_chunk_size
        if contiguous and fits:
            end = max(end, item_end)
            continue
        chunks.append((start, end - start + 1))
        start = item.modbus_register
        end = item_end

    chunks.append((start, end - start + 1))
    return chunks


def decode_value(registers: List[int], data_type: str):
    data_type = data_type.upper()
    if data_type == "UINT16":
        return registers[0]
    if data_type == "INT16":
        value = registers[0]
        return value - 0x10000 if value & 0x8000 else value
    if data_type == "UINT32":
        return (registers[0] << 16) | registers[1]
    if data_type == "INT32":
        value = (registers[0] << 16) | registers[1]
        return value - 0x100000000 if value & 0x80000000 else value
    if data_type == "INT64":
        if len(registers) != 4:
            raise ValueError("INT64 requires exactly four Modbus registers")
        value = 0
        for word in registers:
            value = (value << 16) | word
        return value - (1 << 64) if value & (1 << 63) else value
    if data_type == "FLOAT32":
        import struct

        raw = struct.pack(">HH", registers[0], registers[1])
        return struct.unpack(">f", raw)[0]
    if data_type in {"BITMAP", "DATETIME"}:
        return json.dumps(registers)
    raise ValueError(f"Unsupported modbus_data_type: {data_type}")


def normalize_host(host: str) -> str:
    return host.split("/", 1)[0]


def register_address_offset(model: str) -> int:
    # Select by model, not register magnitude: native ION maps also have
    # registers above 40000, which must not be treated as legacy references.
    model_tokens = model.upper().split()
    if "ION7400" in model_tokens or "ION9000" in model_tokens:
        return 1
    if "ION7650" in model_tokens:
        return 40001
    if "".join(model_tokens) in {"IMETER7A", "CETIMETER7A"}:
        # CET iMeter 7A protocol v7.0 starts at register 0000 (Ua).
        return 0
    raise ValueError(f"Unsupported EKK model/addressing: {model!r}")


def payload_json_default(value):
    if isinstance(value, Decimal):
        # Preserve INT64 energy precision in raw_payload as decimal text.
        # reading_value.value_numeric is still stored as PostgreSQL NUMERIC.
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def dump_payload(payload):
    return json.dumps(payload, default=payload_json_default, allow_nan=False)


def read_all_registers(host: str, port: int, device_id: int, register_map: List[RegisterMapRow],
                       model: str = "PowerLogic ION7650") -> Dict[str, Dict[str, object]]:
    address_offset = register_address_offset(model)
    blocks = group_registers(register_map)
    for start, count in blocks:
        address = start - address_offset
        if not 0 <= address <= 65535 or address + count > 65536:
            raise ValueError(f"Invalid register block {start}:{count} for model {model}")
    host = normalize_host(host)
    client = ModbusTcpClient(host=host, port=port, timeout=10)
    if not client.connect():
        raise ConnectionError(f"Unable to connect to Modbus device {host}:{port}")

    try:
        cache: Dict[int, int] = {}
        for start, count in blocks:
            response = client.read_holding_registers(
                address=start - address_offset,
                count=count,
                slave=device_id,
            )
            if response.isError():
                raise ModbusException(f"Read failed for register block {start}:{count}: {response}")
            for offset, value in enumerate(response.registers):
                cache[start + offset] = value

        result: Dict[str, Dict[str, object]] = {}
        for item in register_map:
            raw_registers = [
                cache[item.modbus_register + offset]
                for offset in range(item.number_of_modbus_registers)
            ]
            decoded = decode_value(raw_registers, item.modbus_data_type)

            value_numeric = None
            value_text = None
            value_json = None

            if isinstance(decoded, (int, float)):
                scaled = Decimal(str(decoded)) * item.scale_multiplier
                if not scaled.is_finite():
                    raise ValueError(f"Non-finite measurement for {item.quantity}")
                value_numeric = scaled if item.modbus_data_type.upper() == "INT64" else float(scaled)
            elif item.modbus_data_type.upper() in {"BITMAP", "DATETIME"}:
                value_json = json.loads(decoded)
            else:
                value_text = str(decoded)

            result[item.quantity] = {
                "register_map_id": item.id,
                "quantity": item.quantity,
                "modbus_register": item.modbus_register,
                "raw_registers": raw_registers,
                "value_numeric": value_numeric,
                "value_text": value_text,
                "value_json": value_json,
                "modbus_units": item.modbus_units,
                "web_page_tag": item.web_page_tag,
            }
        return result
    finally:
        client.close()


def insert_ekk_data(conn, payload: Dict[str, Dict[str, object]], site_id: int, ekk_device: EkkDevice):
    cur = conn.cursor()
    reading_sql = """
        INSERT INTO public.ekk_device_reading (
            ekk_device_id,
            site_id,
            logtime,
            poll_started_at,
            poll_finished_at,
            status,
            raw_payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    finished_at = datetime.now()
    started_at = finished_at
    cur.execute(
        reading_sql,
        (
            ekk_device.id,
            site_id,
            finished_at,
            started_at,
            finished_at,
            "SUCCESS",
            Json(payload, dumps=dump_payload),
        ),
    )
    reading_id = cur.fetchone()["id"]

    value_sql = """
        INSERT INTO public.ekk_device_reading_value (
            reading_id,
            register_map_id,
            quantity,
            modbus_register,
            value_numeric,
            value_text,
            value_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    for item in payload.values():
        cur.execute(
            value_sql,
            (
                reading_id,
                item["register_map_id"],
                item["quantity"],
                item["modbus_register"],
                item["value_numeric"],
                item["value_text"],
                Json(item["value_json"]) if item["value_json"] is not None else None,
            ),
        )

    log_sql = """
        INSERT INTO public.ekk_device_poll_log (
            ekk_device_id,
            site_id,
            poll_started_at,
            poll_finished_at,
            level,
            status,
            message
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(
        log_sql,
        (
            ekk_device.id,
            site_id,
            started_at,
            finished_at,
            "INFO",
            "SUCCESS",
            f"EKK poll completed for site_id={site_id}, device_id={ekk_device.id}",
        ),
    )
    conn.commit()
    cur.close()


def ekk_modbus_read_and_store(site_id: int):
    ekk_device = get_ekk_device(site_id)
    if not ekk_device:
        print(f"No active EKK device found for site_id={site_id}")
        return

    register_map = get_ekk_register_map(ekk_device.id)
    if not register_map:
        print(f"No active register map found for ekk_device_id={ekk_device.id}")
        return

    try:
        print(f"Processing EKK device: {ekk_device.serial_number}")
        result = read_all_registers(
            host=ekk_device.modbus_host,
            port=ekk_device.modbus_port,
            device_id=ekk_device.modbus_unit_id,
            register_map=register_map,
            model=ekk_device.model,
        )
        print("Modbus EKK read result sample:", list(result.items())[:3])

        conn = get_connection()
        if conn:
            insert_ekk_data(conn, result, site_id, ekk_device)
            conn.close()
    except Exception as exc:
        logger.error(f"Error with EKK modbus connection or reading: {exc}", extra={"clientip": CLIENT_IP})
        print(f"Error with EKK modbus connection or reading: {exc}")
        conn_log = get_connection()
        if conn_log:
            insert_modbus_log(
                conn_log,
                f"Error with EKK modbus connection or reading: {exc}",
                CLIENT_IP,
                site_id=site_id,
                ekk_device_id=ekk_device.id,
                error_detail=str(exc),
                traceback_text=traceback.format_exc(),
            )
            conn_log.close()
