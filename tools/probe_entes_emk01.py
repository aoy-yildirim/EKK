"""Read-only EMK-01 address check. Optional site lookup; no DB/device writes."""
import argparse
import json
from pathlib import Path


def probe_addressing(client, unit_id, offset):
    import math
    import struct
    from decimal import Decimal

    result = {"address_offset": offset, "pdu_voltage_address": 40001 - offset}
    fields = (("voltage_ln_v", 40001, 6, "FLOAT32"),
              ("frequency_hz", 40081, 2, "FLOAT32"),
              ("consumed_energy_int64_kwh", 55000, 4, "INT64"),
              ("consumed_energy_float_kwh", 55024, 2, "FLOAT32"),
              ("supplied_energy_int64_kwh", 55012, 4, "INT64"),
              ("supplied_energy_float_kwh", 55030, 2, "FLOAT32"))
    for label, register, count, dtype in fields:
        try:
            response = client.read_holding_registers(
                address=register - offset, count=count, slave=unit_id)
            if response.isError():
                code = getattr(response, "exception_code", None)
                result[label] = {"error": str(response), "pdu_address": register - offset,
                                 "exception_code": code if isinstance(code, int) else None}
                continue
            if len(response.registers) != count:
                result[label] = {"error": "Short Modbus response"}
                continue
            raw = struct.pack(">" + "H" * count, *response.registers)
            if dtype == "INT64":
                result[label] = str(Decimal(int.from_bytes(raw, "big", signed=True)) * Decimal("0.1"))
                continue
            values = list(struct.unpack(">" + "f" * (count // 2), raw))
            result[label] = [v if math.isfinite(v) else None for v in values] if count > 2 else (
                values[0] if math.isfinite(values[0]) else None)
        except Exception as exc:
            result[label] = {"error": str(exc)}
    # No automatic selection: zero/garbage values can also be legal responses.
    return result


def load_site_endpoint(site_id, connection_factory=None):
    if connection_factory is None:
        import sys

        # Direct invocation as `python tools/...py` places tools/ on sys.path.
        root = str(Path(__file__).resolve().parents[1])
        if root not in sys.path:
            sys.path.insert(0, root)
        from utils.db import get_connection

        connection_factory = get_connection
    conn = connection_factory()
    if conn is None:
        raise ValueError("Unable to read device configuration from PostgreSQL")
    try:
        cur = conn.cursor()
        try:
            # Include passive devices so verification can precede activation.
            cur.execute(
                """SELECT id, model, host(modbus_host) AS host,
                          modbus_port AS port, modbus_unit_id AS unit_id
                   FROM public.ekk_device WHERE site_id = %s ORDER BY id LIMIT 1""",
                (site_id,),
            )
            row = cur.fetchone()
        finally:
            cur.close()
    finally:
        conn.close()
    if not row:
        raise ValueError(f"No EKK device found for site_id={site_id}")
    if "".join(row["model"].upper().split()) not in {
        "EMK-01", "EMK01", "ENTESEMK-01", "ENTESEMK01",
    }:
        raise ValueError(f"Site {site_id} model {row['model']!r} is not EMK-01")
    return row


def main(argv=None):
    from pymodbus.client import ModbusTcpClient

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host", nargs="?")
    parser.add_argument("--site-id", type=int, help="Read endpoint from ekk_device (SELECT only)")
    parser.add_argument("--port", type=int)
    parser.add_argument("--unit-id", type=int)
    parser.add_argument("--timeout", type=float, default=5)
    args = parser.parse_args(argv)
    if args.site_id is not None:
        if args.site_id <= 0 or args.host is not None or args.port is not None or args.unit_id is not None:
            parser.error("Use --site-id alone or host/--port/--unit-id, not both")
    elif args.host is None or args.unit_id is None:
        parser.error("Provide --site-id, or host and --unit-id")
    # Load the same local override as the poller; do not print credentials.
    from dotenv import load_dotenv
    import os

    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=True)
    if args.site_id is not None:
        try:
            endpoint = load_site_endpoint(args.site_id)
        except ValueError as exc:
            parser.error(str(exc))
        args.host, args.port, args.unit_id = endpoint["host"], endpoint["port"], endpoint["unit_id"]
        print(f"Using site_id={args.site_id}, device_id={endpoint['id']}, "
              f"endpoint={args.host}:{args.port}, unit_id={args.unit_id}")
    elif args.port is None:
        args.port = 502
    if not 1 <= args.port <= 65535 or not 0 <= args.unit_id <= 255 or args.timeout <= 0:
        parser.error("Invalid port, unit ID or timeout")
    try:
        configured = int(os.getenv("EKK_ENTES_EMK01_ADDRESS_OFFSET", "0"))
    except ValueError:
        parser.error("Invalid EKK_ENTES_EMK01_ADDRESS_OFFSET")
    offsets = list(dict.fromkeys([configured, 0, 40001, 1, 40000]))
    print("FC03 read-only probe; compare voltage, frequency AND energy to the web screen.")
    print("No database records or settings will be changed. No automatic address selection.")
    for offset in offsets:
        if offset not in {0, 1, 40000, 40001}:
            parser.error("Invalid EKK_ENTES_EMK01_ADDRESS_OFFSET")
        client = ModbusTcpClient(host=args.host, port=args.port, timeout=args.timeout)
        try:
            if not client.connect():
                print("Unable to connect to Modbus TCP endpoint")
                return 1
            print(json.dumps(probe_addressing(client, args.unit_id, offset), allow_nan=False))
        finally:
            client.close()
    print("After screen verification, set EKK_ENTES_EMK01_ADDRESS_OFFSET to the matching offset.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
