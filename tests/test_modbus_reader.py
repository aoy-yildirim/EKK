"""Offline reader checks; no database, network or third-party imports required."""
import ast
import json
import unittest
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from unittest.mock import Mock


def load_reader_functions():
    # Execute the actual reader functions without importing DB/logging clients.
    path = Path(__file__).resolve().parents[1] / "utils" / "modbus_utils.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = {
        "RegisterMapRow", "group_registers", "decode_value", "normalize_host",
        "register_address_offset", "payload_json_default", "dump_payload",
        "read_all_registers", "insert_ekk_data", "ekk_modbus_read_and_store",
    }
    tree.body = [node for node in tree.body
                 if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
    namespace = dict(globals(), EkkDevice=object)
    exec(compile(tree, str(path), "exec"), namespace)
    return namespace


class ReaderTests(unittest.TestCase):
    def setUp(self):
        self.reader = load_reader_functions()
        self.client = Mock()
        self.client.connect.return_value = True
        self.reader["ModbusTcpClient"] = Mock(return_value=self.client)
        self.reader["ModbusException"] = RuntimeError

    def row(self, register, dtype="FLOAT32", words=2, scale="1"):
        return self.reader["RegisterMapRow"](
            1, "measurement", register, words, dtype, "kWh", None, Decimal(scale))

    def respond(self, registers):
        response = Mock(registers=registers)
        response.isError.return_value = False
        self.client.read_holding_registers.return_value = response

    def test_new_models_use_native_numbering(self):
        for model in ("PowerLogic ION7400", "PowerLogic ION9000"):
            self.respond([0x4248, 0])  # FLOAT32 50 Hz/A
            result = self.reader["read_all_registers"](
                "example.invalid", 502, 1, [self.row(3000)], model=model)
            self.client.read_holding_registers.assert_called_with(address=2999, count=2, slave=1)
            self.assertEqual(result["measurement"]["value_numeric"], 50.0)
            self.assertEqual(result["measurement"]["modbus_register"], 3000)
            self.client.close.assert_called()

    def test_legacy_address_and_scaling_unchanged(self):
        self.respond([1234])
        result = self.reader["read_all_registers"](
            "example.invalid", 502, 1, [self.row(40150, "UINT16", 1, "0.1")],
            model="PowerLogic ION7650")
        self.client.read_holding_registers.assert_called_with(address=149, count=1, slave=1)
        self.assertEqual(result["measurement"]["value_numeric"], 123.4)

    def test_native_address_above_40000_is_not_legacy(self):
        self.respond([0x4248, 0])
        self.reader["read_all_registers"](
            "example.invalid", 502, 1, [self.row(40002)], model="ION9000")
        self.client.read_holding_registers.assert_called_with(address=40001, count=2, slave=1)

    def test_imeter_zero_based_address_and_unit_id(self):
        for model in ("iMeter 7A", "iMeter7A", "CET iMeter 7A"):
            self.respond([0x4248, 0])
            result = self.reader["read_all_registers"](
                "example.invalid", 502, 100, [self.row(0)], model=model)
            self.client.read_holding_registers.assert_called_with(address=0, count=2, slave=100)
            self.assertEqual(result["measurement"]["value_numeric"], 50.0)

    def test_imeter_energy_address_and_wh_conversion(self):
        self.respond([0, 5, 0x301E, 0x8C00])
        raw = (5 << 32) + (0x301E << 16) + 0x8C00
        result = self.reader["read_all_registers"](
            "example.invalid", 502, 100, [self.row(504, "INT64", 4, "0.001")],
            model="iMeter 7A")
        self.client.read_holding_registers.assert_called_with(address=504, count=4, slave=100)
        self.assertEqual(result["measurement"]["value_numeric"], Decimal(raw) / 1000)

    def test_imeter_negative_active_power_keeps_sign(self):
        import struct
        self.respond(list(struct.unpack(">HH", struct.pack(">f", -1496000.0))))
        result = self.reader["read_all_registers"](
            "example.invalid", 502, 100, [self.row(30, "FLOAT32", 2, "0.001")],
            model="iMeter 7A")
        self.assertEqual(result["measurement"]["value_numeric"], -1496.0)

    def test_imeter_other_model_is_not_assumed_compatible(self):
        with self.assertRaises(ValueError):
            self.reader["register_address_offset"]("iMeter 8")

    def test_janitza_native_address_and_model_names(self):
        import struct
        for model in ("UMG512", "UMG 512", "Janitza UMG512", "janitza umg 512"):
            self.respond(list(struct.unpack(">HH", struct.pack(">f", 18482.125))))
            result = self.reader["read_all_registers"](
                "example.invalid", 502, 1, [self.row(19000)], model=model)
            self.client.read_holding_registers.assert_called_with(address=19000, count=2, slave=1)
            self.assertEqual(result["measurement"]["value_numeric"], 18482.125)

    def test_janitza_power_and_float_energy_conversion(self):
        import struct
        for reg, raw, expected in ((19026, -2189850.0, -2189.85),
                                   (6508, 63015732.0, 63015.732),
                                   (6484, -35169648640.0, -35169648.64)):
            self.respond(list(struct.unpack(">HH", struct.pack(">f", raw))))
            result = self.reader["read_all_registers"](
                "example.invalid", 502, 1, [self.row(reg, "FLOAT32", 2, "0.001")],
                model="UMG512")
            self.client.read_holding_registers.assert_called_with(address=reg, count=2, slave=1)
            self.assertAlmostEqual(result["measurement"]["value_numeric"], expected, places=6)

    def test_other_janitza_models_are_not_assumed_compatible(self):
        for model in ("UMG511", "UMG512-PRO", "UMG 512 PRO", "UMG5120"):
            with self.assertRaises(ValueError):
                self.reader["register_address_offset"](model)

    def test_entes_live_verified_native_address_default_and_aliases(self):
        import os
        import struct
        from unittest.mock import patch
        with patch.dict(os.environ, {}, clear=True):
            for model in ("EMK-01", "EMK01", "ENTES EMK-01", "Entes EMK 01"):
                self.respond(list(struct.unpack(">HH", struct.pack(">f", 230.6))))
                result = self.reader["read_all_registers"](
                    "example.invalid", 502, 7, [self.row(40001)], model=model)
                self.client.read_holding_registers.assert_called_with(address=40001, count=2, slave=7)
                self.assertAlmostEqual(result["measurement"]["value_numeric"], 230.6, places=4)

    def test_entes_energy_uses_tenths_of_kwh(self):
        import os
        from unittest.mock import patch
        # Live probe: tenths of kWh; supplied counter is negative on device.
        with patch.dict(os.environ, {}, clear=True):
            for register, raw, expected in ((55000, 15854546, Decimal("1585454.6")),
                                            (55012, -1388, Decimal("-138.8"))):
                self.respond([(raw >> shift) & 0xFFFF for shift in (48, 32, 16, 0)])
                result = self.reader["read_all_registers"](
                    "example.invalid", 502, 7, [self.row(register, "INT64", 4, "0.1")],
                    model="ENTES EMK-01")
                self.client.read_holding_registers.assert_called_with(
                    address=register, count=4, slave=7)
                self.assertEqual(result["measurement"]["value_numeric"], expected)
                self.assertEqual(json.loads(self.reader["dump_payload"](result))[
                    "measurement"]["value_numeric"], str(expected))

    def test_entes_override_does_not_change_other_models(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, {"EKK_ENTES_EMK01_ADDRESS_OFFSET": "40001"}):
            for model, offset in (("EMK-01", 40001), ("ION7650", 40001),
                                  ("ION7400", 1), ("ION9000", 1),
                                  ("iMeter 7A", 0), ("UMG512", 0)):
                self.assertEqual(self.reader["register_address_offset"](model), offset)
            self.respond([0x4248, 0])
            self.reader["read_all_registers"]("example.invalid", 502, 7,
                                             [self.row(40001)], model="EMK-01")
            self.client.read_holding_registers.assert_called_with(address=0, count=2, slave=7)

    def test_entes_invalid_override_and_unrelated_model_are_rejected(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, {"EKK_ENTES_EMK01_ADDRESS_OFFSET": "-1"}):
            with self.assertRaises(ValueError):
                self.reader["register_address_offset"]("EMK-01")
        with self.assertRaises(ValueError):
            self.reader["register_address_offset"]("EMK-10P")

    def test_entes_probe_is_read_only_and_compares_energy_formats(self):
        import importlib.util
        import struct
        path = Path(__file__).resolve().parents[1] / "tools" / "probe_entes_emk01.py"
        spec = importlib.util.spec_from_file_location("emk_probe_test", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        values = {(40001, 6): (234.2357635498047, 232.04287719726562, 231.5345001220703),
                  (40081, 2): (49.99498748779297,),
                  (55024, 2): (1585454.625,), (55030, 2): (-138.79998779296875,)}
        def respond(*, address, count, slave):
            self.assertEqual(slave, 7)
            if count == 4:
                raw = 15854546 if address == 55000 else -1388
                words = [(raw >> shift) & 0xFFFF for shift in (48, 32, 16, 0)]
            else:
                floats = values[(address, count)]
                words = list(struct.unpack(">" + "H" * count,
                                           struct.pack(">" + "f" * len(floats), *floats)))
            response = Mock(registers=words)
            response.isError.return_value = False
            return response
        self.client.read_holding_registers.side_effect = respond
        result = module.probe_addressing(self.client, 7, 0)
        self.assertEqual(result["consumed_energy_int64_kwh"], "1585454.6")
        self.assertEqual(result["consumed_energy_float_kwh"], 1585454.625)
        self.assertEqual(result["supplied_energy_int64_kwh"], "-138.8")
        self.assertAlmostEqual(result["supplied_energy_float_kwh"], -138.8, places=4)
        self.assertAlmostEqual(result["frequency_hz"], 49.99498748779297, places=4)
        self.assertEqual(len(self.client.method_calls), 6)
        self.assertTrue(all(call[0] == "read_holding_registers" for call in self.client.method_calls))

    def test_int64_boundaries_and_word_order(self):
        decode = self.reader["decode_value"]
        self.assertEqual(decode([0, 0, 1, 2], "INT64"), 65538)
        self.assertEqual(decode([0xFFFF] * 4, "INT64"), -1)
        self.assertEqual(decode([0x8000, 0, 0, 0], "INT64"), -(1 << 63))
        self.assertEqual(decode([0x7FFF, 0xFFFF, 0xFFFF, 0xFFFF], "INT64"), (1 << 63) - 1)
        with self.assertRaises(ValueError):
            decode([0, 0], "INT64")

    def test_energy_precision_and_payload_serialization(self):
        self.respond([0x7FFF, 0xFFFF, 0xFFFF, 0xFFFF])
        payload = self.reader["read_all_registers"](
            "example.invalid", 502, 1, [self.row(3204, "INT64", 4, "0.001")],
            model="ION7400")
        expected = Decimal("9223372036854775.807")
        self.assertEqual(payload["measurement"]["value_numeric"], expected)
        encoded = self.reader["dump_payload"](payload)
        self.assertEqual(json.loads(encoded)["measurement"]["value_numeric"], str(expected))

    def test_invalid_model_and_address_fail_before_connect(self):
        for model, register in (("Other meter", 3000), ("ION7650", 3000), ("ION7400", 0)):
            with self.assertRaises(ValueError):
                self.reader["read_all_registers"](
                    "example.invalid", 502, 1, [self.row(register)], model=model)
        self.client.connect.assert_not_called()

    def test_modbus_error_closes_connection(self):
        response = Mock()
        response.isError.return_value = True
        self.client.read_holding_registers.return_value = response
        with self.assertRaises(RuntimeError):
            self.reader["read_all_registers"](
                "example.invalid", 502, 1, [self.row(3000)], model="ION7400")
        self.client.close.assert_called_once()

    def test_poll_passes_device_model_to_reader(self):
        device = Mock(id=7, model="PowerLogic ION7400", modbus_host="example.invalid",
                      modbus_port=502, modbus_unit_id=1)
        self.reader["get_ekk_device"] = Mock(return_value=device)
        rows = [self.row(3000)]
        self.reader["get_ekk_register_map"] = Mock(return_value=rows)
        read = Mock(return_value={})
        self.reader["read_all_registers"] = read
        self.reader["get_connection"] = Mock(return_value=None)
        self.reader["ekk_modbus_read_and_store"](3)
        read.assert_called_once_with(host="example.invalid", port=502, device_id=1,
                                    register_map=rows, model="PowerLogic ION7400")


class ProbeSiteTests(unittest.TestCase):
    def setUp(self):
        import importlib.util
        path = Path(__file__).resolve().parents[1] / "tools" / "probe_entes_emk01.py"
        spec = importlib.util.spec_from_file_location("emk_site_probe_test", path)
        self.probe = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.probe)

    def test_site_lookup_uses_only_select_and_closes_connection(self):
        conn = Mock()
        cur = conn.cursor.return_value
        expected = {"id": 52, "model": "EMK-01", "host": "example.invalid",
                    "port": 40502, "unit_id": 1}
        cur.fetchone.return_value = expected
        self.assertEqual(self.probe.load_site_endpoint(52, lambda: conn), expected)
        cur.execute.assert_called_once()
        sql, params = cur.execute.call_args.args
        self.assertTrue(sql.strip().startswith("SELECT"))
        self.assertEqual(params, (52,))
        # A passive device must be inspectable before enabling polling.
        self.assertNotIn("status = 1", sql)
        conn.commit.assert_not_called()
        cur.close.assert_called_once()
        conn.close.assert_called_once()

    def test_site_lookup_rejects_missing_or_other_model(self):
        for row in (None, {"model": "PowerLogic ION7650"}):
            conn = Mock()
            conn.cursor.return_value.fetchone.return_value = row
            with self.assertRaises(ValueError):
                self.probe.load_site_endpoint(52, lambda: conn)
            conn.close.assert_called_once()
            conn.commit.assert_not_called()

    def test_site_cli_reuses_endpoint_and_tests_all_offsets_without_write(self):
        import os
        import types
        from unittest.mock import patch
        parent = types.ModuleType("pymodbus")
        child = types.ModuleType("pymodbus.client")
        client = Mock()
        client.connect.return_value = True
        factory = Mock(return_value=client)
        child.ModbusTcpClient = factory
        parent.client = child
        dotenv = types.ModuleType("dotenv")
        dotenv.load_dotenv = Mock()
        endpoint = {"id": 123, "model": "EMK-01", "host": "example.invalid",
                    "port": 41502, "unit_id": 17}
        with patch.dict("sys.modules", {"pymodbus": parent, "pymodbus.client": child,
                                       "dotenv": dotenv}), \
             patch.dict(os.environ, {}, clear=True), \
             patch.object(self.probe, "load_site_endpoint", return_value=endpoint) as lookup, \
             patch.object(self.probe, "probe_addressing", return_value={}) as read, \
             patch("builtins.print"):
            self.assertEqual(self.probe.main(["--site-id", "52", "--timeout", "3"]), 0)
        lookup.assert_called_once_with(52)
        factory.assert_called_with(host="example.invalid", port=41502, timeout=3)
        self.assertEqual([call.args for call in read.call_args_list],
                         [(client, 17, offset) for offset in (0, 40001, 1, 40000)])
        self.assertEqual(client.close.call_count, 4)


if __name__ == "__main__":
    unittest.main()
