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


if __name__ == "__main__":
    unittest.main()
