"""Unit tests for the pgoutput binary parser — no database needed."""

from __future__ import annotations

import struct

from cdc_harness.pgoutput import UNCHANGED_TOAST, parse_message


def _cstr(s: str) -> bytes:
    return s.encode() + b"\x00"


def _relation_msg(oid: int, namespace: str, name: str, columns: list[str]) -> bytes:
    body = b"R" + struct.pack(">I", oid) + _cstr(namespace) + _cstr(name) + b"d"
    body += struct.pack(">H", len(columns))
    for col in columns:
        body += b"\x01" + _cstr(col) + struct.pack(">Ii", 20, -1)
    return body


def _tuple(values: list[str | None]) -> bytes:
    body = struct.pack(">H", len(values))
    for v in values:
        if v is None:
            body += b"n"
        elif v == UNCHANGED_TOAST:
            body += b"u"
        else:
            raw = v.encode()
            body += b"t" + struct.pack(">I", len(raw)) + raw
    return body


COLUMNS = ["pk", "seq", "tx_id", "payload", "emitted_us"]


def _relations() -> dict:
    relations: dict = {}
    assert parse_message(_relation_msg(42, "cdc_bench", "events", COLUMNS), relations) is None
    assert relations[42].qualified == "cdc_bench.events"
    assert relations[42].columns == COLUMNS
    return relations


def test_insert() -> None:
    relations = _relations()
    msg = b"I" + struct.pack(">I", 42) + b"N" + _tuple(["7", "1", "3", "\\x00ff", "123456"])
    change = parse_message(msg, relations)
    assert change is not None
    assert change.op == "insert"
    assert change.values["pk"] == "7"
    assert change.values["seq"] == "1"
    assert change.values["emitted_us"] == "123456"


def test_update_with_old_key_and_unchanged_toast() -> None:
    relations = _relations()
    old = _tuple(["7", None, None, None, None])
    new = _tuple(["7", "2", "4", UNCHANGED_TOAST, "999"])
    msg = b"U" + struct.pack(">I", 42) + b"K" + old + b"N" + new
    change = parse_message(msg, relations)
    assert change is not None
    assert change.op == "update"
    assert change.values["seq"] == "2"
    assert change.values["payload"] == UNCHANGED_TOAST


def test_delete_key_only() -> None:
    relations = _relations()
    msg = b"D" + struct.pack(">I", 42) + b"K" + _tuple(["7", None, None, None, None])
    change = parse_message(msg, relations)
    assert change is not None
    assert change.op == "delete"
    assert change.values["pk"] == "7"
    assert change.values["seq"] is None


def test_begin_commit_skipped() -> None:
    relations = _relations()
    begin = b"B" + struct.pack(">QQI", 0, 0, 1)
    commit = b"C" + b"\x00" + struct.pack(">QQQ", 0, 0, 0)
    assert parse_message(begin, relations) is None
    assert parse_message(commit, relations) is None
