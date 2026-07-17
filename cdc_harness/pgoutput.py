"""Minimal pgoutput (logical replication protocol v1) message parser.

Parses the binary messages returned by
``pg_logical_slot_peek_binary_changes(..., 'proto_version','1', ...)`` or a
streaming START_REPLICATION session. Only the message types the baseline
adapter needs are decoded: Relation ('R') to learn column layouts, and
Insert/Update/Delete ('I'/'U'/'D') to produce row changes. Begin/Commit and
the rest are recognised and skipped.

Pure functions over bytes — unit-tested without a database.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass


@dataclass
class Relation:
    oid: int
    namespace: str
    name: str
    replica_identity: str
    columns: list[str]

    @property
    def qualified(self) -> str:
        return f"{self.namespace}.{self.name}"


@dataclass
class RowChange:
    op: str  # insert | update | delete
    relation: Relation
    # Column name -> text value. None for SQL NULL; the sentinel
    # UNCHANGED_TOAST for TOAST columns omitted from the new tuple.
    # Deletes under REPLICA IDENTITY DEFAULT carry key columns only.
    values: dict[str, str | None]


UNCHANGED_TOAST = "\x00__unchanged_toast__"


def _cstring(buf: bytes, pos: int) -> tuple[str, int]:
    end = buf.index(b"\x00", pos)
    return buf[pos:end].decode("utf-8"), end + 1


def _tuple_data(
    buf: bytes, pos: int, columns: list[str]
) -> tuple[dict[str, str | None], int]:
    (ncols,) = struct.unpack_from(">H", buf, pos)
    pos += 2
    values: dict[str, str | None] = {}
    for i in range(ncols):
        kind = buf[pos : pos + 1]
        pos += 1
        name = columns[i] if i < len(columns) else f"col{i}"
        if kind == b"n":
            values[name] = None
        elif kind == b"u":
            values[name] = UNCHANGED_TOAST
        elif kind == b"t":
            (length,) = struct.unpack_from(">I", buf, pos)
            pos += 4
            values[name] = buf[pos : pos + length].decode("utf-8")
            pos += length
        else:
            raise ValueError(f"unknown tuple column kind {kind!r} at byte {pos - 1}")
    return values, pos


def parse_message(
    buf: bytes, relations: dict[int, Relation]
) -> RowChange | None:
    """Parse one pgoutput message.

    Relation messages update ``relations`` in place and return None.
    Insert/Update/Delete return a RowChange (Update returns the NEW tuple;
    Delete returns whatever tuple pgoutput sent — key-only under REPLICA
    IDENTITY DEFAULT). Everything else (Begin, Commit, Origin, Type,
    Truncate, logical Message) returns None.
    """
    tag = buf[0:1]
    if tag == b"R":
        pos = 1
        (oid,) = struct.unpack_from(">I", buf, pos)
        pos += 4
        namespace, pos = _cstring(buf, pos)
        name, pos = _cstring(buf, pos)
        replident = buf[pos : pos + 1].decode()
        pos += 1
        (ncols,) = struct.unpack_from(">H", buf, pos)
        pos += 2
        columns: list[str] = []
        for _ in range(ncols):
            pos += 1  # flags
            col_name, pos = _cstring(buf, pos)
            pos += 8  # type oid + typmod
            columns.append(col_name)
        relations[oid] = Relation(
            oid=oid,
            namespace=namespace or "pg_catalog",
            name=name,
            replica_identity=replident,
            columns=columns,
        )
        return None

    if tag == b"I":
        (oid,) = struct.unpack_from(">I", buf, 1)
        rel = relations[oid]
        assert buf[5:6] == b"N", "insert must carry a new tuple"
        values, _ = _tuple_data(buf, 6, rel.columns)
        return RowChange(op="insert", relation=rel, values=values)

    if tag == b"U":
        (oid,) = struct.unpack_from(">I", buf, 1)
        rel = relations[oid]
        pos = 5
        marker = buf[pos : pos + 1]
        if marker in (b"K", b"O"):
            _, pos = _tuple_data(buf, pos + 1, rel.columns)
            marker = buf[pos : pos + 1]
        assert marker == b"N", f"update: expected new tuple, got {marker!r}"
        values, _ = _tuple_data(buf, pos + 1, rel.columns)
        return RowChange(op="update", relation=rel, values=values)

    if tag == b"D":
        (oid,) = struct.unpack_from(">I", buf, 1)
        rel = relations[oid]
        marker = buf[5:6]
        assert marker in (b"K", b"O"), f"delete: expected key/old tuple, got {marker!r}"
        values, _ = _tuple_data(buf, 6, rel.columns)
        return RowChange(op="delete", relation=rel, values=values)

    # B (Begin), C (Commit), O (Origin), Y (Type), T (Truncate), M (Message):
    # nothing to extract for the canonical event stream.
    return None
