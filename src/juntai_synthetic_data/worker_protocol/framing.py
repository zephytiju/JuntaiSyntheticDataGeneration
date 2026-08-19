"""Canonical SWP/v1 Unix-socket frame codec."""

from __future__ import annotations

import socket
import struct

from .models import MAX_FRAME_BYTES, Envelope, ProtocolError, decode_envelope


def encode_frame(envelope: Envelope) -> bytes:
    payload = envelope.canonical_bytes()
    return struct.pack(">I", len(payload)) + payload


def _read_exact(stream: socket.socket, length: int) -> bytes:
    parts: list[bytes] = []
    remaining = length
    while remaining:
        chunk = stream.recv(remaining)
        if not chunk:
            raise ProtocolError("ENVELOPE_INVALID", "truncated SWP frame")
        parts.append(chunk)
        remaining -= len(chunk)
    return b"".join(parts)


def read_frame(stream: socket.socket) -> Envelope:
    header = _read_exact(stream, 4)
    (length,) = struct.unpack(">I", header)
    if length == 0 or length > MAX_FRAME_BYTES:
        raise ProtocolError("ENVELOPE_INVALID", "invalid SWP frame length")
    return decode_envelope(_read_exact(stream, length))


def write_frame(stream: socket.socket, envelope: Envelope) -> None:
    stream.sendall(encode_frame(envelope))
