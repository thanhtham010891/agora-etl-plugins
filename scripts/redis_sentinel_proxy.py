from __future__ import annotations

import asyncio
import os


def _encode_resp_command(*parts: str) -> bytes:
    encoded = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        payload = part.encode("utf-8")
        encoded.append(f"${len(payload)}\r\n".encode())
        encoded.append(payload + b"\r\n")
    return b"".join(encoded)


async def _read_resp_line(reader: asyncio.StreamReader) -> bytes:
    line = await reader.readline()
    if not line:
        raise ConnectionError("Sentinel closed the connection unexpectedly.")
    return line.rstrip(b"\r\n")


async def _read_resp_bulk_string(reader: asyncio.StreamReader) -> str:
    length_line = await _read_resp_line(reader)
    if not length_line.startswith(b"$"):
        raise ValueError(f"Expected bulk string length, got: {length_line!r}")
    length = int(length_line[1:])
    if length < 0:
        raise ValueError("Sentinel returned a null bulk string.")
    payload = await reader.readexactly(length)
    await reader.readexactly(2)
    return payload.decode("utf-8")


async def _query_current_master(
    *,
    sentinel_host: str,
    sentinel_port: int,
    master_name: str,
    timeout_s: float = 3.0,
) -> tuple[str, int]:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(sentinel_host, sentinel_port),
        timeout=timeout_s,
    )
    try:
        writer.write(_encode_resp_command("SENTINEL", "get-master-addr-by-name", master_name))
        await writer.drain()
        header = await _read_resp_line(reader)
        if not header.startswith(b"*2"):
            raise ValueError(f"Unexpected Sentinel response header: {header!r}")
        host = await _read_resp_bulk_string(reader)
        port = int(await _read_resp_bulk_string(reader))
        return host, port
    finally:
        writer.close()
        await writer.wait_closed()


async def _pipe_stream(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    try:
        while chunk := await reader.read(65536):
            writer.write(chunk)
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def _handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
) -> None:
    sentinel_host = os.getenv("REDIS_SENTINEL_HOST", "redis-sentinel")
    sentinel_port = int(os.getenv("REDIS_SENTINEL_PORT", "26379"))
    master_name = os.getenv("REDIS_SENTINEL_MASTER_NAME", "mymaster")
    upstream_host, upstream_port = await _query_current_master(
        sentinel_host=sentinel_host,
        sentinel_port=sentinel_port,
        master_name=master_name,
    )
    upstream_reader, upstream_writer = await asyncio.open_connection(upstream_host, upstream_port)

    client_to_upstream = asyncio.create_task(_pipe_stream(client_reader, upstream_writer))
    upstream_to_client = asyncio.create_task(_pipe_stream(upstream_reader, client_writer))
    done, pending = await asyncio.wait(
        {client_to_upstream, upstream_to_client},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    for task in done:
        task.result()


async def _main() -> None:
    host = os.getenv("REDIS_PROXY_HOST", "0.0.0.0")
    port = int(os.getenv("REDIS_PROXY_PORT", "6379"))
    server = await asyncio.start_server(_handle_client, host, port)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(_main())
