from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
sys.path = [item for item in sys.path if item and Path(item).resolve() != _SCRIPT_DIR]

# These imports must follow the path cleanup: this file is launched directly,
# and its directory contains helpers whose names may shadow standard modules.
from agora import DeliveryConfig, Pipeline  # noqa: E402
from agora.core.checkpoint import Checkpoint, SQLiteCheckpointStore  # noqa: E402


class _ExitAfterSaveCheckpointStore:
    def __init__(self, path: str, *, exit_code: int = 88) -> None:
        self._inner = SQLiteCheckpointStore(path)
        self._exit_code = exit_code

    async def load(self, key: str) -> Checkpoint | None:
        return await self._inner.load(key)

    async def save(self, key: str, checkpoint: Checkpoint) -> None:
        await self._inner.save(key, checkpoint)
        os._exit(self._exit_code)

    async def close(self) -> None:
        await self._inner.close()


class _JsonlSink:
    sink_name = "jsonl"

    def __init__(self, path: str) -> None:
        self._path = Path(path)

    async def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)

    async def write(self, record: object) -> None:
        with self._path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


async def _run_kafka(config: dict[str, Any]) -> None:
    from agora_plugins.kafka import KafkaSource

    source = KafkaSource(
        topics=[str(config["topic"])],
        bootstrap_servers=str(config["bootstrap_servers"]),
        group_id=str(config["group_id"]),
        deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        commit_every=1,
        poll_timeout_ms=250,
        max_idle_polls=8,
    )
    await (
        Pipeline(source, id=str(config["pipeline_id"]))
        .build(
            _JsonlSink(str(config["output_path"])),
            config=DeliveryConfig(
                checkpoint=_ExitAfterSaveCheckpointStore(str(config["checkpoint_path"]))
            ),
        )
        .run(max_records=1)
    )


async def _run_redis(config: dict[str, Any]) -> None:
    from agora_plugins.redis import RedisStreamSource

    source = RedisStreamSource(
        url=str(config["redis_url"]),
        stream=str(config["stream"]),
        group=str(config["group"]),
        consumer=str(config["consumer"]),
        deserializer=lambda fields: int(fields["value"]),
        block_ms=250,
        batch_size=1,
    )
    await (
        Pipeline(source, id=str(config["pipeline_id"]))
        .build(
            _JsonlSink(str(config["output_path"])),
            config=DeliveryConfig(
                checkpoint=_ExitAfterSaveCheckpointStore(str(config["checkpoint_path"]))
            ),
        )
        .run(max_records=1)
    )


async def _main() -> int:
    config = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    mode = str(config["mode"])
    if mode == "kafka":
        await _run_kafka(config)
        return 0
    if mode == "redis":
        await _run_redis(config)
        return 0
    raise ValueError(f"Unsupported process-death child mode: {mode!r}")


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
