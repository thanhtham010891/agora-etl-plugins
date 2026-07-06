"""Connection and reconnect helpers for Redis stream sources."""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any

from agora_plugins.redis.connection import build_async_redis_client

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from datetime import datetime

    from agora.core.retry import RetryPolicy


class RedisConnectionRuntime:
    """Owns client creation, group setup, and reconnect/retry orchestration."""

    def __init__(
        self,
        source: Any,
        *,
        now_utc: Callable[[], datetime],
    ) -> None:
        self._source = source
        self._now_utc = now_utc

    async def build_client(self) -> Any:
        try:
            __import__("redis.asyncio")
        except ImportError:
            raise ImportError(
                "RedisStreamSource requires redis. Install via: pip install 'agora-etl-plugins[redis]'"
            ) from None
        return await build_async_redis_client(
            url=self._source._url,
            decode_responses=self._source._decode_responses,
            redis_cluster=self._source._redis_cluster,
            sentinel_service_name=self._source._sentinel_service_name,
            sentinel_urls=self._source._sentinel_urls,
        )

    async def ensure_group(self, client: Any) -> None:
        try:
            await client.xgroup_create(
                self._source._stream,
                self._source._group,
                id="0",
                mkstream=True,
            )
            self._source._group_ready = True
            self._source.logger.info(
                "redis_stream_group_created",
                stream=self._source._stream,
                group=self._source._group,
            )
        except Exception as exc:
            if "BUSYGROUP" in str(exc):
                self._source._group_ready = True
                self._source.logger.debug(
                    "redis_stream_group_exists",
                    stream=self._source._stream,
                    group=self._source._group,
                )
            else:
                self._source._remember_error(exc)
                raise

    def require_client(self) -> Any:
        if self._source._client is None:
            raise RuntimeError("RedisStreamSource.open() was not called")
        return self._source._client

    def remember_error(self, exc: Exception) -> None:
        self._source._last_error = str(exc)
        self._source._last_error_at = self._now_utc()

    def is_retryable_connection_error(self, exc: Exception) -> bool:
        try:
            from redis.exceptions import ConnectionError, ReadOnlyError, TimeoutError
        except ImportError:
            return False
        return isinstance(exc, (ConnectionError, TimeoutError, ReadOnlyError))

    async def recover_from_connection_error(
        self,
        exc: Exception,
        *,
        context: str,
        reconnect_client: Callable[[], Awaitable[None]],
    ) -> bool:
        if not self.is_retryable_connection_error(exc):
            return False
        self.remember_error(exc)
        self._source.logger.warning(
            "redis_stream_retryable_connection_error",
            stream=self._source._stream,
            group=self._source._group,
            consumer=self._source._consumer,
            context=context,
            error=str(exc),
        )
        await reconnect_client()
        return True

    async def reconnect_client(
        self,
        *,
        build_client: Callable[[], Awaitable[Any]],
        ensure_group: Callable[[Any], Awaitable[None]],
        retry_policy: RetryPolicy[Any],
    ) -> None:
        previous_client = self._source._client
        self._source._client = None
        self._source._group_ready = False
        if previous_client is not None:
            with contextlib.suppress(Exception):
                await previous_client.aclose()

        last_error: Exception | None = None
        attempt = 1
        while True:
            try:
                client = await build_client()
                await ensure_group(client)
                self._source._client = client
                self._source._reconnect_count += 1
                self._source._last_reconnect_at = self._now_utc()
                self._source.logger.info(
                    "redis_stream_source_reconnected",
                    stream=self._source._stream,
                    group=self._source._group,
                    consumer=self._source._consumer,
                    reconnect_count=self._source._reconnect_count,
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception as reconnect_error:
                last_error = reconnect_error
                self.remember_error(reconnect_error)
                if not retry_policy.should_retry(reconnect_error, attempt=attempt):
                    break
                delay = retry_policy.backoff_for(attempt=attempt)
                self._source.logger.warning(
                    "redis_stream_source_reconnect_retry",
                    stream=self._source._stream,
                    group=self._source._group,
                    consumer=self._source._consumer,
                    attempt=attempt,
                    wait_s=delay,
                    error=str(reconnect_error),
                )
                if delay > 0:
                    await asyncio.sleep(delay)
                attempt += 1
        if last_error is not None:
            raise last_error
