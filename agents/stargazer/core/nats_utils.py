# -*- coding: utf-8 -*-
# @File: nats_utils.py
# @Time: 2025/12/16
# @Author: windyzhao
# @Modified: 2026-06-11 — singleton client (per-process) to eliminate per-call TLS handshake.
"""
NATS 通用工具方法
提供简洁的 NATS 请求封装，无需手动管理连接

[2026-06-11 hotfix]
原实现每次调用 nats_request/nats_publish 都新建 client + connect + drain，
在 TLS 启用环境下每次调用都要做一次 TLS 握手，造成 stargazer worker CPU 脉冲飙到 200%+，
NATS server 看到 stargazer 单容器 100+ 短命连接堆积。
改为按 (event_loop, config_signature) 做 singleton：每个 worker 进程一条长连接，
nats-py 自带 reconnect/ping，连接断了自动恢复。
"""
import asyncio
import json
from typing import Any, Optional, Tuple
from nats.aio.client import Client as NATS
from sanic.log import logger
from core.nats import NATSConfig


# 按 (event_loop_id, config 签名) 缓存。多进程下每个进程都会自己新建一份，
# 同进程内多个 event loop（罕见，但 sanic 多 worker 场景可能出现）也彼此隔离。
_clients: dict = {}
_locks: dict = {}
# 缓存 (loop_id) -> (config, key)，避免快路径每次都 NATSConfig.from_env()。
# from_env 不仅做 regex 解析，还会走 logger.info("Parsed NATS servers: ...")，
# 在 publish_metrics_to_nats 这种 N 行 batch 路径下会刷出几千条日志。
_config_cache: dict = {}


def _get_cached_config_and_key():
    """快速取 config / key — 单 loop 内只解析一次环境。"""
    loop = asyncio.get_event_loop()
    loop_id = id(loop)
    cached = _config_cache.get(loop_id)
    if cached is not None:
        return cached
    config = NATSConfig.from_env()
    key = (loop_id, _config_key(config))
    _config_cache[loop_id] = (config, key)
    return config, key


def _config_key(config: NATSConfig) -> Tuple:
    """生成 config 的可哈希签名（仅捕影响连接的字段）。"""
    return (
        tuple(config.servers),
        config.user,
        config.password,
        config.tls_enabled,
        config.tls_insecure,
        config.tls_ca_file,
        config.tls_cert_file,
        config.tls_key_file,
        config.tls_hostname,
        config.connect_timeout,
        config.max_reconnect_attempts,
        config.reconnect_time_wait,
        config.ping_interval,
        config.max_outstanding_pings,
    )


async def _get_client() -> NATS:
    """获取或创建当前 event loop 内的共享 NATS 客户端。"""
    config, key = _get_cached_config_and_key()

    # 双检锁：先无锁快路径，命中直接返回
    nc = _clients.get(key)
    if nc is not None and nc.is_connected:
        return nc

    lock = _locks.setdefault(key, asyncio.Lock())
    async with lock:
        nc = _clients.get(key)
        if nc is not None and nc.is_connected:
            return nc

        # 旧 client 存在但状态不健康（closed/reconnecting 卡死） — 丢掉重建
        if nc is not None:
            try:
                if not nc.is_closed:
                    await nc.close()
            except Exception as err:
                logger.warning(f"[NATS] failed to close stale client: {err}")

        new_nc = NATS()
        await new_nc.connect(**config.to_connect_options())
        _clients[key] = new_nc
        logger.info(
            f"[NATS] singleton client connected: servers={config.servers}, tls={config.tls_enabled}"
        )
        return new_nc


async def _drop_cached_client():
    """连接失败时丢弃缓存的 client，下次 _get_client 重建。"""
    _, key = _get_cached_config_and_key()
    bad = _clients.pop(key, None)
    if bad is not None:
        try:
            if not bad.is_closed:
                await bad.close()
        except Exception:
            pass


async def _ensure_request(subject: str, payload: bytes, timeout: float) -> dict:
    """实际发送 request，单次重试以兜住"短暂断连后客户端状态怪异"。"""
    try:
        nc = await _get_client()
        response_msg = await nc.request(subject, payload=payload, timeout=timeout)
    except Exception as err:
        # 第一次失败 — 把缓存里那条连接干掉，下次 _get_client 会重建
        logger.warning(
            f"[NATS] request failed once, dropping cached client: {type(err).__name__}: {err}"
        )
        await _drop_cached_client()
        # 重试一次
        nc = await _get_client()
        response_msg = await nc.request(subject, payload=payload, timeout=timeout)

    return json.loads(response_msg.data.decode())


async def nats_request(subject: str, payload: bytes, timeout: float = 30.0) -> dict:
    """
    通用的 NATS 请求方法（singleton 客户端，进程级共享）。

    Args:
        subject: NATS 主题
        payload: 请求负载（已编码的字节数据）
        timeout: 超时时间（秒），默认 30 秒

    Returns:
        解析后的响应数据（字典格式）
    """
    try:
        return await _ensure_request(subject, payload, timeout)
    except Exception as e:
        logger.error(f"NATS request failed: {type(e).__name__}: {e}")
        raise


async def nats_publish_raw(subject: str, raw_bytes: bytes) -> None:
    """
    发布原始字节到 NATS（不做 JSON 编码，给 telegraf line-protocol 这种用）。

    复用 singleton 客户端。失败时丢掉缓存的连接，下次 _get_client 重建。
    Note: 只重建连接，不重试 publish —— 上层（如 publish_metrics_to_nats）
    自己控制 batch 是否要 retry。
    """
    try:
        nc = await _get_client()
        await nc.publish(subject, raw_bytes)
    except Exception as err:
        logger.warning(
            f"[NATS] publish_raw failed, dropping cached client: {type(err).__name__}: {err}"
        )
        await _drop_cached_client()
        raise


async def nats_publish(subject: str, data: Any) -> None:
    """
    通用的 NATS 发布方法（singleton 客户端，进程级共享）。

    Args:
        subject: NATS 主题
        data: 要发布的数据（将自动转换为 JSON）
    """
    try:
        nc = await _get_client()
        payload = json.dumps(data).encode()
        await nc.publish(subject, payload)
    except Exception as err:
        # 一次重试
        logger.warning(
            f"[NATS] publish failed once, dropping cached client: {type(err).__name__}: {err}"
        )
        await _drop_cached_client()
        try:
            nc = await _get_client()
            payload = json.dumps(data).encode()
            await nc.publish(subject, payload)
        except Exception as e:
            logger.error(f"NATS publish failed: {type(e).__name__}: {e}")
            raise
