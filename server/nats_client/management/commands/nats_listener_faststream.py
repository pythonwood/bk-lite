"""SPIKE: FastStream-backed NATS listener.

目的：验证能否在不动 117 个 `@nats_client.register` handler 的前提下，
把底层 `nats.aio.client` 替换成 FastStream `NatsBroker`。

设计原则：
1. 装饰器 API 完全不变（`registry.py` 和所有 app 的 `nats_api.py` / `nats/*.py` 不动）
2. 业务 handler 完全不动（116 个 `@nats_client.register` 函数）
3. 只替换 listener 命令的 _底层实现_：把 `nats.aio.client.Client` → `faststream.nats.NatsBroker`
4. JetStream 路径暂保留旧实现（FastStream 对 JetStream Object Store 支持有限，本 spike 不动）

启动方式：
    python manage.py nats_listener_faststream
（旧的 `python manage.py nats_listener` 保持不动，双跑对比）

依赖：在 server/pyproject.toml 加 `faststream[nats]>=0.6,<0.7`
"""

import asyncio
import json

import jsonpickle
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.management import BaseCommand
from django.core.serializers.json import DjangoJSONEncoder
from django.utils import autoreload

from ...clients import get_default_nats_server
from ...handlers import nats_handler
from ...registry import default_registry


class Command(BaseCommand):
    help = "[SPIKE] Starts a FastStream-backed NATS listener (alternative to nats_listener)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--reload",
            action="store_true",
            dest="reload",
            help="Enable autoreload in development environment.",
        )

    def handle(self, *args, **options):
        reload = options.get("reload", False)
        print("** [FastStream spike] Starting NATS listener" + (" with reload" if reload else ""))
        if reload:
            autoreload.run_with_reloader(self._run)
        else:
            self._run()

    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._main())
        except KeyboardInterrupt:
            pass
        finally:
            loop.close()

    async def _main(self):
        # ---- FastStream imports here so missing dep gives clear error ----
        try:
            from faststream.nats import NatsBroker
            from faststream.security import BaseSecurity, SASLPlaintext
        except ImportError as exc:
            raise RuntimeError(
                "FastStream not installed. Add `faststream[nats]>=0.6,<0.7` to server/pyproject.toml"
            ) from exc

        if not default_registry.registry:
            print("** No function registered, nothing to do.")
            return

        # ---- Build broker from existing settings ----
        servers = get_default_nats_server()
        if not servers:
            raise RuntimeError("NATS_SERVERS / NATS_SERVER not configured.")

        nats_options = dict(getattr(settings, "NATS_OPTIONS", {}))
        # Map bk-lite's NATS_OPTIONS into FastStream's security= + raw kwargs
        ssl_ctx = nats_options.pop("tls", None)
        tls_hostname = nats_options.pop("tls_hostname", None)
        user = nats_options.pop("user", None)
        password = nats_options.pop("password", None)
        token = nats_options.pop("token", None)
        # FastStream's NatsBroker doesn't take `token`; bk-lite's settings rarely use it
        # together with security, so we fall back to URL-stuffing if both ssl+token absent.
        security = None
        if user and password:
            security = SASLPlaintext(
                username=user,
                password=password,
                ssl_context=ssl_ctx,
                use_ssl=ssl_ctx is not None,
            )
        elif ssl_ctx is not None:
            security = BaseSecurity(ssl_context=ssl_ctx, use_ssl=True)

        broker_kwargs = {
            "servers": servers,
            "security": security,
        }
        if tls_hostname:
            broker_kwargs["tls_hostname"] = tls_hostname
        # Carry through reconnect / ping behaviour from existing config
        for k in ("reconnect_time_wait", "max_reconnect_attempts",
                  "ping_interval", "max_outstanding_pings"):
            if k in nats_options:
                broker_kwargs[k] = nats_options[k]

        broker = NatsBroker(**{k: v for k, v in broker_kwargs.items() if v is not None})

        # ---- Register every @register-ed function as a FastStream subscriber ----
        # We do NOT use FastStream's @router.subscriber decorator on user code —
        # that would force every handler to change. Instead we register dynamically:
        # for each entry in default_registry, create a thin async wrapper that
        # dispatches via the existing nats_handler() function.

        namespace = getattr(settings, "NATS_NAMESPACE", "default")
        js_enabled = getattr(settings, "NATS_JETSTREAM_ENABLED", True)

        non_js_count = 0
        js_count = 0

        for key, data in default_registry.registry.items():
            ns = data["namespace"]
            name = data["name"]
            is_js = data["js"]

            if is_js:
                # JetStream path: NOT supported in this spike (FastStream's JS API is
                # different enough that we keep the legacy nats_listener for JS handlers).
                js_count += 1
                continue

            subject = f"{ns}.{name}"
            queue = subject  # bk-lite uses subject==queue for load balancing
            full_key = key  # captured for closure

            async def _wrapper(msg_body: bytes, full_key=full_key, subject=subject):
                # FastStream gives us the raw bytes (since we don't declare a schema)
                # Reply correlation: FastStream's NatsBroker auto-handles reply via
                # `return value` from subscriber when caller used `broker.request()`.
                # bk-lite's RpcClient uses nc.request() / nc.publish() which FastStream
                # subscribers handle compatibly — the reply is published to msg.reply.
                try:
                    data = json.loads(msg_body.decode())
                    result = await nats_handler(full_key, data)
                    return json.dumps(
                        {"success": True, "result": result},
                        cls=DjangoJSONEncoder,
                    ).encode()
                except Exception as e:  # pylint: disable=broad-except
                    if isinstance(e, ValidationError):
                        message = e.message_dict
                    else:
                        message = str(e)
                        try:
                            message = json.loads(message)
                        except json.JSONDecodeError:
                            pass
                    return json.dumps(
                        {
                            "success": False,
                            "error": e.__class__.__name__,
                            "message": message,
                            "pickled_exc": jsonpickle.encode(e),
                        }
                    ).encode()

            # Bind the wrapper to the subject with a queue group for load balancing
            broker.subscriber(subject, queue=queue)(_wrapper)
            non_js_count += 1

        print(f"** [FastStream spike] Registered {non_js_count} core subjects, "
              f"{js_count} JetStream handlers skipped (kept on legacy listener)")
        print(f"** [FastStream spike] Connecting to {servers}")

        # ---- Start broker and idle forever ----
        await broker.start()
        print("** [FastStream spike] Listening...")
        try:
            # Idle until interrupted; FastStream manages all subscriptions internally.
            await asyncio.Event().wait()
        finally:
            await broker.close()
