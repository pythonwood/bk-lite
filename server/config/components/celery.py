import importlib
import logging
import os

from config.components.locale import TIME_ZONE

logger = logging.getLogger("celery_config")

IS_USE_CELERY = os.getenv("ENABLE_CELERY", "False").lower() == "true"
# celery
CELERY_IMPORTS = ()
CELERY_TIMEZONE = TIME_ZONE  # celery 时区问题
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "amqp://admin:password@rabbitmq.lite/")

# celery -A apps.core.celery flower --port=5555 -E
# celery -A apps.core.celery flower --port=5555 --address=0.0.0.0 --enable-events --persistent=True --db=/tmp/flower.db --max-tasks=10000
# 2026-05-22 反馈 这两块我正在加强可观测性建设。
# nats方面我nat-surevyor 和faststream 自行监控 celery方面建议server容器中镜像安装flower，增加flower的supervisor配置。
CELERY_WORKER_SEND_TASK_EVENTS = True
CELERY_TASK_SEND_SENT_EVENT = True
# 我本地加了，想提pr的。不过发现  requirements.txt 和 supervisor配置 我看源码没有的。 明天再说

if IS_USE_CELERY:
    INSTALLED_APPS = locals().get("INSTALLED_APPS", [])
    INSTALLED_APPS += (
        "django_celery_beat",
        "django_celery_results",
    )
    CELERY_ENABLE_UTC = True
    CELERY_WORKER_CONCURRENCY = 2  # 并发数
    CELERY_MAX_TASKS_PER_CHILD = 5  # worker最多执行5个任务便自我销毁释放内存
    CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers.DatabaseScheduler"
    CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True
    CELERY_ACCEPT_CONTENT = ["application/json"]
    CELERY_TASK_SERIALIZER = "json"
    CELERY_RESULT_SERIALIZER = "json"
    CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND")
    DJANGO_CELERY_BEAT_TZ_AWARE = True

    CELERY_BEAT_SCHEDULE = {}
    for app_label in INSTALLED_APPS:
        config_module = f"{app_label}.config"
        try:
            mod = importlib.import_module(config_module)
            app_schedule = getattr(mod, "CELERY_BEAT_SCHEDULE", None)
            if app_schedule:
                CELERY_BEAT_SCHEDULE.update(app_schedule)
        except ImportError:
            pass
        except Exception:
            logger.exception("Failed to load CELERY_BEAT_SCHEDULE from %s", config_module)
