import binascii
import socket
import os
import platform
from datetime import datetime

import dotenv
import msgpack
import psutil

from celery import Celery

dotenv.load_dotenv("ams.env")
broker = os.getenv("CELERY_BROKER")
hostname = socket.gethostname()

with Celery("ams", broker=broker, backend="rpc://") as app:
    app.conf.update(**{
        "broker_connection_retry_on_startup": True,
        "broker_connection_max_retries": 99,
        "worker_enable_remote_control": False,
        "worker_send_task_events": True,
        "result_expires": 60,
        "result_persistent": False,
        "task_acks_late": False,
        "task_reject_on_worker_lost": True,
        "task_protocol": 1,
        "task_default_queue": "crawlab.report",
        "task_default_exchange": "crawlab",
    })
    cpu_percent = psutil.cpu_percent(interval=1)
    cpu_count = psutil.cpu_count()
    vr_mem = psutil.virtual_memory()
    swap_mem = psutil.swap_memory()
    disk = psutil.disk_usage("/")
    metric = {
        "host": hostname,
        "ts": datetime.now().timestamp(),
        "boot": psutil.boot_time(),
        "platform": platform.platform(),
        "cpu": {
            "count": cpu_count,
            "percent": cpu_percent,
        },
        "mem": {
            "total": vr_mem.total,
            "used": vr_mem.used,
            "free": vr_mem.available,
            "percent": vr_mem.percent,
        },
        "swap": {
            "total": swap_mem.total,
            "used": swap_mem.used,
            "free": swap_mem.free,
            "percent": swap_mem.percent,
        },
        "disk": {
            "total": disk.total,
            "used": disk.used,
            "free": disk.free,
            "percent": disk.percent,
        },
    }
    body = binascii.b2a_base64(msgpack.packb(metric)).decode()  # type: ignore
    app.send_task(name="crawlab.report.metric", args=[body], ignore_result=True)
