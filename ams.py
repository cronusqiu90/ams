import signal
from threading import Event
import os
import sys
import time
import socket
import platform
from datetime import datetime, timedelta
import json
import glob
import re
import binascii
import sqlite3
from dataclasses import dataclass

import click
from loguru import logger
import psutil
import msgpack

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from celery import Celery


sys.tracebacklimit = 2
logger.remove()
logger.add(
    sink=sys.stdout,
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="{time:YYYY/MM/DD HH:mm:ss.SSS} [{level: >7}]: {message}",
    backtrace=True,
)

event = Event()


@dataclass
class TCLog:
    category: int
    domain: int
    module: int
    batch_id: str
    seq_id: str
    date: str
    host: str
    finish: int
    success: int
    count: int

    @staticmethod
    def create(**kwargs):
        return TCLog(
            category=int(kwargs["category"]),
            domain=int(kwargs["domain"]),
            module=int(kwargs["module"]),
            batch_id=kwargs["batch_id"],
            seq_id=kwargs["seq_id"],
            date=kwargs["date"],
            host=kwargs["hostname"],
            finish=int(kwargs["finish"]),
            success=int(kwargs["success"]),
            count=int(kwargs["count"]),
        )


class DB:
    def __init__(self, path=None):
        path = path or ":memory:"
        self._db = sqlite3.connect(path)
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS tc (
                pk INTEGER NOT NULL, 
                category INTEGER DEFAULT '0', 
                domain INTEGER DEFAULT '0', 
                module INTEGER DEFAULT '0', 
                batch_id VARCHAR(64) NOT NULL, 
                seq_id VARCHAR(16) DEFAULT '', 
                date VARCHAR(16) DEFAULT '', 
                host VARCHAR(16) NOT NULL, 
                finish INTEGER DEFAULT '0', 
                success INTEGER DEFAULT '0', 
                count INTEGER DEFAULT '0', 
                PRIMARY KEY (pk)
            );"""
        )
        self._db.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._db:
            self._db.commit()
            self._db.close()
        return False

    def upsert_tc_log(self, o: TCLog):
        cursor = self._db.cursor()
        try:
            stmt = "SELECT pk,finish,success,count FROM tc WHERE category=? AND domain=? AND module=? AND batch_id=? AND seq_id=? AND date=?;"
            where = (o.category, o.domain, o.module, o.batch_id, o.seq_id, o.date)
            cursor.execute(stmt, where)
            row = cursor.fetchone()
            if row:
                pk = row[0]
                o.finish += row[1]
                o.success += row[2]
                o.count += row[3]
                stmt = "UPDATE tc SET finish=?, success=?, count=?, host=? WHERE pk=?;"
                where = (o.finish, o.success, o.count, o.host, pk)
                cursor.execute(stmt, where)
            else:
                stmt = "INSERT INTO tc (category, domain, module, batch_id, seq_id, date, host, finish, success, count) VALUES (?,?,?,?,?,?,?,?,?,?);"
                where = (
                    o.category,
                    o.domain,
                    o.module,
                    o.batch_id,
                    o.seq_id,
                    o.date,
                    o.host,
                    o.finish,
                    o.success,
                    o.count,
                )
                cursor.execute(stmt, where)

            self._db.commit()
        finally:
            cursor.close()


def setup_signal_watchdog(): 
    def _signal_handler(name):
        def _handler(num, frame):
            logger.warning(f"signal {name} received, stopping ...")
            event.set()
            return
        return _handler

    signal.signal(signal.SIGTERM, _signal_handler("SIGTERM"))
    signal.signal(signal.SIGINT, _signal_handler("SIGINT"))

setup_signal_watchdog()

conf = {}
if not os.path.exists("ams.json"):
    raise FileNotFoundError("ams.json")
with open("ams.json", "r") as fd:
    conf = json.load(fd)

broker_url = conf["celery"]["broker_url"]
celery_app = Celery(__name__, broker=broker_url, backend="rpc://")
celery_app.conf.update(
    **{
        "broker_connection_retry_on_startup": True,
        "broker_connection_max_retries": 99,
        "worker_send_task_events": True,
        "worker_enable_remote_control": True,
        "task_reject_on_worker_lost": True,
        "task_protocol": 1,
        "task_default_queue": "crawlab.report",
        "task_default_exchange": "crawlab",


    }
)


def collect_metric():
    hostname = socket.gethostname()
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
    celery_app.send_task(name="crawlab.report.metric", args=[body], ignore_result=True)  # type: ignore


def _re_extract_values(line, pattern):
    values = {}
    for key, pattern in pattern.items():
        is_num = r"\d+" in pattern
        m = re.search(pattern, line, re.I)
        if not m:
            values[key] = 0 if is_num else ""
            continue
        v = m.group(1)
        values[key] = int(v) if is_num else v
    return values


def _extract_param_from_path(path: str):
    pre, _ = os.path.splitext(os.path.basename(path))
    parts, date = pre.split("-", 1)
    category, domain, module = parts.split("_")
    return {
        "category": int(category),
        "domain": int(domain),
        "module": int(module),
        "date": date,
    }


def _find_matched_files(dir_paths: list, date=None):
    date = date or "*"
    files = set()
    for dir_path in dir_paths:
        for file in glob.glob(f"{dir_path}*_*_*-{date}.log"):
            if event.is_set():
                raise SystemExit
            files.add(os.path.abspath(os.path.normpath(file)))
    return sorted(files)


def _collect_tc_statistic(db, date, hostname):
    files = _find_matched_files(conf["tc"]["paths"], date)
    if not files:
        return

    pattern = {
        "batch_id": r"batch_id\((.*?)\)",
        "seq_id": r"seq_id\((.*?)\)",
        "count": r"count\((\d+)\)",
        "finish": r"finish\((\d+)\)",
        "success": r"success\((\d+)\)",
        "date": r"(\d{4}-\d{2}-\d{2})",
    }
    for file in files:
        logger.info(f"parsing {file}")
        params = _extract_param_from_path(file)
        params['hostname'] = hostname

        for line in open(file, "r"):
            if event.is_set():
                return

            try:
                values = _re_extract_values(line, pattern)
                batch_id = values.get("batch_id", None)
                seq_id = values.get("seq_id", None)
                if not batch_id or not seq_id:
                    continue
                values.update(**params)
                if db.upsert_tc_log(TCLog.create(**values)) is False:
                    logger.error(f"failed to upsert log={line}")
            except Exception as err:
                logger.error(err)
                continue


def collect_statistic(date=None):
    hostname = socket.gethostname()
    yesterday = date or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    path = os.path.join(os.getcwd(), f"{yesterday}.dat")

    with DB(path) as db:
        _collect_tc_statistic(db, yesterday, hostname)

    with open(path, "rb") as fd:
        body = fd.read()
    raw = binascii.b2a_base64(msgpack.packb(body)).decode()  # type: ignore

    if yesterday == "*":
        yesterday = "all"
        
    r = celery_app.send_task(
        name="crawlab.report.statistic", args=[yesterday, hostname, raw]
    )  # type: ignore
    for _ in range(300):
        time.sleep(1)
        if event.is_set():
            return
        
        if r.state == "SUCCESS":
            logger.info("report statistic succeeded")
            os.remove(path)
            return
    logger.error(f"failed to report statistic: timeout")


@click.group(name="ams")
def cli():
    pass

@cli.command(name="once", help="run as once task")
@click.option("--date", default="*")
def run_once(date):
    collect_statistic(date)

@cli.command(name="service", help="run as service")
def run_service():
    scheduler = BackgroundScheduler(
        **{
            "executors": {
                "default": ThreadPoolExecutor(max_workers=5),
            },
            "job_defaults": {
                "coalesce": True,
                "max_instances": 1,
                "misfire_grace_time": 30,
                "replace_existing": True,
            },
        }
    )

    j1 = scheduler.add_job(collect_metric, trigger=IntervalTrigger(seconds=55))
    logger.info(str(j1))

    j2 = scheduler.add_job(
        collect_statistic,
        trigger=CronTrigger(hour="1"),
        next_run_time=datetime.now(),
    )
    logger.info(str(j2))

    try:
        scheduler.start()
        while event.is_set() is False:
            time.sleep(1)
    except Exception as ex:
        logger.exception(ex)
    finally:
        scheduler.shutdown()



if __name__ == "__main__":
    cli()
