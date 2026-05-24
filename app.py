import logging
import time
import threading
import yaml
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Gauge, generate_latest, CONTENT_TYPE_LATEST

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

class _NoFavicon(logging.Filter):
    def filter(self, record):
        return "favicon.ico" not in record.getMessage()

logging.getLogger("uvicorn.access").addFilter(_NoFavicon())

with open("config.yaml") as f:
    CONFIG = yaml.safe_load(f)

with open("VERSION") as f:
    VERSION = f.read().strip()

BOOTSTRAP = CONFIG["kafka"]["bootstrap_servers"]

# state: { topic: { last_seen: float|None, alarm: bool } }
STATE = {}
STATE_LOCK = threading.Lock()

g_alarm = Gauge("kafka_watchdog_topic_alarm", "1 if ERROR, 0 if OK, -1 if UNKNOWN", ["topic"])
g_threshold = Gauge("kafka_watchdog_threshold_seconds", "Configured silence threshold in seconds", ["topic"])
g_last_message = Gauge("kafka_watchdog_last_message_timestamp", "Unix timestamp of last received message", ["topic"])


def watch_topic(topic, threshold):
    servers = BOOTSTRAP if isinstance(BOOTSTRAP, str) else ",".join(BOOTSTRAP)
    consumer = Consumer({
        "bootstrap.servers": servers,
        "group.id": f"watchdog-{topic}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": "true",
        "allow.auto.create.topics": "true",
    })
    consumer.subscribe([topic])
    log.info("watching topic=%s threshold=%ds", topic, threshold)

    with STATE_LOCK:
        STATE[topic] = {"last_seen": None, "alarm": False}

    while True:
        msg = consumer.poll(timeout=1.0)
        now = time.time()
        if msg is not None:
            err = msg.error()
            if err:
                if err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    log.warning("topic=%s not found yet, retrying...", topic)
                else:
                    log.error("topic=%s kafka error: %s", topic, err)
                continue

            _, ts_ms = msg.timestamp()
            with STATE_LOCK:
                STATE[topic]["last_seen"] = ts_ms / 1000
                STATE[topic]["alarm"] = False

        with STATE_LOCK:
            ps = STATE[topic]
            if ps["last_seen"] and (now - ps["last_seen"]) > threshold:
                if not ps["alarm"]:
                    log.warning("ALARM topic=%s silence=%.0fs", topic, now - ps["last_seen"])
                ps["alarm"] = True


@asynccontextmanager
async def lifespan(app):
    for wdog in CONFIG["watchdogs"]:
        t = threading.Thread(
            target=watch_topic,
            kwargs={
                "topic": wdog["topic"],
                "threshold": wdog["threshold"],
            },
            daemon=True,
            name=f"watchdog-{wdog['topic']}",
        )
        t.start()
    yield


app = FastAPI(lifespan=lifespan)


def get_state():
    with STATE_LOCK:
        raw = dict(STATE)

    result = []
    for wdog in CONFIG["watchdogs"]:
        topic = wdog["topic"]
        ps = raw.get(topic)

        if ps is None or ps["last_seen"] is None:
            status = "UNKNOWN"
            last_message = None
        else:
            status = "ERROR" if ps["alarm"] else "OK"
            last_message = datetime.fromtimestamp(ps["last_seen"], tz=timezone.utc).isoformat()

        result.append({
            "topic": topic,
            "status": status,
            "threshold_seconds": wdog["threshold"],
            "last_message": last_message,
        })

    return result


# --- endpoints ---

@app.get("/version")
def version():
    return {"version": VERSION}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/livez")
def livez():
    return {"status": "alive"}


@app.get("/readyz")
def readyz():
    with STATE_LOCK:
        has_data = any(ps["last_seen"] is not None for ps in STATE.values())
    if has_data:
        return {"status": "ready"}
    return JSONResponse(status_code=503, content={"status": "not ready", "reason": "no data yet"})


@app.get("/status")
def status():
    states = get_state()
    ready = all(s["status"] != "UNKNOWN" for s in states)
    body = {"status": "ok" if ready else "error", "version": VERSION, "states": states}
    return JSONResponse(status_code=200 if ready else 503, content=body)


@app.get("/state")
def state():
    return get_state()


@app.get("/metrics")
def metrics():
    with STATE_LOCK:
        raw = dict(STATE)
    for wdog in CONFIG["watchdogs"]:
        topic = wdog["topic"]
        ps = raw.get(topic)
        status = "UNKNOWN" if ps is None or ps["last_seen"] is None else ("ERROR" if ps["alarm"] else "OK")
        g_alarm.labels(topic=topic).set({"OK": 0, "ERROR": 1}.get(status, -1))
        g_threshold.labels(topic=topic).set(wdog["threshold"])
        if ps and ps["last_seen"]:
            g_last_message.labels(topic=topic).set(ps["last_seen"])
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
