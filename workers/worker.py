"""
Python worker: registers with the dispatcher, consumes its dedicated RabbitMQ queue,
executes subtasks (simulated work), and reports status back to the dispatcher.
"""
from __future__ import annotations

import asyncio
import json
import os
import socket
from contextlib import asynccontextmanager
from typing import Any

import aio_pika
import httpx
from aio_pika import IncomingMessage
from fastapi import FastAPI


def env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None:
        raise RuntimeError(f"missing env {name}")
    return v


WORKER_ID = env("WORKER_ID", socket.gethostname())
HTTP_PORT = int(env("HTTP_PORT", "9101"))
DISPATCHER_URL = env("DISPATCHER_URL", "http://localhost:8081").rstrip("/")
RABBITMQ_URL = env("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
SUBTASK_SLEEP = float(env("SUBTASK_SLEEP_SECONDS", "1"))
WORKER_CAPACITY = int(env("WORKER_CAPACITY", "4"))
WORKER_HOST = env("WORKER_HOST", socket.gethostname())

consumer_task: asyncio.Task | None = None
rabbit_connection: aio_pika.RobustConnection | None = None
http_client: httpx.AsyncClient | None = None


async def register_worker() -> None:
    assert http_client is not None
    payload = {
        "worker_id": WORKER_ID,
        "host": WORKER_HOST,
        "port": HTTP_PORT,
        "capacity": WORKER_CAPACITY,
    }
    r = await http_client.post(f"{DISPATCHER_URL}/register", json=payload, timeout=10.0)
    r.raise_for_status()


async def deregister_worker() -> None:
    if http_client is None:
        return
    try:
        r = await http_client.delete(f"{DISPATCHER_URL}/register/{WORKER_ID}", timeout=5.0)
        if r.status_code not in (200, 204):
            return
    except Exception:
        return


async def report_status(job_id: str, subtask_id: str, status: str, error: str | None = None) -> None:
    assert http_client is not None
    body: dict[str, Any] = {"status": status}
    if error:
        body["error"] = error
    url = f"{DISPATCHER_URL}/jobs/{job_id}/subtasks/{subtask_id}/status"
    r = await http_client.post(url, json=body, timeout=10.0)
    r.raise_for_status()


async def handle_message(message: IncomingMessage) -> None:
    async with message.process(requeue=False):
        try:
            payload = json.loads(message.body.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            print(f"bad message: {exc}")
            return

        job_id = payload.get("job_id")
        subtask_id = payload.get("subtask_id")
        instruction = payload.get("instruction", "")
        if not job_id or not subtask_id:
            print("missing job_id/subtask_id")
            return

        try:
            await report_status(job_id, subtask_id, "running")
            await asyncio.sleep(SUBTASK_SLEEP)
            result = {
                "worker": WORKER_ID,
                "instruction": instruction,
                "result": f"processed order={payload.get('order')} attempt={payload.get('attempt', 1)}",
            }
            await report_status(job_id, subtask_id, "completed")
            print(f"completed {job_id}/{subtask_id}: {result}")
        except Exception as exc:  # noqa: BLE001
            print(f"subtask failed: {exc}")
            try:
                await report_status(job_id, subtask_id, "failed", error=str(exc))
            except Exception:
                pass


async def consume_loop() -> None:
    global rabbit_connection
    rabbit_connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await rabbit_connection.channel()
    await channel.set_qos(prefetch_count=1)
    queue_name = f"worker.{WORKER_ID}"
    queue = await channel.declare_queue(queue_name, durable=True)
    print(f"worker {WORKER_ID} listening on {queue_name}")

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            await handle_message(message)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client, consumer_task
    http_client = httpx.AsyncClient()
    await register_worker()
    print(f"registered {WORKER_ID} at {WORKER_HOST}:{HTTP_PORT}")
    consumer_task = asyncio.create_task(consume_loop())
    try:
        yield
    finally:
        if consumer_task:
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
        await deregister_worker()
        if rabbit_connection:
            await rabbit_connection.close()
        if http_client:
            await http_client.aclose()


app = FastAPI(title=f"ATE Worker {WORKER_ID}", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "worker_id": WORKER_ID}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT, log_level="info")
