#!/usr/bin/env python3
"""Keep bounded unauthenticated pressure on the isolated test listeners."""

import argparse
import asyncio
import json
import os
import signal
import time
from pathlib import Path


def initial_state(endpoint):
    node, host, port, transport = endpoint.split(",")
    return {
        "node": node,
        "host": host,
        "port": int(port),
        "transport": transport,
        "attempts": 0,
        "connected": 0,
        "closed": 0,
        "early_closed": 0,
        "client_timeouts": 0,
        "connect_errors": 0,
        "close_errors": 0,
        "held": 0,
        "max_held": 0,
    }


def payload(transport, mode):
    if mode == 0:
        return b"\x00"
    if mode == 1:
        # A partial versioned hello holds only a bounded pre-authentication slot.
        return b"KAFDAUTH\x03"
    return b"OBSOLETE"


async def worker(state, mode):
    body = payload(state["transport"], mode)
    while True:
        writer = None
        retained = False
        state["attempts"] += 1
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(state["host"], state["port"]), 1
            )
            state["connected"] += 1
            writer.write(body)
            await asyncio.wait_for(writer.drain(), 1)
            try:
                await asyncio.wait_for(reader.read(1), 0.5)
                state["early_closed"] += 1
            except TimeoutError:
                retained = True
                state["held"] += 1
                state["max_held"] = max(state["max_held"], state["held"])
                try:
                    await asyncio.wait_for(reader.read(1), 6)
                except TimeoutError:
                    state["client_timeouts"] += 1
            state["closed"] += 1
        except (OSError, TimeoutError):
            if writer is None:
                state["connect_errors"] += 1
            else:
                state["closed"] += 1
                if not retained:
                    state["early_closed"] += 1
        finally:
            if retained:
                state["held"] -= 1
            if writer is not None:
                writer.close()
                try:
                    await asyncio.wait_for(writer.wait_closed(), 1)
                except (OSError, TimeoutError):
                    state["close_errors"] += 1
        await asyncio.sleep(0.1)


def write_status(path, state):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state) + "\n", encoding="ascii")
    os.replace(temporary, path)


async def run(args):
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopped.set)
    endpoints = [initial_state(endpoint) for endpoint in args.endpoint]
    state = {"running": True, "elapsed": 0, "endpoints": endpoints}
    tasks = [
        asyncio.create_task(worker(endpoint, index % 3))
        for endpoint in endpoints
        for index in range(args.connections)
    ]
    started = time.monotonic()
    try:
        while not stopped.is_set() and not args.stop.exists():
            state["elapsed"] = round(time.monotonic() - started, 2)
            if state["elapsed"] >= args.duration:
                raise TimeoutError("pressure scenario did not stop before its deadline")
            for task in tasks:
                if task.done():
                    task.result()
                    raise RuntimeError("pressure worker stopped unexpectedly")
            write_status(args.status, state)
            await asyncio.sleep(0.2)
    finally:
        for task in tasks:
            task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        failures = [
            str(result)
            for result in results
            if isinstance(result, BaseException)
            and not isinstance(result, asyncio.CancelledError)
        ]
        state["running"] = False
        state["worker_errors"] = failures
        write_status(args.status, state)
        print(json.dumps(state), flush=True)
        if failures:
            raise RuntimeError(f"pressure workers failed: {failures}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", action="append", required=True)
    parser.add_argument("--status", type=Path, required=True)
    parser.add_argument("--stop", type=Path, required=True)
    parser.add_argument("--duration", type=int, default=90)
    parser.add_argument("--connections", type=int, default=32)
    args = parser.parse_args()
    # The three-VIP fallback adds 34.5 seconds to the 90-second pressure window.
    if not 9 <= args.connections <= 64 or not 1 <= args.duration <= 125:
        parser.error("connections must be 9 to 64 and duration 1 to 125 seconds")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
