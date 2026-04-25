import asyncio
import os
import signal

from market_data_broker.logging_config import configure_logging, get_logger


async def run() -> None:
    log = get_logger("hub")
    log.info("hub starting", version="0.1.0")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    log.info("hub ready")
    await stop.wait()
    log.info("hub stopping")


def main() -> None:
    configure_logging(level=os.environ.get("LOG_LEVEL", "INFO"))
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
