import asyncio
import os
import time
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Thread

from db_connectors.sqlite_connector import SqliteConnector
from rate_updater.rate_updater import RateUpdater
from utils.asyncio_helper import run_loop
from web.web_app import WebApp

ONE_DAY_IN_SECONDS = 60*60*24

if __name__ == "__main__":
    thread_pool_executor = ThreadPoolExecutor()
    loop = asyncio.get_event_loop()
    Thread(target=lambda: run_loop(loop)).start()
    db_connector = SqliteConnector()
    asyncio.run_coroutine_threadsafe(db_connector.init_db_structure(), loop)

    # for sure database initiate
    time.sleep(3)

    rate_updater = RateUpdater(loop, thread_pool_executor, db_connector)
    asyncio.run_coroutine_threadsafe(rate_updater.rate_update_loop(), loop)

    web_app = WebApp(rate_updater.cached_currencies, loop, db_connector)
    Thread(target=web_app.start).start()
    try:
        while True:
            time.sleep(ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        os._exit(0)
