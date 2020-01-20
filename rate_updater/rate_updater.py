import asyncio
from datetime import datetime, timedelta
from asyncio import Event
from concurrent.futures.thread import ThreadPoolExecutor
from logging import getLogger
from typing import Dict

from data_types.currency import Currency
from db_connectors.sqlite_connector import SqliteConnector
from utils.request import make_async_request, GET

logger = getLogger(__name__)

CBR_URL = "https://www.cbr-xml-daily.ru/archive/{}/daily_json.js"
CBR_DATE_FORMAT = "%Y/%m/%d"


class RateUpdater:
    def __init__(self, loop, thread_pool_executor: ThreadPoolExecutor, db_connector: SqliteConnector):
        self.db_connector = db_connector
        self.thread_pool_executor = thread_pool_executor if thread_pool_executor else ThreadPoolExecutor()
        self.loop = loop if loop else asyncio.get_event_loop()
        self.cached_dates = set()
        self.cached_currencies = {}
        self.exit = Event()

    async def rate_update_loop(self):
        """After first update next will at 3 o'clock next day"""
        try:
            while True:
                await self.get_rates()
                seconds_to_next_day_plus_three_hour = (datetime(*(datetime.now() +
                                                                  timedelta(days=1)).timetuple()[:3]) -
                                                       datetime.now()).seconds + 60*60*3
                await asyncio.sleep(seconds_to_next_day_plus_three_hour)
        except Exception as ex:
            logger.error(ex)

    async def get_rates(self):
        """Getting rates for last week using cache"""
        today_date = datetime.now().date()
        self.cached_currencies.update(await self.db_connector.get_currencies())
        self.cached_dates.update(await self.db_connector.get_rate_dates())
        for i in range(7):
            required_date = today_date - timedelta(i)
            if required_date not in self.cached_dates:
                rate_dates_count = await self.db_connector.get_certain_rate_date(required_date)
                if not rate_dates_count:
                    cbr_date_str = required_date.strftime(CBR_DATE_FORMAT)
                    err_msg, data = await make_async_request(GET,
                                                             CBR_URL.format(cbr_date_str),
                                                             loop=self.loop,
                                                             thread_pool_executor=self.thread_pool_executor)
                    await self.fill_rates_currencies(data, required_date)
                    if err_msg:
                        logger.error(err_msg)

    async def fill_rates_currencies(self, data: Dict, required_date: datetime.date):
        """Update database if rates was gotten"""
        if data:
            rates = []
            for currency_code, value in data["Valute"].items():
                currency = Currency.from_dict(value)
                if currency_code not in self.cached_currencies:
                    await self.db_connector.add_currency(currency)
                    self.cached_currencies[currency_code] = currency
                rates.append((currency_code, required_date, value["Value"]))
            if rates:
                await self.db_connector.add_currencies_rates(rates)
                self.cached_dates.add(required_date)
