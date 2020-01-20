import datetime
import sqlite3
from logging import getLogger
from typing import Union, Tuple, List, Optional, Any

import aiosqlite

from data_types.currency import Currency
from data_types.currency_rate import CurrencyRate
from utils.singleton import Singleton

logger = getLogger(__name__)

DB_NAME = "resources/currencies_rates.sqlite"


class SqliteConnection:
    def __init__(self):
        self.connection = None

    async def __aenter__(self):
        self.connection = await aiosqlite.connect(DB_NAME, detect_types=sqlite3.PARSE_DECLTYPES |
                                                                        sqlite3.PARSE_COLNAMES)
        self.connection.row_factory = aiosqlite.Row
        return self.connection

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        await self.connection.close()


class SqliteConnector(metaclass=Singleton):
    async def init_db_structure(self):
        await self._execute("""CREATE TABLE IF NOT EXISTS currencies (
                                "code" TEXT PRIMARY KEY NOT NULL UNIQUE,
                                "name" TEXT NOT NULL,
                                "nominal" INTEGER NOT NULL);""")
        await self._execute("""CREATE TABLE IF NOT EXISTS rates (
                               "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                               "code" TEXT NOT NULL,
                               "rate_date" date NOT NULL,
                               "value" REAL NOT NULL,
                               UNIQUE("code", "rate_date")
                               ) ;""")

    @staticmethod
    async def _execute(sql: str,
                       data: Union[Tuple, List] = None,
                       *,
                       with_commit=True) -> Optional[Any]:
        async with SqliteConnection() as _connector:
            try:
                if data is None:
                    cursor = await _connector.execute(sql)
                elif isinstance(data, list):
                    cursor = await _connector.executemany(sql, data)
                else:
                    cursor = await _connector.execute(sql, data)
                if with_commit:
                    await _connector.commit()
                result = await cursor.fetchall()
            except Exception as ex:
                logger.error(f"Error in _execute: {ex}")
                result = {}
            return result

    async def get_rate_dates(self):
        request = "SELECT DISTINCT(rate_date) from rates"
        rate_dates_row = await self._execute(request, with_commit=False)
        rate_dates = [rate_date['rate_date'] for rate_date in rate_dates_row]
        return rate_dates

    async def get_certain_rate_date(self, date: datetime.datetime.date):
        request = "SELECT DISTINCT(rate_date) from rates where rate_date = ?"
        rate_dates_count = await self._execute(request, (date,), with_commit=False)
        rate_dates_count = rate_dates_count[0] if rate_dates_count else None
        return rate_dates_count

    async def add_currencies_rates(self, currencies_rates: List):
        request = 'INSERT INTO rates (code,rate_date,value) VALUES(?,?,?)'
        return await self._execute(request, currencies_rates)

    async def get_today_rates(self, today_date: datetime.datetime.date):
        request = """SELECT rates.code,
                            rates.rate_date,
                            rates.value,
                            currencies.nominal,                            
                            currencies.name
                            FROM rates 
                            INNER JOIN currencies on currencies.code = rates.code
                            WHERE rate_date = ?"""
        rates = await self._execute(request, (today_date, ), with_commit=False)
        rates = [CurrencyRate.from_row(rate) for rate in rates]
        return rates

    async def add_currency(self, currency: Currency):
        request = 'INSERT INTO currencies (code, name, nominal) VALUES(?,?,?)'
        return await self._execute(request, currency.to_tuple())

    async def get_currencies(self):
        request = "SELECT code, name, nominal from currencies"
        currencies = await self._execute(request, with_commit=False)
        currencies = {currency['code']: Currency.from_row(currency) for currency in currencies}
        return currencies

    async def get_weekly_currency_rates(self, currency_code, today_date: datetime.datetime.date):
        request = """SELECT rates.code,
                            rates.rate_date,
                            rates.value,
                            currencies.nominal,                            
                            currencies.name
                            FROM rates 
                            INNER JOIN currencies on currencies.code = rates.code
                            WHERE rates.code = ? AND rates.rate_date >= ?
                            ORDER BY rate_date DESC"""
        rates = await self._execute(request, (currency_code, today_date - datetime.timedelta(7)), with_commit=False)
        rates = [CurrencyRate.from_row(rate) for rate in rates]
        return rates
