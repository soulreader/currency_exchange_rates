import sqlite3
from datetime import datetime
from typing import Optional, Dict


class CurrencyRate:
    __slots__ = ("code", "name", "date", "value", "nominal")

    def __init__(self):
        self.code: Optional[str] = None
        self.date: Optional[datetime.date] = None
        self.value: Optional[float] = None

    @staticmethod
    def from_dict(data: Dict, date: datetime.date):
        if data is None:
            return None
        currency_rate = CurrencyRate()
        currency_rate.code = data["CharCode"]
        currency_rate.date = date
        currency_rate.value = data["Value"]
        return currency_rate

    @staticmethod
    def from_row(data: sqlite3.Row):
        if data is None:
            return None
        currency_rate = CurrencyRate()
        currency_rate.code = data["code"]
        currency_rate.date = data["rate_date"]
        currency_rate.value = data["value"]
        currency_rate.name = data["name"]
        currency_rate.nominal = data["nominal"]
        return currency_rate

    def to_tuple(self):
        return self.code, self.date, self.value
