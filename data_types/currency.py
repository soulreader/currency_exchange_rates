import sqlite3
from typing import Dict, Optional


class Currency:
    __slots__ = ("code", "name", "nominal")

    def __init__(self):
        self.code: Optional[str] = None
        self.name: Optional[str] = None
        self.nominal: Optional[int] = None

    @staticmethod
    def from_dict(data: Dict):
        if data is None:
            return None
        currency = Currency()
        currency.code = data["CharCode"]
        currency.name = data["Name"]
        currency.nominal = data["Nominal"]
        return currency

    @staticmethod
    def from_row(data: sqlite3.Row):
        if data is None:
            return None
        currency = Currency()
        currency.code = data['code']
        currency.name = data['name']
        currency.nominal = data['nominal']
        return currency

    def to_tuple(self):
        return self.code, self.name, self.nominal
