import asyncio
import datetime
from typing import Dict

from flask import Flask, render_template, request

from db_connectors.sqlite_connector import SqliteConnector


class WebApp:
    def __init__(self, currencies: Dict, loop, db_connector):
        self.currencies = currencies
        self.db_connector: SqliteConnector = db_connector
        self.loop = loop
        self.app = Flask(__name__)

    def start(self):
        self.add_endpoint(endpoint='/', endpoint_name='/', view_func=self.main_page)
        self.add_endpoint(endpoint='/weekly_rates', endpoint_name='weekly_rates', view_func=self.weekly_rates)
        try:
            self.app.run()
        except RuntimeError:
            pass

    def add_endpoint(self, endpoint=None, endpoint_name=None, view_func=None):
        self.app.add_url_rule(endpoint, endpoint_name, view_func=view_func, methods=['GET', 'POST'])

    def main_page(self):
        for i in range(3):
            today_date = datetime.datetime.now().date() - datetime.timedelta(i)
            future = asyncio.run_coroutine_threadsafe(self.db_connector.get_today_rates(today_date), self.loop)
            rates = future.result()
            if rates:
                break
        return render_template(
            "index.html",
            today_rates=rates, date=today_date
        )

    def weekly_rates(self):
        currency_select = request.form.get('currency_select')
        if not currency_select:
            currency_select = "AUD"
        today_date = datetime.datetime.now().date()
        future = asyncio.run_coroutine_threadsafe(self.db_connector.get_weekly_currency_rates(currency_select,
                                                                                              today_date), self.loop)
        rates = future.result()
        return render_template(
            "weekly_rates.html", rates=rates, currencies=self.currencies,
            currency=self.currencies.get(currency_select).name
        )
