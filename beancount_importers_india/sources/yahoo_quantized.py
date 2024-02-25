"""
This source is a wrapper around pricehist source.
Since BSE data fetched from yahoo by pricehist is not quantized, we get quotes like INR 120.099999234 while the actual price is INR 120.1
In beanount, there is no way to pass the quantize flag to pricehist source, hence this source is a wrapper around the pricehist source that quantizes the prices fetched from the pricehist source.
"""
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import List, NamedTuple, Optional


from pricehist import exceptions
from pricehist.series import Series
from pricehist.sources.yahoo import Yahoo

SourcePrice = NamedTuple(
    "SourcePrice",
    [
        ("price", Decimal),
        ("time", Optional[datetime]),
        ("quote_currency", Optional[str]),
    ],
)

class Source:
    def get_latest_price(self, ticker: str) -> Optional[SourcePrice]:
        time_end = datetime.combine(date.today(), datetime.min.time())
        time_begin = time_end - timedelta(days=7)
        prices = self.get_prices_series(ticker, time_begin, time_end)
        if prices:
            return prices[-1]
        else:
            return None

    def get_historical_price(
        self, ticker: str, time: datetime
    ) -> Optional[SourcePrice]:
        prices = self.get_prices_series(ticker, time, time)
        if prices:
            return prices[-1]
        else:
            return None

    def get_prices_series(
        self,
        ticker: str,
        time_begin: datetime,
        time_end: datetime,
    ) -> Optional[List[SourcePrice]]:
        base, quote, type = self._decode(ticker)

        start = time_begin.date().isoformat()
        end = time_end.date().isoformat()

        local_tz = datetime.now(timezone.utc).astimezone().tzinfo
        user_tz = time_begin.tzinfo or local_tz

        try:
            series = Yahoo().fetch(Series(base, quote, type, start, end))
        except exceptions.SourceError:
            return None

        return [
            SourcePrice(
                price.amount.quantize(Decimal("1.00")),
                datetime.fromisoformat(price.date).replace(tzinfo=user_tz),
                series.quote,
            )
            for price in series.prices
        ]

    def _decode(self, ticker):
        # https://github.com/beancount/beanprice/blob/b05203/beanprice/price.py#L166
        parts = [
            re.sub(r"_[0-9a-fA-F]{2}", lambda m: chr(int(m.group(0)[1:], 16)), part)
            for part in ticker.split(":")
        ]
        base, quote, candidate_type = (parts + [""] * 3)[0:3]
        type = candidate_type or Yahoo().types()[0]
        return (base, quote, type)