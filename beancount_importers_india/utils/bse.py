import re
from textwrap import dedent
from pathlib import Path
import json
import subprocess
import datetime
import numpy as np
import requests


BSE_DATA = Path(__file__).parent.absolute()/'bse.json'
TICKER_KEY = 'scrip_id'
ISIN_KEY = 'ISIN_NUMBER'
NAME_KEY = 'SCRIP_NAME'
PRICE_KEY = 'wap' # weight average price
SCRIP_CODE_KEY = 'SCRIP_CD'


class BSEClient:
    """ A Client to interact with Bombay Stock Exchange, to fetch company data and prices
    """
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'referer':'https://www.bseindia.com/',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            })
        # We refresh the data if it's older than 2 days
        if not BSE_DATA.exists() or datetime.date.today() - datetime.datetime.fromtimestamp(BSE_DATA.stat().st_mtime).date() > datetime.timedelta(days=2):
            # download the data
            new_data = self._fetch_bse_data()
            old_data = json.loads(BSE_DATA.read_text()) if BSE_DATA.exists() else []
            assert isinstance(new_data, list), 'BSE data being downloaded should be list of dicts where each dict contains a single company info'
            all_entries = {e['ISIN_NUMBER']:e for e in old_data}
            # copy over new entries to all_entries
            # this ensures we don't lose any data incase the bse api changes or doesn't return anything
            for e in new_data:
                all_entries[e['ISIN_NUMBER']] = e
            self.bse_data = list(all_entries.values())
            # write to our local store
            BSE_DATA.write_text(json.dumps(self.bse_data))
        else:
            # load the data
            with open(BSE_DATA, 'r') as f:
                self.bse_data = json.load(f)
        self.bse_isin_to_company = {x['ISIN_NUMBER']: x for x in self.bse_data}
        self.ticker_to_isin = {x[TICKER_KEY]:x[ISIN_KEY] for x in self.bse_data}

    def isin_to_ticker(self, isin: str) -> str:
        assert isin in self.bse_isin_to_company, f"ISIN {isin} not found in BSE data"
        return self.sanitize_ticker(self.bse_isin_to_company[isin]['scrip_id'])
    
    def ticker_to_price(self, ticker:str)->float:
        ticker = self.unsanitize_ticker(ticker)
        assert ticker in self.ticker_to_isin
        return self.isin_to_price(self.ticker_to_isin[ticker])
    
    def isin_to_price(self, isin:str)->float:
        assert isin in self.bse_isin_to_company
        scrip_code = self.bse_isin_to_company[isin][SCRIP_CODE_KEY]
        resp = self.session.get(f'https://api.bseindia.com/BseIndiaAPI/api/StockTrading/w?flag=&quotetype=EQ&scripcode={scrip_code}')
        if resp.status_code != 200:
            raise ValueError(f'Failed to Fetch price for ISIN {isin}. Status code: {resp.status_code}')
        return float(resp.json()[PRICE_KEY])
        
    def sanitize_ticker(self, ticker:str)->str:
        """AR&M -> AR-AND-M
        """
        if re.match('^\d', ticker):
            ticker = 'N-'+ticker
        ticker = re.sub(r'&', '-AND-', ticker)
        ticker = re.sub(r' ', '', ticker)
        return ticker

    def unsanitize_ticker(self, ticker:str)->str:
        """AR-AND-M -> AR&M
        """
        for company in self.bse_data:
            if self.sanitize_ticker(company[TICKER_KEY]) == ticker:
                return company[TICKER_KEY]
        raise ValueError(f"Ticker {ticker} not found in BSE data")
    
    def ticker_to_price_source(self, ticker:str)->str:
        """ Eg. RELIANCE -> pricehist.beanprice.yahoo/RELIANCE.BO
        """
        for company in self.bse_data:
            if self.sanitize_ticker(company[TICKER_KEY]) == ticker:
                return 'beancount_importers_india.sources.yahoo_quantized/'+company[TICKER_KEY]+'.BO'
        raise ValueError(f"Ticker {ticker} not found in BSE data")
    
    def export_commodity_declaration(self)->str:
        """
        2000-01-01 commodity RELIANCE
            price: "pricehist.beanprice.yahoo/RELIANCE.BO"
        2000-01-01 commodity TATAMOTORS
            price: "pricehist.beanprice.yahoo/TATAMOTORS.BO"
        ...
        """
        data = []
        for company in self.bse_data:
            data.append(f"2000-01-01 commodity {self.sanitize_ticker(company[TICKER_KEY])}\n    price: \"{self.ticker_to_price_source(self.sanitize_ticker(company[TICKER_KEY]))}\"")
        return '\n'.join(data)
        
    def _fetch_bse_data(self)->list[dict]:
        """Loads the BSE data from the BSE API and returns it as a list of Dict
        The curl command for grabbed from https://www.bseindia.com/corporates/List_Scrips.html

        One dict per company with the following keys
        {'FACE_VALUE': '10.00',
         'GROUP': 'IP',
         'INDUSTRY': '',
         'ISIN_NUMBER': 'INE067R01015',
         'Issuer_Name': 'Adhiraj Distributors Limited',
         'Mktcap': '',
         'NSURL': '',
         'SCRIP_CD': '780018',
         'Scrip_Name': 'Adhiraj Distributors Ltd',
         'Segment': 'Equity',
         'Status': 'Active',
         'scrip_id': 'ADHIRAJ'}

        """
        resp = self.session.get('https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&industry=&segment=Equity&status=Active')
        assert resp.status_code == 200, 'Failed to fetch BSE data.'
        data = resp.json()
        assert isinstance(data, list), 'Returned data is not a list of dict as expected. The api might have changed. try manually'
        return data

