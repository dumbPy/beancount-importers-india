from textwrap import dedent
from pathlib import Path
import json
import subprocess
import datetime
import numpy as np


BSE_DATA = Path(__file__).parent.absolute()/'bse.json'


class TickerFetcher:
    
    def __init__(self):
        if not BSE_DATA.exists() or datetime.date.today() - datetime.datetime.fromtimestamp(BSE_DATA.stat().st_mtime).date() > datetime.timedelta(days=7):
            # download the data and create the embeddings
            self.bse_data = get_bse_data()
            with open(BSE_DATA, 'w') as f:
                json.dump(self.bse_data, f)

        else:
            # load the data
            with open(BSE_DATA, 'r') as f:
                self.bse_data = json.load(f)
        self.bse_isin_to_ticker = {x['ISIN_NUMBER']: x['scrip_id'] for x in self.bse_data}

    def isin_to_ticker(self, isin: str) -> str:
        assert isin in self.bse_isin_to_ticker, f"ISIN {isin} not found in BSE data"
        return self.bse_isin_to_ticker[isin]


def get_bse_data()-> list[dict]:
    """Loads the BSE data from the BSE API and returns it as a list of Dict

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
    # use subprocess to run a curl command to get the json response and parse
    resp = subprocess.run(dedent("""
        curl 'https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&industry=&segment=Equity&status=Active' \
          -H 'authority: api.bseindia.com' \
          -H 'accept: application/json, text/plain, */*' \
          -H 'accept-language: en-US,en;q=0.9,hi;q=0.8,mr;q=0.7' \
          -H 'dnt: 1' \
          -H 'origin: https://www.bseindia.com' \
          -H 'referer: https://www.bseindia.com/' \
          -H 'sec-ch-ua: "Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"' \
          -H 'sec-ch-ua-mobile: ?0' \
          -H 'sec-ch-ua-platform: "macOS"' \
          -H 'sec-fetch-dest: empty' \
          -H 'sec-fetch-mode: cors' \
          -H 'sec-fetch-site: same-site' \
          -H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' \
          --compressed
  """), shell=True, capture_output=True)
    return json.loads(resp.stdout)
