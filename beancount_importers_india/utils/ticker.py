from textwrap import dedent
from pathlib import Path
import json
import subprocess
import datetime
import numpy as np


BSE_DATA = Path(__file__).parent.absolute()/'bse.json'


class TickerFetcher:
    
    def __init__(self):
        # We refresh the data if it's older than 2 days
        if not BSE_DATA.exists() or datetime.date.today() - datetime.datetime.fromtimestamp(BSE_DATA.stat().st_mtime).date() > datetime.timedelta(days=2):
            # download the data
            new_data = get_bse_data()
            old_data = json.loads(BSE_DATA.read_text()) if BSE_DATA.exists() else []
            assert (isinstance(new_data, list), 'BSE data being downloaded should be list of dicts where each dict contains a single company info')
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
        self.bse_isin_to_ticker = {x['ISIN_NUMBER']: x['scrip_id'] for x in self.bse_data}

    def isin_to_ticker(self, isin: str) -> str:
        assert isin in self.bse_isin_to_ticker, f"ISIN {isin} not found in BSE data"
        return self.bse_isin_to_ticker[isin]


def get_bse_data()-> list[dict]:
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
