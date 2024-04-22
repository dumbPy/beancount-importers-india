from beancount.core.number import D
from beancount.ingest import importer
from beancount.core import account
from beancount.core import amount
from beancount.core import flags
from beancount.core import data
from beancount.core.position import Cost
import pandas as pd
from beancount.ingest.cache import _FileMemo
import math
from dateutil.parser import parse
import subprocess
import os
import re
import logging
from pathlib import Path


logger = logging.getLogger(f'ICICIImporter')

DATE = "DATE"
DESCRIPTION = "PARTICULARS"
CREDIT = "DEPOSITS"
DEBIT = "WITHDRAWALS"
BALANCE = "BALANCE"

class Importer(importer.ImporterProtocol):
    def __init__(self,account_number:str, account):
        self.account = account
        self.account_number = str(account_number)

    def identify(self, file: _FileMemo):
        # skip non pdf files
        path = Path(file.name)
        if not path.name.lower().endswith('csv'): return False
        # grepping the account number from the file should return 0
        return "XXXXXXXX0056" in path.read_text()

    def file_account(self, file:_FileMemo):
        return self.account

    def file_name(self, file:_FileMemo):
        text = Path(file.name).read_text()
        date_range_string = re.findall(r'(?<=for the period) -?(.*)', text)[0]
        return f"ICICI_Savings_Statement_{date_range_string}.csv"

    def find_start_and_end_row(self, f):
        lines = Path(f.name).read_text().splitlines()
        start, end = 0, 0
        for i, row in enumerate(lines):
            if row.startswith("DATE,"):
                start = i
            if re.match(r"\d{2}-\d{2}-\d{4}", row):
                end = i
        return start, end

    def extract(self, f, existing_entries=None):
        entries = []
        start, end = self.find_start_and_end_row(f)
        tab = pd.read_csv(f.name, skiprows=start, nrows=end-start, header=0, usecols=[0,1,2,3,4,5])
        logger.info(tab.columns)

        for index, row in tab.iterrows():
            trans_date = parse(row[DATE], dayfirst=True).date()
            trans_desc = row[DESCRIPTION]
            # Debit Transactions are positive in the xlsx
            trans_amt  = D(row[CREDIT]) - D(row[DEBIT])


            meta = data.new_metadata(f.name, index)
            meta['document'] = Path(f.name).name

            txn = data.Transaction(
                meta=meta,
                date=trans_date,
                flag=flags.FLAG_OKAY,
                payee=None,
                narration = trans_desc,
                tags=set(),
                links=set(),
                postings=[],
            )

            txn.postings.append(
                data.Posting(self.account, amount.Amount(trans_amt,
                    'INR'), None, None, None, None)
            )

            entries.append(txn)
        data

        return entries

if __name__ == "__main__":
    import os
    import logging
    import coloredlogs
    from argparse import ArgumentParser
    from pprint import pprint
    
    parser = ArgumentParser()
    parser.add_argument("file", help="The file to be imported from")
    args = parser.parse_args()

    coloredlogs.install("INFO")
    logger = logging.getLogger("ICICI")
    filememo = _FileMemo(args.file)

    importer = Importer("456776530056", "Assets:ICICI:Savings")
    if importer.identify(filememo):
        logger.info("File identification passed")
    else:
        logger.error(f"File Identification failed: {args.file}")
    pprint(importer.extract(filememo, None))