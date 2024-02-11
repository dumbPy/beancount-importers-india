import sys
import numpy as np
from pathlib import Path
from beancount.core.number import D
from beancount.ingest import importer
from beancount.core import amount
from beancount.core import flags
from beancount.core import data
from beancount.ingest.cache import _FileMemo
import pandas as pd
import tabula
from dateutil.parser import parse
import subprocess
import os
import re
import mimetypes
import datetime
from typing import Tuple

class SBIEmailStatementImporter(importer.ImporterProtocol):
    def __init__(self, account_number, name_in_file, password, account="Assets:INR:SBI:Saving"):
        self.account = account
        self.account_number = str(account_number)
        self.password = password
        self.name_in_file = name_in_file

    def file_account(self, file):
        return self.account

    def identify(self, f):
        # skip non pdf files
        if mimetypes.guess_type(f.name)[0] != 'application/pdf': return False
        # grepping the account number from the file should return 0
        lines_to_check = [
                'XXXXXXX'+self.account_number[-4:],
                f'{self.name_in_file}'
                ]
        try:
            all(
            [

                not subprocess.call(
                    f"pdf2txt.py -P {self.password} '{f.name}' | grep '{line}' > /dev/null",
                    shell=True,
                    stderr=open(os.devnull, "w"),
                )
                for line in lines_to_check
            ]
        )
        except:
            return False
    
    def parse_opening_balance(self, f)->data.Balance:
        result = subprocess.run(
            f"pdf2txt.py -M 1000 -L 1000 -P {self.password} '{f.name}' "
            "| awk '/Opening Balance/ {print $5, $6}'",
            shell=True,
            stdout=subprocess.PIPE
        )
        date_and_balance = result.stdout.decode('utf-8')
        date, balance = date_and_balance.split()
        # since balance is as of the end of the day, we subtract a day
        date = parse(date.strip(':')).date() - datetime.timedelta(days=1)
        return data.Balance(
            meta = data.new_metadata(f.name, 0),
            date=date,
            amount = amount.Amount(D(balance), 'INR'),
            account=self.account,
            tolerance=None,
            diff_amount=None,
        )
    def parse_closing_balance(self, f)->data.Balance:
        result = subprocess.run(
            f"pdf2txt.py -M 1000 -L 1000 -P {self.password} '{f.name}' "
            "| awk '/Closing Balance/ {print $5, $6}'",
            shell=True,
            stdout=subprocess.PIPE
        )
        date_and_balance = result.stdout.decode('utf-8')
        date, balance = date_and_balance.split()
        return data.Balance(
            meta = data.new_metadata(f.name, 0),
            date=parse(date.strip(':')).date(),
            amount = amount.Amount(D(balance), 'INR'),
            account=self.account,
            tolerance=None,
            diff_amount=None,
        )

    def extract(self, f, existing_entries=None):
        entries = []
        parse_amount = lambda val: float(val.replace(",","")) if isinstance(val, str) else val
        tables = tabula.read_pdf(f.name, pages='all', lattice=True, password=self.password)
        filtered_tables = []
        column_names = ['Date', 'Transaction Reference', 'Debit', 'Credit', 'Ref.No./Chq.No.','Balance']
        for t in tables:
            if set(column_names).issubset(t.columns):
                filtered_tables.append(t)
        
        try: tab = pd.concat(filtered_tables, ignore_index=True)
        # If topmost description is extracted as 0th table
        except: tab = pd.concat(tables[1:], ignore_index=True)

        for index, row in tab.iterrows():
            if row['Balance']=='null' or type(row['Balance']) is float and np.isnan(row['Balance']): continue
            trans_date = parse(row['Date']).date()
            trans_desc = row['Transaction Reference'].replace("\r"," ")
            is_debit = isinstance(row['Debit'], str) and row['Debit'].strip('-')
            trans_amt  = -1*D(row["Debit"]) if is_debit else D(row['Credit'])
            # if not trans_amt in [1,-1]: continue


            meta = data.new_metadata(f.name, index)
            posting_meta = {}
            ref_col = [c for c in tab.columns if re.search("Ref",c)][-1]
            ref = row[ref_col]
            if isinstance(ref, str): posting_meta["transaction_ref"] = ref.replace('\r',' ')

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
                    'INR'), None, None, None, posting_meta)
            )

            entries.append(txn)

        return entries

if __name__ == "__main__":
    import os
    import logging
    import coloredlogs
    from argparse import ArgumentParser
    from pprint import pprint
    
    parser = ArgumentParser()
    parser.add_argument('--file', required=True)
    parser.add_argument('-A','--account_number', required=True)
    parser.add_argument('-N', '--name_in_file', required=True, help="If the file contains Welcome Mr. XYZ, then use --name_in_file 'Mr. XYZ'")
    parser.add_argument('-P', '--password', required=True)
    args = parser.parse_args()

    coloredlogs.install("INFO")
    logger = logging.getLogger("SBI")
    filememo = _FileMemo(args.file)

    sbi_importer = SBIEmailStatementImporter(args.account_number, args.name_in_file, password=args.password)
    if sbi_importer.identify(filememo):
        logger.info("File identification passed")
    else:
        logger.error(f"File Identification failed: {args.file}")
    
    opening = sbi_importer.parse_opening_balance(filememo)
    logger.info(f"Opening Balance: {opening}")
    logger.info(f"Closing Balance: {sbi_importer.parse_closing_balance(filememo)}")

    entries = sbi_importer.extract(filememo, None)
    pprint(entries)



