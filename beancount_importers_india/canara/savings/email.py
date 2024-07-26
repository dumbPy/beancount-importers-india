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
import camelot
from dateutil.parser import parse
import subprocess
import os
import re
import mimetypes
import datetime
from typing import Tuple

# headers from the canara bank email statement
DATE = "Txn Date"
NARRATION = "Txn Description"
DEBIT = "Debit"
CREDIT = "Credit"
BALANCE = "Balance"


class CanaraSavingsEmailStatementImporter(importer.ImporterProtocol):
    def __init__(self, account_number, name_in_file, password, account):
        self.account = account
        self.account_number = account_number
        self.password = password
        self.name_in_file = name_in_file

    def file_account(self, file):
        return self.account

    def identify(self, f):
        # skip non pdf files
        if mimetypes.guess_type(f.name)[0] != "application/pdf":
            return False
        # grepping the account number from the file should return 0
        lines_to_check = [self.account_number, self.name_in_file]
        try:
            return all(
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

    def load_df(self, f):
        tables = camelot.read_pdf(f.name, pages="all", password=self.password)
        dfs = [table.df.copy() for table in tables]
        headers = list(dfs[0].iloc[0])
        # camelot returns the first row as the header, so we need to drop it after copying
        # but only in first page df as the header is not repeated in subsequent pages
        dfs[0].drop(index=0, inplace=True)
        df = pd.concat(dfs, ignore_index=True)
        df.columns = headers
        df = df[df["Txn Date"] != ""]
        df.reset_index(drop=True, inplace=True)
        return df

    def parse_opening_balance(self, f) -> data.Balance:
        df = self.load_df(f)
        date = parse(df.iloc[0][DATE]).date()
        balance = str(df.iloc[0][BALANCE])
        return data.Balance(
            meta=data.new_metadata(f.name, 0),
            date=date,
            amount=amount.Amount(D(balance), "INR"),
            account=self.account,
            tolerance=None,
            diff_amount=None,
        )

    def parse_closing_balance(self, f) -> data.Balance:
        df = self.load_df(f)
        date = parse(df.iloc[-1][DATE]).date()
        balance = str(df.iloc[-1][BALANCE])
        return data.Balance(
            meta=data.new_metadata(f.name, 0),
            date=parse(date.strip(":")).date(),
            amount=amount.Amount(D(balance), "INR"),
            account=self.account,
            tolerance=None,
            diff_amount=None,
        )

    def extract(self, f, existing_entries=None):
        entries = []
        parse_amount = lambda val: (
            float(val.replace(",", "")) if isinstance(val, str) else val
        )
        df = self.load_df(f)
        for index, row in df.iterrows():
            assert (
                row[DEBIT] != "" or row[CREDIT] != ""
            ), "Both Debit and Credit Amounts are empty in row {} of statement {}".format(
                index, f.name
            )
            trans_date = parse(row[DATE]).date()
            trans_desc = re.sub(r'(\r|\n|\r\n)', ' ', row[NARRATION]) 
            is_debit = (
                isinstance(row[DEBIT], str)
                and row[DEBIT].strip() != ""
                and row[DEBIT].strip() != "0.00"
            )
            trans_amt = -1 * D(row[DEBIT]) if is_debit else D(row[CREDIT])
            # if not trans_amt in [1,-1]: continue

            meta = data.new_metadata(f.name, index)
            posting_meta = {'document': Path(f.name).name}

            txn = data.Transaction(
                meta=meta,
                date=trans_date,
                flag=flags.FLAG_OKAY,
                payee=None,
                narration=trans_desc,
                tags=set(),
                links=set(),
                postings=[],
            )

            txn.postings.append(
                data.Posting(
                    self.account,
                    amount.Amount(trans_amt, "INR"),
                    None,
                    None,
                    None,
                    posting_meta,
                )
            )

            entries.append(txn)

        return entries
