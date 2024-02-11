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


# Copied from the pdf
DATE = "Date"
NARRATION = "Transaction Description"
AMOUNT = "Amount (in Rs.)"

def row_to_amount(row):
    if row[AMOUNT].strip().endswith("Cr"):
        return D(row[AMOUNT].replace("Cr", "").strip())
    else:
        return -1 * D(row[AMOUNT].replace("Dr", "").strip())

def isempty(value):
    if isinstance(value, float) and np.isnan(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False
    
def make_df_headers(df:pd.DataFrame):
    df = df.replace('', np.nan)
    df.columns = df.iloc[0]
    df = df.drop(index=[0])
    df.reset_index()
    return df

def drop_non_date_rows(df:pd.DataFrame):
    return df.dropna(subset=[DATE])
    


class HDFCCreditCardEmailStatementImporter(importer.ImporterProtocol):
    def __init__(self, lines_to_grep:list[str], password:str, account:str):
        self.account = account
        self.password = password
        assert len(lines_to_grep) > 0, "At least one line to grep is required for identification of the file"
        self.lines_to_grep = lines_to_grep

    def file_account(self, file):
        return self.account

    def identify(self, f):
        # skip non pdf files
        if mimetypes.guess_type(f.name)[0] != "application/pdf":
            return False
        # grepping the account number from the file should return 0
        try:
            return all(
                [
                    not subprocess.call(
                        f"pdf2txt.py -P {self.password} '{f.name}' | grep '{line}' > /dev/null",
                        shell=True,
                        stderr=open(os.devnull, "w"),
                    )
                    for line in self.lines_to_grep
                ]
            )
        except:
            return False

    def parse_opening_balance(self, f) -> data.Balance:
        result = subprocess.run(
            f"pdf2txt.py -M 1000 -L 1000 -P {self.password} '{f.name}' "
            "| awk '/Opening Balance/ {print $5, $6}'",
            shell=True,
            stdout=subprocess.PIPE,
        )
        date_and_balance = result.stdout.decode("utf-8")
        date, balance = date_and_balance.split()
        # since balance is as of the end of the day, we subtract a day
        date = parse(date.strip(":")).date() - datetime.timedelta(days=1)
        return data.Balance(
            meta=data.new_metadata(f.name, 0),
            date=date,
            amount=amount.Amount(D(balance), "INR"),
            account=self.account,
            tolerance=None,
            diff_amount=None,
        )

    def parse_closing_balance(self, f) -> data.Balance:
        result = subprocess.run(
            f"pdf2txt.py -M 1000 -L 1000 -P {self.password} '{f.name}' "
            "| awk '/Closing Balance/ {print $5, $6}'",
            shell=True,
            stdout=subprocess.PIPE,
        )
        date_and_balance = result.stdout.decode("utf-8")
        date, balance = date_and_balance.split()
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
        tables = camelot.read_pdf(
            f.name,
            pages="all",
            flavor="stream",
            row_tol=10,
            password=self.password,
        )
        filtered_tables = []
        for t in tables:
            # set 0th row as header since camelot doesn't do it automatically
            t = make_df_headers(t.df.copy())
            if set([DATE, AMOUNT, NARRATION]).issubset(t.columns):
                filtered_tables.append(t)
            else:
                print(t.columns, [DATE, AMOUNT, NARRATION])

        tab = pd.concat(filtered_tables, ignore_index=True)

        new_table = drop_non_date_rows(tab)
        # new_table.to_csv("/tmp/hdfc_tables.csv")
        for index, row in new_table.iterrows():
            trans_date = parse(row[DATE], dayfirst=True).date()
            trans_desc = row[NARRATION].replace("\r", " ")
            trans_amt = row_to_amount(row)
            # if not trans_amt in [1,-1]: continue

            meta = data.new_metadata(f.name, index)
            posting_meta = {}

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


if __name__ == "__main__":
    import os
    import logging
    import coloredlogs
    from argparse import ArgumentParser
    from pprint import pprint

    parser = ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("-A", "--account_number", required=True)
    parser.add_argument(
        "-N",
        "--name_in_file",
        required=True,
        help="A unique string (usually your name) to match in file",
    )
    parser.add_argument("-P", "--password", required=True)
    args = parser.parse_args(
        [
            "--file",
            "~/Downloads/foo_bar_statement.pdf",
            "-A",
            "50100489269927",
            "-N",
            "Mr X",
            "-P",
            "1234567",
        ]
    )

    coloredlogs.install("INFO")
    logger = logging.getLogger("SBI")
    filememo = _FileMemo(args.file)
