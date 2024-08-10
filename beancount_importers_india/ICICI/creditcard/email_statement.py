"""
This is an importer for ICICI Credit Card Email Statements. It reads the pdf file and extracts the transactions.
Since the pdf is a mess, we only support extracting transactions from 1st page of the pdf and that too from a specific area,
defined below as `table_areas`, with specific column separators defined as `column_separators`.
"""
import re
from pathlib import Path
from typing import Optional
import pdfplumber
from beancount.core.number import D
from beancount.ingest import importer
from beancount.core import amount
from beancount.core import flags
from beancount.core import data
from beancount.ingest.cache import _FileMemo
import pandas as pd
import camelot
from dateutil.parser import parse
import re
import mimetypes


# Copied from the pdf
DATE = "Date"
NARRATION = "Transaction Details"
AMOUNT = "Amount (in`)"
INTERNATIONAL_AMOUNT = "Intl.# amount"
REWARD_POINTS = "Reward Points"
TRANSACTION_REF = "SerNo."

column_separators = ["250,300,435,470,520"]
table_areas = ["205,475,600,160"]

def row_to_amount(row:pd.Series):
    if row[AMOUNT].strip().lower().endswith("cr"):
        return D(re.sub(r'cr', '', row[AMOUNT], flags=re.IGNORECASE))
    else:
        return -1 * D(row[AMOUNT].lower().replace("Dr", ""))

def make_df_headers(df:pd.DataFrame):
    header_idx = 0
    for i,row in df.iterrows():
        if row[0]=='Date':
            header_idx = i
            break
    column_headers = [re.sub(r'(\r)?\n', ' ', s.strip()) for s in df.iloc[header_idx]]
    df.columns = column_headers
    df = df.drop(index=[i for i in range(header_idx+1)])
    df.reset_index()
    return df

def add_card_name_column(df:pd.DataFrame, card_names:Optional[list[str]]):
    if not card_names: return None
    names = pd.Series(index=df.index, dtype=str)
    # Find card names in the narration column
    names_idx = df[NARRATION].str.match("|".join(card_names))
    # Fill the names in corresponding names column from narration
    names[names_idx] = df[NARRATION]
    # Since names appear above the transactions, we fill the names down
    names = names.ffill()
    df["Card Holder"] = names

def drop_non_date_rows(df:pd.DataFrame):
    def tryDate(string):
        try:
            return parse(string, dayfirst=True, fuzzy=True).date()
        except:
            return ""
    df[DATE] = df[DATE].apply(tryDate)
    return df.drop(df[~((df[DATE]!="") & (df[AMOUNT].str.match(r'\d+([.,]\d+)*')))].index)
    


class ICICICreditCardEmailStatementImporter(importer.ImporterProtocol):
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
            page = pdfplumber.open(f.name, password=self.password).pages[0]
            text = page.extract_text()
            for line in self.lines_to_grep:
                if not line in text:
                    return False
            return True
        except:
            return False
    
    def extract_transactions_table(self, f):
        tables = camelot.read_pdf(
            f.name,
            pages="1",
            flavor="stream",
            row_tol=10,
            password=self.password,
            columns=column_separators,
            table_areas=table_areas
            )
        assert len(tables) == 1, f"Expected 1 table, got {len(tables)}"
        df:pd.DataFrame = tables[0].df
        df = make_df_headers(df)
        df = drop_non_date_rows(df)
        return df


    def extract(self, f, existing_entries=None):
        entries = []
        table = self.extract_transactions_table(f)
        # new_table.to_csv("/tmp/hdfc_tables.csv")
        for index, row in table.iterrows():
            trans_date = row[DATE]
            trans_desc = re.sub(r'(\r)?\n',' ', row[NARRATION])
            if row[INTERNATIONAL_AMOUNT].strip():
                trans_desc += f" :: Intl. Amount: {row[INTERNATIONAL_AMOUNT]}"
            trans_amt = row_to_amount(row)
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


if __name__ == "__main__":
    import os
    import logging
    import coloredlogs
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("account", help="The account name in beancount. eg. Liabilities:CreditCard:ICICI:AmazonPay")
    parser.add_argument("file")
    parser.add_argument(
        "-N",
        "--name_in_file",
        required=True,
        help="A unique string (usually your name) to match in file",
    )
    parser.add_argument("-P", "--password", required=True)
    args = parser.parse_args()

    coloredlogs.install("INFO")
    logger = logging.getLogger("SBI")
    filememo = _FileMemo(args.file)
    importer = ICICICreditCardEmailStatementImporter([args.name_in_file, ], args.password, args.account)
    logger.info(f"Identifying {args.file}")
    if importer.identify(filememo):
        logger.info("File identification passed")
    else:
        logger.error(f"File Identification failed: {args.file}")
    entries = importer.extract(filememo, [])
    for txn in entries:
        logger.info(txn)
