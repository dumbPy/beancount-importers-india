from pathlib import Path
from beancount.core.number import D
from beancount.ingest import importer
from beancount.core import amount
from beancount.core import flags
from beancount.core import data
from beancount.ingest.cache import _FileMemo
import pandas as pd
from dateutil.parser import parse
import re
import mimetypes
from enum import Enum
import re
from dateutil.parser import parse as dateparse
import pdfplumber
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextBoxHorizontal,  LAParams

class Column(Enum):
    DATE = 'DATE'
    MODE = 'MODE'
    PARTICULARS = 'PARTICULARS'
    DEPOSITS = 'DEPOSITS'
    WITHDRAWALS = 'WITHDRAWALS'
    BALANCE = 'BALANCE'

def extract_headers(elements:list, header_line)->dict[Column, dict]:
    """Extract headers from a list of elements that are within the header block"""
    elements = [e for e in elements if header_line['x0'] <= e['x0'] and header_line['x1'] >= e['x1'] and header_line['top'] <= e['top'] and header_line['bottom'] >= e['bottom']]
    return {Column(e['text'].strip()): e for e in elements}

def extract_transactions_from_page(pdf_path:str, password:str, page_number:int=0)->pd.DataFrame:
    # We are looking for a table with the following headers.
    # we first detect the position of these headers
    headers_text = 'DATE MODE PARTICULARS DEPOSITS WITHDRAWALS BALANCE'
    # for page in pdfplumber.open(pdf_path).pages:
    page = pdfplumber.open(pdf_path, password=password).pages[page_number]
    text_lines = page.extract_text_lines()
    header = [line for line in text_lines if line['text'].strip() == headers_text]
    if not header:
        print(f"Skipping page {page.page_number}. No headers found. File: {pdf_path}")
        return pd.DataFrame(columns=[c for c in Column])
    header = header[0]
    footer = [line for line in text_lines if re.match(r'^Total:[ \d,.]+$', line['text'].strip())]
    footer = footer[0] if footer else None
    headers_map = extract_headers(page.extract_words(), header)
    # collect all row separators
    row_separators = [line for line in page.lines if line['x1'] - line['x0'] > 100 and line['y0'] < header['chars'][0]['y0'] and (footer is None or line['y0'] > footer['chars'][0]['y0'])]
    # bottom to top transaction separators
    row_separators.sort(key=lambda x: x['y0'], reverse=True)
    words = page.extract_words()
    # get all text lines between the header and footer
    # text_lines = [e for e in page.layout._objs if isinstance(e, LTTextBoxHorizontal) and e.y0 > header['bottom'] and (footer is None or e.y1 < footer['top'])]
    text_lines = [e for page_ in extract_pages(pdf_path, password=password, laparams=LAParams(line_margin=0), page_numbers=[page_number]) for e in page_]
    text_lines = [e for e in text_lines if isinstance(e, LTTextBoxHorizontal) and e.y1 < header['chars'][0]['y0'] and (footer is None or e.y0 > footer['chars'][0]['y1'])]

    # sort the text lines from bottom to top since pop() will be used to get the lines which is O(1) instead of O(n) for pop(0)
    text_lines.sort(key=lambda x: x.y0, reverse=True)
    rows = [{}]
    while text_lines:
        line = text_lines.pop()
        if row_separators and row_separators[-1]['y0'] < line.y0:
            rows.append({})
            row_separators.pop()
        for column, column_element in headers_map.items():
            if column_element['x0'] <= line.x1 and column_element['x1'] >= line.x0:
                rows[-1][column] = '--'.join([line.get_text().strip(), rows[-1].get(column, '')])
                break
    rows = [row for row in rows if row] # remove empty rows
    rows.reverse() # get top to bottom rows
    for row in rows:
        for k, v in row.items(): # clean up the rows
            row[k] = v.strip('--') if k != Column.DATE else dateparse(v, dayfirst=True, fuzzy=True).date()
    df = pd.DataFrame(rows)
    df = df[[Column.DATE, Column.PARTICULARS, Column.DEPOSITS, Column.WITHDRAWALS, Column.BALANCE]]
    return df

class ICICISavingsEmailImporter(importer.ImporterProtocol):
    def __init__(self, account_number, name_in_file, password, account="Assets:Saving:ICICI"):
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
        page = pdfplumber.open(f.name, password=self.password).pages[0]
        text = page.extract_text()
        num_chars = len(str(self.account_number))
        if not 'X'*(num_chars-4)+self.account_number[-4:] in text: return False
        if not self.name_in_file in text: return False
        return True
    
    def parse_opening_balance(self, f)->data.Balance:
        df = extract_transactions_from_page(f.name, self.password, 0)
        return data.Balance({}, df.iloc[0][Column.DATE], self.account, data.Amount(D(df.iloc[0][Column.BALANCE]), 'INR'), None, None)

    def parse_closing_balance(self, f)->data.Balance:
        num_pages = len(pdfplumber.open(f.name, password=self.password).pages)
        df = pd.concat([extract_transactions_from_page(f.name, self.password, i) for i in range(num_pages)], ignore_index=True)
        return data.Balance({}, df.iloc[-1][Column.DATE], self.account, data.Amount(D(df.iloc[-1][Column.BALANCE]), 'INR'), None, None)
            
    def extract(self, f, existing_entries=None):
        entries = []
        
        num_pages = len(pdfplumber.open(f.name, password=self.password).pages)
        df = pd.concat([extract_transactions_from_page(f.name, self.password, i) for i in range(num_pages)], ignore_index=True)
        df = df[(~df[Column.DEPOSITS].isna()) | (~df[Column.WITHDRAWALS].isna())]
        df.reset_index(drop=True, inplace=True)

        for index, row in df.iterrows():
            trans_date = row[Column.DATE]
            trans_desc = row[Column.PARTICULARS]
            is_debit = isinstance(row[Column.WITHDRAWALS], str) # since the other column is NaN
            trans_amt  = -1*D(row[Column.WITHDRAWALS]) if is_debit else D(row[Column.DEPOSITS])

            meta = data.new_metadata(f.name, index)
            
            posting_meta = {}
            posting_meta['document'] = Path(f.name).name

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
    parser.add_argument('file')
    parser.add_argument('-A','--account_number', required=True)
    parser.add_argument('-N', '--name_in_file', required=True, help="If the file contains Welcome Mr. XYZ, then use -N 'Mr. XYZ'")
    parser.add_argument('-P', '--password', required=True)
    args = parser.parse_args()

    coloredlogs.install("INFO")
    logger = logging.getLogger("ICICISavingsEmailStatementImporter")
    filememo = _FileMemo(args.file)

    icici_importer = ICICISavingsEmailImporter(args.account_number, args.name_in_file, password=args.password)
    if icici_importer.identify(filememo):
        logger.info("File identification passed")
    else:
        logger.error(f"File Identification failed: {args.file}")
    
    opening = icici_importer.parse_opening_balance(filememo)
    logger.info(f"Opening Balance: {opening}")
    logger.info(f"Closing Balance: {icici_importer.parse_closing_balance(filememo)}")

    entries = icici_importer.extract(filememo, None)
    pprint(entries)
