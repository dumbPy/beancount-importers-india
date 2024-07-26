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
    PARTICULARS = 'TRANSACTION DETAILS'
    AMOUNT = 'AMOUNT (Rs.)'
    CASHBACK= 'CASHBACK EARNED' # optional. available in Flipkart Axis Bank Credit Card statement but not in Axis Neo

def is_debit(text:str)->bool:
    if 'Dr' in text: return True
    if 'Cr' in text: return False
    raise ValueError(f'Cannot determine if the transaction is a debit or credit: {text}')

def extract_headers(elements:list, header_line)->dict[Column, dict]:
    """Extract headers from a list of elements that are within the header block"""
    elements = [e for e in elements if header_line['chars'][0]['y0'] >= e.y0 and header_line['chars'][0]['y1'] <= e.y1]
    headers = {}
    for e in elements:
        try:
            column = Column(e.get_text().strip())
            headers[column] = e
        except:
            pass
    return headers

def extract_transactions_from_page(pdf_path:str, password:str, page_number:int=0)->pd.DataFrame:
    # We are looking for a table with the following headers.
    # we first detect the position of these headers
    header_pattern = r'MERCHANT CATEGORY'
    footer_pattern = r'End of Statement'
    # for page in pdfplumber.open(pdf_path).pages:
    page = pdfplumber.open(pdf_path, password=password).pages[page_number]
    text_lines = page.extract_text_lines()
    header = [line for line in text_lines if re.search(header_pattern, line['text'].strip())]
    if not header:
        print(f"Skipping page {page.page_number}. No headers found. File: {pdf_path}")
        return pd.DataFrame(columns=[c for c in Column])
    header = header[0]
    footer = [line for line in text_lines if re.search(footer_pattern, line['text'].strip())]
    footer = footer[0] if footer else None
    
    if not (header and footer):
        return pd.DataFrame(columns=['DATE'])
    
    # get all text lines between the header and footer
    text_lines = [e for page_ in extract_pages(pdf_path, password=password, laparams=LAParams(line_margin=0), page_numbers=[page_number]) for e in page_ if isinstance(e, LTTextBoxHorizontal)]
    headers_map = extract_headers(text_lines, header)
    
    text_lines = [e for e in text_lines if e.y1 < header['chars'][0]['y0'] and (footer is None or e.y0 > footer['chars'][0]['y1'])]

    # sort so we start top to bottom and left to right
    text_lines.sort(key=lambda x: (x.y0, -x.x0), reverse=True)
    rows = []
    for line in text_lines:
        # date marks the beginning of a new row
        if re.search('^\d{2}[/-]\d{2}[/-]\d{4}$', line.get_text().strip()):
            rows.append({})
        # skip text before the first date
        if not rows:
            continue
        for column, column_element in headers_map.items():
            # if line overlaps with the column header, add it to the row under this column
            if column_element.x1 >= line.x0 and line.x1 >= column_element.x0:
                rows[-1][column] = '--'.join([line.get_text().strip(), rows[-1].get(column, '')])
                break
    rows = [row for row in rows if row] # remove empty rows
    for row in rows:
        for k, v in row.items(): # clean up the rows
            row[k] = v.strip('--') if k != Column.DATE else dateparse(v, dayfirst=True, fuzzy=True).date()
    df = pd.DataFrame(rows)
    
    # return df in consistent order
    cols = [Column.DATE, Column.PARTICULARS, Column.AMOUNT]
    if Column.CASHBACK in df.columns:
        cols.append(Column.CASHBACK)
    return df[cols]

class AxisCreditCardEmailImporter(importer.ImporterProtocol):
    def __init__(self, name_in_file:str, password:str, last_4:int, account="Liabilities:CreditCard:Axis", cashback_account:str|None=None):
        self.account = account
        self.last_4 = last_4
        self.password = password
        self.name_in_file = name_in_file
        self.cashback_account = cashback_account

    def file_account(self, file):
        return self.account

    def identify(self, f):
        # skip non pdf files
        if mimetypes.guess_type(f.name)[0] != 'application/pdf': return False
        # grepping the account number from the file should return 0
        try:
            page = pdfplumber.open(f.name, password=self.password).pages[0]
            text = page.extract_text()
            if not f'******{self.last_4}' in text: return False
            if not self.name_in_file in text: return False
            return True
        except:
            return False
    
    def extract(self, f, existing_entries=None):
        entries = []
        
        num_pages = len(pdfplumber.open(f.name, password=self.password).pages)
        df = pd.concat([extract_transactions_from_page(f.name, self.password, i) for i in range(num_pages)], ignore_index=True)

        for index, row in df.iterrows():
            trans_date = row[Column.DATE]
            trans_desc = row[Column.PARTICULARS]
            is_debit_ = is_debit(row[Column.AMOUNT])
            trans_amt  = (-1 if is_debit_ else 1)*D(row[Column.AMOUNT].rstrip('CcDdr'))
            cashback_amt  = (1 if is_debit_ else -1)*D(row[Column.CASHBACK].rstrip('CcDdr')) if Column.CASHBACK in df.columns else None

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
            if cashback_amt is not None and cashback_amt != 0:
                txn.postings.append(
                    data.Posting(self.cashback_account, amount.Amount(cashback_amt,
                        'INR'), None, None, None, {})
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
    parser.add_argument('-n','--last4', required=True, type=int, help="Last 4 digits of your credit card number")
    parser.add_argument('-N', '--name_in_file', required=True, help="Copy your name from the statement")
    parser.add_argument('-P', '--password', required=True)
    parser.add_argument('-a', '--account', default="Liabilities:Axis:CreditCard")
    args = parser.parse_args()

    coloredlogs.install("INFO")
    logger = logging.getLogger("AxisSavingsEmailStatementImporter")
    filememo = _FileMemo(args.file)

    icici_importer = AxisCreditCardEmailImporter(args.name_in_file, args.password, args.last4, args.account)
    if icici_importer.identify(filememo):
        logger.info("File identification passed")
    else:
        logger.error(f"File Identification failed: {args.file}")
    
    entries = icici_importer.extract(filememo, None)
    pprint(entries)
