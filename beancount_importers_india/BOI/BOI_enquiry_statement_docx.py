from typing import List
from beancount.core.number import D
from beancount.ingest import importer
from beancount.core import amount
from beancount.core import flags
from beancount.core import data
from beancount.ingest.cache import _FileMemo
import pandas as pd
import docx
from dateutil.parser import parse
import os
import loguru
import docx2txt


def table2df(table):
    data = []

    keys = None
    for i, row in enumerate(table.rows):
        text = (cell.text for cell in row.cells)

        if i == 0:
            keys = tuple(text)
            continue
        row_data = dict(zip(keys, text))
        data.append(row_data)
    return pd.DataFrame(data)

class Importer(importer.ImporterProtocol):
    def __init__(self, account_number, account):
        self.account = account
        self.account_number = account_number

    def file_account(self, file):
        return self.account

    def identify(self, f:_FileMemo):
        # skip non docx files
        if not f.name.endswith('docx'): return False
        # grepping the account number from the file should return 0
        if str(self.account_number) in docx2txt.process(f.name):
            return True

    def extract(self, f:_FileMemo, existing_entries=None):
        doc = docx.Document(f.name)
        # Get all tables
        tables = [table2df(table) for table in doc.tables]
        # Get table with date in headers somewhere
        table = [table for table in tables if ('date' in "".join(table.columns).lower())][0]
        entries:List[data.Transaction] = []

        for index, row in table.iterrows():
            try:
                trans_date = parse(row['Value Date'], dayfirst=True).date()
            except:
                loguru.logger.info(f"Skipping non-date line:\n{row}")
                continue
            trans_desc = row['Narration'].strip()
            is_debit = bool(row['Withdrawl'].strip()) # non-zero string in withdrawl
            trans_amt  = -1*D(row["Withdrawl"].strip().rstrip('Dr')) if is_debit else D(row['Deposit'].strip().strip('Cr'))
            # if not trans_amt in [1,-1]: continue


            meta = data.new_metadata(f.name, index)
            posting_meta = {}
            if trans_desc: posting_meta["transaction_ref"] = trans_desc
            cheque_num = row.get('Chq. No.', "").strip()
            if cheque_num: posting_meta['Cheque_number'] = cheque_num

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
        
        # reverse the entries if they are in assending order
        if entries and entries[0].date > entries[-1].date:
            entries = list(reversed(entries))

        return entries

if __name__ == "__main__":
    import os
    from pprint import pprint

    example_file = os.path.join(os.path.dirname(__file__), "xxxx343_2020-2021.docx")
    filememo = _FileMemo(example_file)

    importer = Importer('xxxx343', 'Assets:Current:BOI')
    if importer.identify(filememo):
        loguru.logger.info("File identification passed")
    else:
        loguru.logger.error(f"File Identification failed: {example_file}")
    
    entries = importer.extract(filememo, None)
    pprint(entries)
    

    


