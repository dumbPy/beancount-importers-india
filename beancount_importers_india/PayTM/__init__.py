from beancount.core.number import D
from beancount.ingest import importer
from beancount.core import amount
from beancount.core import flags
from beancount.core import data
from beancount.ingest.cache import _FileMemo
import pandas as pd
import camelot
from dateutil.parser import parse
import subprocess
import os
import re
import mimetypes

class Importer(importer.ImporterProtocol):
    def __init__(self, account_number, account="Assets:INR:PAYTM:Saving"):
        self.account = account
        self.account_number = account_number

    def file_account(self, file):
        return self.account

    def identify(self, f):
        # skip non pdf files
        if mimetypes.guess_type(f.name)[0] != 'application/pdf': return False
        # grepping the account number from the file should return 0
        return not all((subprocess.call(
        f'ps2txt {f.name} | grep -P "Account statement for: \\d+ \\w+ \\d{4} to \\d+ \\w+ \\d{4}" > /dev/null',
        shell=True),
                        subprocess.call(
        f'ps2txt {f.name} | grep -P "{self.account_number} \\s+ SAVING" > /dev/null', shell=True
                        )))

    def extract(self, f:_FileMemo, existing_entries=None):
        tables = camelot.read_pdf(f.name, flavor='stream', pages='all')
        tables = [t.df for t in tables if len(t.df.columns) == 4]
        header = list(tables[0].iloc[0,:])
        table = pd.concat([t.drop(index=0) for t in tables], ignore_index=True)
        table.columns = header

        def make_transaction(date_and_time, desc_tuple, trans_amt, is_debit, line_no):
            trans_amt  = -1*D(trans_amt) if is_debit else D(trans_amt)
            meta = data.new_metadata(f.name, line_no)
            posting_meta = {"transaction_ref":"\n".join(desc_tuple[2:])}
            txn = data.Transaction(
                meta=meta,
                date=date_and_time.date(),
                flag=flags.FLAG_OKAY,
                payee=None,
                narration = " ".join(desc_tuple[:2]),
                tags=set(),
                links=set(),
                postings=[],
            )
            txn.postings.append(
                data.Posting(self.account, amount.Amount(trans_amt,
                    'INR'), None, None, None, posting_meta)
            )
            return txn
    
    
        def table2entries(df):
            DATE = [k for k in table.columns if 'date' in k.lower()][0]
            AMOUNT = 'AMOUNT'
            DETAILS = [k for k in table.columns if 'transaction' in k.lower()][0]
            transaction = []
            entries = []
            date = []
            for i, row in table.iterrows():
                if not ":" in row[DATE].strip() and len(row[DATE].strip())>2:
                    # if previous transaction collected, file it
                    if date: entries.append(make_transaction(date, transaction, amount, is_debit, i))
                    date = [row[DATE].strip()]
                    is_debit = row[AMOUNT].strip().startswith('-')
                    amount = row[AMOUNT].strip('+- ₹')
                if re.match(r'\d\d?:\d{2} +(AM|PM)', row[DATE].strip()):
                    date.append(row[DATE].strip())
                    date = parse(" ".join(date))
                if row[DETAILS]:
                    transaction.append(row[DETAILS])
            entries.append(make_transaction(date, transaction, amount, is_debit, i))
            return entries 
        return table2entries(table)

if __name__ == "__main__":
    import os
    import logging
    import coloredlogs
    from pprint import pprint

    coloredlogs.install("INFO")
    logger = logging.getLogger("SBI")
    example_file = os.path.join(os.path.dirname(__file__),"statements", "Account_Statement_Jan2020_to_July_2020.pdf")
    filememo = _FileMemo(example_file)

    importer = Importer(918686474225)
    if importer.identify(filememo):
        logger.info("File identification passed")
    else:
        logger.error(f"File Identification failed: {example_file}")
    
    entries = importer.extract(filememo, None)
    pprint(entries)
    

    