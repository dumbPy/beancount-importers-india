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


logger = logging.getLogger(f'ICICIImporter')


class Importer(importer.ImporterProtocol):
    def __init__(self,last_four:int, account):
        self.account = account
        self.last_four = last_four # last 4 digits of credit card number

    def identify(self, file: _FileMemo):
        # skip non pdf files
        if not file.name.lower().endswith('xls'): return False
        # grepping the account number from the file should return 0
        if not subprocess.call(f'xls2csv {file.name} | grep -P "Card Number.*{self.last_four}" > /dev/null', shell=True):
            return True
        return False

    def file_account(self, file:_FileMemo):
        return self.account

    def file_name(self, file:_FileMemo):
        p = subprocess.Popen('xls2csv '+str(file.name)+' | grep -P "to \\d{2}\\/\\d{2}\\/\\d{4}"', shell=True, stdout=subprocess.PIPE)
        output = p.communicate()[0]
        start, end = re.findall(r'(\d{2}\/\d{2}\/\d{4}) +to +(\d{2}\/\d{2}\/\d{4})', output.decode())[0]

        return f"ICICI_Statement_{start.replace('/','-')}_to_{end.replace('/','-')}.xls"

    def find_start_and_end_row(self, f):
        t = pd.read_excel(f.name, header=None)
        start = t[t[1]=='Date'].index[0]
        end = 0
        for i,row in t.iloc[start:,:].iterrows():
            if isinstance(row[1], float) and math.isnan(row[1]):
                end = i
                break
        return start, end

    def extract(self, f, existing_entries=None):
        entries = []
        start, end = self.find_start_and_end_row(f)
        tab = pd.read_excel(f.name, skiprows=start, nrows=end-start-1, header=0, usecols=[1,2,3,4,5,6])
        logger.info(tab.columns)

        for index, row in tab.iterrows():
            trans_date = parse(row['Date']).date()
            trans_desc = row['Transaction Details']
            # Debit Transactions are positive in the xlsx
            trans_amt  = -1*D(row["Amount(in ₹)"])


            meta = data.new_metadata(f.name, index)
            ref_col = [c for c in tab.columns if re.search("Ref",c)][0]
            ref = row[ref_col]
            if isinstance(ref, str): meta["transaction_ref"] = ref.replace('\r',' ')

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
    from pprint import pprint

    coloredlogs.install("INFO")
    logger = logging.getLogger("ICICI")
    example_file = os.path.join(os.path.dirname(__file__),"statements", "2020-08-03.ICICI_Statement_01-04-2019_to_31-03-2020.xls")
    filememo = _FileMemo(example_file)

    importer = Importer(6006, "Liabilities:INR:ICICI:CreditCard")
    if importer.identify(filememo):
        logger.info("File identification passed")
    else:
        logger.error(f"File Identification failed: {example_file}")

    # entries = importer.extract(filememo, None)
    # pprint(entries)
