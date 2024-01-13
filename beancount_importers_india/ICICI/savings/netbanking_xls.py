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
import xlrd 


logger = logging.getLogger(f'ICICISavingsImporter')


class IciciSavingImporter(importer.ImporterProtocol):
    def __init__(self,accountNumber:int, account:str):
        self.account = account
        self.accountNumber = accountNumber

    def identify(self, file: _FileMemo):
        # skip non pdf files
        if not file.name.lower().endswith('xls'): return False
        # grepping the account number from the file should return 0
        if subprocess.call('which xls2csv', shell=True) == 0:
            command = f'xls2csv {file.name} | grep "{self.accountNumber}" > /dev/null'
        else:
            raise Exception("No xls2csv installed. See README.md")

        if not subprocess.call(command, shell=True):
            return True
        return False

    def file_account(self, file:_FileMemo):
        return self.account

    def file_name(self, file:_FileMemo):
        ws = xlrd.open_workbook(file.name).sheet_by_index(0)
        start = ws.cell_value(4,3)
        end = ws.cell_value(4,5)
        return f"ICICI_Saving_{start.replace('/','-')}_to_{end.replace('/','-')}.xls"

    def extract(self, f, existing_entries=None):
        entries = []
        tab = pd.read_excel(f.name, skiprows=12, skipfooter=29, header=0, usecols=[1,2,3,4,5,6,7,8], index_col=0)
        logger.info(tab.columns)
        for index, row in tab.iterrows():
            assert D(row['Deposit Amount (INR )']) !=0 or D(row["Withdrawal Amount (INR )"]) != 0, "Both Deposit and Withdrawl Amounts are zero" 
            trans_date = parse(row['Transaction Date']).date()
            trans_desc = row['Transaction Remarks']
            # Debit Transactions are positive in the xlsx
            trans_amt  = -1*D(row["Withdrawal Amount (INR )"])
            if trans_amt == 0:
                trans_amt = D(row["Deposit Amount (INR )"])


            meta = data.new_metadata(f.name, index)
            ref = row['Transaction Remarks']
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
    example_file = os.path.dirname(os.path.join(__file__))+'/OpTransactionHistory13-01-2024.xls'
    filememo = _FileMemo(example_file)

    importer = IciciSavingImporter(123456789012, "Assets:INR:ICICI:Saving")
    if importer.identify(filememo):
        logger.info("File identification passed")
        logger.info(importer.file_name(filememo))
        logger.info(importer.extract(filememo, None))
    else:
        logger.error(f"File Identification failed: {example_file}")

    # entries = importer.extract(filememo, None)
    # pprint(entries)
