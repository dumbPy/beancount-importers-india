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

class Importer(importer.ImporterProtocol):
    def __init__(self, account_number, account="Assets:INR:SBI:Saving"):
        self.account = account
        self.account_number = account_number

    def file_account(self, file):
        return self.account

    def identify(self, f):
        # skip non pdf files
        if mimetypes.guess_type(f.name)[0] != 'application/pdf': return False
        # grepping the account number from the file should return 0
        return not subprocess.call(
        f'pdf2txt.py "{f.name}" | grep "{self.account_number}" > /dev/null',
        shell=True,
        stderr=open(os.devnull, "w"),
        )
    
    def clean_description(self, desc:str):
        desc = desc.replace("\r"," ")
        if desc.count('-') == 2:
            return desc.split('-')[1]
        return desc

    def extract(self, f, existing_entries=None):
        entries = []
        parse_amount = lambda val: float(val.replace(",","")) if isinstance(val, str) else val
        tables = tabula.read_pdf(f.name, pages='all', lattice=True)
        try: tab = pd.concat(tables, ignore_index=True)
        # If topmost description is extracted as 0th table
        except: tab = pd.concat(tables[1:], ignore_index=True)

        for index, row in tab.iterrows():
            trans_date = parse(row['Txn Date']).date()
            trans_desc = self.clean_description(row['Description'])
            is_debit = isinstance(row['Debit'], str)
            trans_amt  = -1*D(row["Debit"]) if is_debit else D(row['Credit'])
            # if not trans_amt in [1,-1]: continue


            meta = data.new_metadata(f.name, index)
            posting_meta = {}
            ref_col = [c for c in tab.columns if re.search("Ref",c)][0]
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
    from pprint import pprint

    coloredlogs.install("INFO")
    logger = logging.getLogger("SBI")
    example_file = os.path.join(os.path.dirname(__file__), "sample_SBI_multipage.pdf")
    filememo = _FileMemo(example_file)

    sbi_importer = Importer(30789863193)
    if sbi_importer.identify(filememo):
        logger.info("File identification passed")
    else:
        logger.error(f"File Identification failed: {example_file}")

    entries = sbi_importer.extract(filememo, None)
    pprint(entries)



