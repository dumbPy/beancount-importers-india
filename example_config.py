import sys
from os import path
from typing import Tuple, Union, overload
sys.path.insert(0, path.join(path.dirname(__file__)))

from beancount_importers_india.SBI import Importer as SBIImporter
from beancount_importers_india.HSBC import Importer as HSBCImporter
from beancount_importers_india.PayTM import Importer as PayTMImporter
from beancount_importers_india.BOI.BOI_enquiry_statement_docx import Importer as BOIImporter
from beancount_importers_india.ICICI.csv_monthly import Importer as ICICICSVImporter
from beancount_importers_india.ICICI.excel_yearly import Importer as ICICIFYImporter

# from smart_importer import apply_hooks, PredictPayees, PredictPostings

CONFIG = [
        SBIImporter(30789863193, account="Assets:Saving:SBI"), # the account number is used to find the valid pdf
        ICICICSVImporter(last4=6006, "Liability:CreditCard:ICICI"), # ICICI creditcard monthly csv importer
        ICICIFYImporter(6006, "Liability:CreditCard:ICICI"), # ICICI creditcard yearly excel importer
        HSBCImporter("Assets:Saving:HSBC"), # HSBC importer doesn't support account number based filtering, since account number is absent in csv
        PayTMImporter(account_number=91123456780, "Assets:Saving:PayTM"), # Paytm bank importer
        BOIImporter(91123456780, "Assets:Saving:BOI"), # Bank of India statement importer

        ]

# these hooks apply smart_importer's ML models to the imported transactions
# and try to predict the payee and missing postings
#for importer in CONFIG:
    #apply_hooks(importer, [PredictPostings(), PredictPayees()])

# NOTE: smart_importer does not work with beancount-importer project

