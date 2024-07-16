import sys
from os import path
from typing import Tuple, Union, overload
sys.path.insert(0, path.join(path.dirname(__file__)))

from beancount_importers_india.SBI import Importer as SBIImporter
from beancount_importers_india.BOI.BOI_enquiry_statement_docx import Importer as BOIImporter
from beancount_importers_india.HSBC import Importer as HSBCCSVImporter
from beancount_importers_india.PhonePe.transaction_email import PhonePeTransactionEmailImporter
from beancount_importers_india.HDFC.creditcard import HDFCCreditCardEmailStatementImporter

# from smart_importer import apply_hooks, PredictPayees, PredictPostings

CONFIG = [
        SBIImporter(12345678901, account="Assets:Savings:SBI"), # the account number is used to find the valid pdf
        HSBCCSVImporter("Assets:Savings:HSBC"), # HSBC importer doesn't support account number based filtering, since account number is absent in csv
        BOIImporter(91123456780, "Assets:Savings:BOI"), # Bank of India statement importer
        PhonePeTransactionEmailImporter(accounts_map_from_emails={'XXXX1234':'Assets:Savings:SBI', 'XXXX5678':'Assets:Savings:HSBC'}), # PhonePe transaction email notifications importer
        HDFCCreditCardEmailStatementImporter(lines_to_grep=['Mr. JOHN DOE', 'XXXXXXXXXXXX1234'], password="password", account="Liabilities:CreditCard:HDFC") # HDFC Credit Card statement importer

        ]

# these hooks apply smart_importer's ML models to the imported transactions
# and try to predict the payee and missing postings
#for importer in CONFIG:
    #apply_hooks(importer, [PredictPostings(), PredictPayees()])

# NOTE: smart_importer does not work with beancount-importer project

