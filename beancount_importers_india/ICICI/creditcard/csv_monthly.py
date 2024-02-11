from beancount.ingest.importers.csv import Importer as CSVImporter, Col
# from beangulp.importers.csv import Col, CSVImporter #v3 migration
from beancount.core import data
from beancount.core import amount

def transaction_categorizer(txn, row):
    # amount should be positive only for credit to CC
    # last col has CR for credit
    if row[-1] == 'CR':
        p = txn.postings[0]
        new_units = p.units._replace(number = -1*p.units.number)
        txn.postings[0] = p._replace(units=new_units)
    return txn

def Importer(last4:int, account, invert_sign=True):
    return CSVImporter({Col.DATE: 0,
                            Col.REFERENCE_ID:1,
                            Col.NARRATION1: 2,
                            Col.AMOUNT: 5,
                            Col.DRCR:6
                            },
                        account,
                        'INR',
                        'X{8}'+str(last4), # regex that matches inside HSBC CSV statement content
                        skip_lines=8,
                        dateutil_kwds={"dayfirst":True},
                        invert_sign=invert_sign, # creditcard default transactions are debit
                        categorizer=transaction_categorizer,
                        matchers=[('mime', 'text/csv')]
                        # encoding='utf-8-sig'
                        )

if __name__ == "__main__":
    import os
    import logging
    import coloredlogs
    from pprint import pprint
    from beancount.ingest.cache import _FileMemo

    coloredlogs.install("INFO")
    logger = logging.getLogger("HSBC")
    example_file = os.path.join(os.path.dirname(__file__),"statements", "CreditCardStatement_03March2020-03April2020.csv")
    filememo = _FileMemo(example_file)

    importer = Importer(6006, account="Liabilities:INR:ICICI:CreditCard")
    if importer.identify(filememo):
        logger.info("File identification passed")
    else:
        logger.error(f"File Identification failed: {example_file}")

    entries = importer.extract(filememo, None)
    pprint(entries)
