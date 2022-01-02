from beancount.ingest.importers.csv import Importer as CSVImporter, Col
import warnings

def Importer(account="Assets:INR:HSBC:Saving"):
    warnings.warn(f"HSBC csv imported doesn't support identifying files by account number. Avoid using auto identify if you have multiple accounts in HSBC")
    return CSVImporter({Col.DATE: 0,
                            Col.NARRATION1: 1,
                            Col.AMOUNT: 2,
                            Col.BALANCE:3
                            },
                        account,
                        'INR',
                        r' *\d{2}/\d{2}/\d{4}, *TRANSFER', # regex that matches inside HSBC CSV statement content
                        dateutil_kwds={"dayfirst":True},
                        encoding='utf-8-sig'
                        )

if __name__ == "__main__":
    import os
    import logging
    import coloredlogs
    from pprint import pprint
    from beancount.ingest.cache import _FileMemo

    coloredlogs.install("INFO")
    logger = logging.getLogger("HSBC")
    example_file = os.path.join(os.path.dirname(__file__),"statements", "TransactionHistory_HSBC_26_Jan-2020-26_Jul_2020.csv")
    filememo = _FileMemo(example_file)

    importer = Importer()
    if importer.identify(filememo):
        logger.info("File identification passed")
    else:
        logger.error(f"File Identification failed: {example_file}")

    entries = importer.extract(filememo, None)
    pprint(entries)
