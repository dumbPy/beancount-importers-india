from pathlib import Path
from beancount.ingest import importer
from beancount.core import flags
from beancount.core import data
from beancount.ingest.cache import _FileMemo
from dateutil.parser import parserinfo, parse as dateparse
import re
import mimetypes


def month_to_number(month):
    for i, m in enumerate(parserinfo.MONTHS):
        if month in m:
            return i + 1


class PhonePeTransactionEmailImporter(importer.ImporterProtocol):
    def __init__(
        self,
        accounts_map_from_emails={
            "XXXXXXXXXX56": "Assets:Savings:Canara"
        },  # Copy the account number exactly from the email and map it to the account in your chart of accounts
        add_payee: bool = True,
    ):
        self.add_payee = add_payee
        self.accounts_map_from_emails = accounts_map_from_emails
        self.account = None

    def extract_transaction_from_html(self, file: _FileMemo) -> data.Transaction:
        payee_regex = re.compile(
            r"(Paid to|Received from)\s*((\w+ {1,2})*)"
        )  # payee can have multiple words separated by max 2 spaces
        narration_regex = re.compile(
            r"(Message\s*:\s*)((\w+ {1,2})*)"
        )  # narration can have multiple words separated by max 2 spaces
        # account_regex = re.compile(r'(Debited from|Credited to) *: *(X\w*)')
        account_regex = re.compile(r"(\w+XXXX+\d+)")
        reference_regex = re.compile(r"(Txn. ID\s*:\s*)(\w+)")
        transaction_status_regex = re.compile(r"(Txn. status\s*:\s*)(\w*)")
        bank_reference_regex = re.compile(r"(Bank Ref. No.\s*: \s*)(\w*)")
        amount_regex = re.compile(r"₹ *(?P<amount>(\d+,)*\d+(.\d{2})?)")
        date_regex = re.compile(r"(?P<month>\w+) (?P<day>\d+), (?P<year>\d{4})")

        # dfs = pd.read_html(file.name)

        payee, narration, account, reference, bank_reference, amount, date = (
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )

        text = Path(file.name).read_text()
        text = re.sub(r"<.*?>", "", text)  # remove html tags
        text = re.sub(r"&#8377;", "₹", text)

        sign = -1 if "Debited from" in text else 1

        if date is None and (match := date_regex.search(text)):
            try:
                month = month_to_number(match.groupdict()["month"])
            except:
                raise ValueError(
                    f'Could not parse the month from the date {match.groupdict()["month"]}'
                )
            date = dateparse(
                f"{match.groupdict()['year']}-{month}-{match.groupdict()['day']}"
            ).date()

        if match := reference_regex.findall(text):
            reference = match[0][1].strip()

        if match := bank_reference_regex.findall(text):
            bank_reference = match[0][1].strip()

        if match := amount_regex.search(text):
            amount = data.Amount(sign * data.D(match.groupdict()["amount"]), "INR")

        if match := account_regex.findall(text):
            account = self.accounts_map_from_emails.get(match[0], None)
            if account is None:
                raise ValueError(
                    f"Account '{match[0]}' not found in the accounts_map_from_emails. available accounts are {self.accounts_map_from_emails.keys()}"
                )

        if match := narration_regex.findall(text):
            narration = match[0][1].strip()

        if match := payee_regex.findall(text):
            payee = match[0][1].strip()

        if match := transaction_status_regex.search(text):
            status = match.group(2)

        assert (
            date and amount and account
        ), f"Could not extract date, amount or account from the file {file.name}. Date: {date}, Amount: {amount}, Account: {account}"
        assert (
            status.lower() == "successful"
        ), f"Transaction status is not success. Status: {status}"

        txn_meta = data.new_metadata(file.name, 0)

        txn = data.Transaction(
            meta=txn_meta,
            date=date,
            flag=flags.FLAG_OKAY,
            payee=payee if self.add_payee else None,
            narration=narration if narration else f"unknown transaction",
            tags=set(["phonepe"]),
            links=set(),
            postings=[
                data.Posting(
                    account=account,
                    units=amount,
                    cost=None,
                    price=None,
                    flag=None,
                    meta={
                        "document": Path(file.name).name,
                        "transaction_ref": bank_reference if bank_reference else '-',
                        "phonepe_txn_id": reference,
                    },
                )
            ],
        )
        return txn

    def file_account(self, file):
        return self.extract_transaction_from_html(file).postings[0].account

    def file_date(self, f):
        return self.extract_transaction_from_html(f).date

    def file_name(self, f):
        # strip the date from the file name to support refiling the statements
        existing_name = Path(f.name).name
        if re.match(r"^\d{4}-\d{2}-\d{2}.*", existing_name):
            return existing_name[11:]
        else:
            return existing_name

    def identify(self, f):
        # skip non pdf files
        if mimetypes.guess_type(f.name)[0] != "text/html":
            return False
        # grepping the account number from the file should return 0
        try:
            return self.extract_transaction_from_html(f)
        except:
            return False

    def extract(self, f, existing_entries=None):
        return [self.extract_transaction_from_html(f)]


if __name__ == "__main__":
    from glob import glob

    paths = glob("/Users/sufiyan/Library/CloudStorage/GoogleDrive-sufi1308@gmail.com/My Drive/Documents/Statements/*.html")
    print(paths)

    importer = PhonePeTransactionEmailImporter(
        accounts_map_from_emails={
            "XXXXXXXXXX56": "Assets:Savings:ICICI",
            "XXXXXXXXXX08156": "Assets:Savings:Canara",
            "XXXXXX9927": "Assets:Savings:HDFC",
        }
    )
    for path in paths:
        print(
            f"Extracted transaction: {importer.extract_transaction_from_html(_FileMemo(path))}"
        )
