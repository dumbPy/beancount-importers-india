import camelot
import numpy as np
from pathlib import Path
from beancount.core.number import D, Decimal
from beancount.ingest import importer
from beancount.core import amount
from beancount.core import flags
from beancount.core import data
from beancount.ingest.cache import _FileMemo
import pandas as pd
from dateutil.parser import parse
import subprocess
import os
import re
import mimetypes
import datetime
from typing import Tuple
from beancount_importers_india.utils.bse import BSEClient
from logging import getLogger

logger = getLogger('GROWW_contract_note')

DESCRIPTION = "Security/Contractdescription"
COST = 'Net Rateper Unit (Rs)'
BUY_OR_SELL = 'Buy(B)/ Sell(S)'
QUANTITY = 'Quantity'
TRADE_NO = 'Trade no.'
ISIN = 'ISIN' # International Securities Identification Number, We define this column to store the ISIN of the stock extracted from the description
TRADE_DATE = 'Trade Date'
TOTAL = 'Net Total (Before Levies) (Rs)'


class GrowwContractNoteImporter(importer.ImporterProtocol):
    def __init__(self,
        strings_to_match:list[str],
        password,
        wallet='Assets:Investments:Stocks:Groww:Cash',
        holding_account="Assets:Stocks:Groww",
        brokerage_account="Expenses:Investments:Stocks:Groww:Brokerage",
        capital_gains_account="Income:Groww:CapitalGains",
        buyback_account="Assets:Savings"
        ):
        """ Import the trades from the Groww Contract Note
        Downloads the ticker data from the BSE website and uses it to map the ISIN to the ticker
        
        Args:
        wallet: The account where the money is deducted from while buying and added to while selling. Usually groww wallet        
        holding_account: The account where the stocks are held. A placeholder for your Demat account
        brokerage_account: The difference between the net cost and the sum of the individual trades is the brokerage and taxes. Usually an expense account
        capital_gains_account: The account where the capital gains are booked.
        buyback_account: account (usually savings) where buyback amount is credited.
            This is used to balance the contract note if it contains a single buyback transaction.
            For multiple transactions that aren't balanced, they are marked with !Warning flag and need to be fixed manually

        See https://github.com/redstreet/beancount_reds_plugins/tree/main/beancount_reds_plugins/capital_gains_classifier#readme on how to use the capital gains account for tax purposes
        This importer maps the gains to the capital gains account and the above plugin then changes them to STCG and LTCG based on the duration of the holding
        Also make sure you use FIFO booking method for the holding account. see https://beancount.github.io/docs/a_proposal_for_an_improvement_on_inventory_booking.html#implicit-booking-methods
        """
        self.wallet = wallet
        self.holding_account = holding_account
        self.brokerage_account = brokerage_account
        self.strings_to_match = strings_to_match
        self.capital_gains_account = capital_gains_account
        self.buyback_account = buyback_account
        self.password = password
        self.bse_client = BSEClient()
        self.cache = {} # cache tables instead of re-extracting

    def file_account(self, file):
        return self.wallet

    def file_date(self, f):
        tables = self.extract_tables(f)
        date = self.extract_trade_date(tables[0].df) # first table contains the trade date
        return date
    
    def file_name(self, f):
        # strip the date from the file name to support refiling the statements
        existing_name = Path(f.name).name
        if re.match(r'^\d{4}-\d{2}-\d{2}\..*', existing_name):
            return existing_name[11:]
        else:
            return existing_name

    
    def identify(self, f):
        # skip non pdf files
        if mimetypes.guess_type(f.name)[0] != 'application/pdf': return False
        # grepping the account number from the file should return 0
        try:
            return all(
            [
                not subprocess.call(
                    f"pdf2txt.py -P {self.password} '{f.name}' | grep '{line}' > /dev/null",
                    shell=True,
                    stderr=open(os.devnull, "w"),
                )
                for line in self.strings_to_match
            ]
        )
        except:
            return False
    
    def extract_trade_date(self, df)->datetime.date:
        """Extract trade date from first table of the contract note"""
        df = df.T
        df.columns = df.iloc[0]
        df = df.drop(index=[0])
        df = df.reset_index()
        return parse(df[TRADE_DATE][0], dayfirst=True).date()
    
    def clean_df(self, df:pd.DataFrame)->pd.DataFrame:
        # cast all columns to string
        df = df.astype(str)
        # Empty columns for ISIN
        ISIN_col = pd.Series(index=df.index, dtype=str)
        # Find ISIN values index in description column
        ISIN_idx = df[DESCRIPTION].str.match(r'INE\w+')
        # Fill the isin values in corresponding ISIN column
        ISIN_col[ISIN_idx] = df[DESCRIPTION]
        # Since ISIN is below the trades, backfill it to rows above it
        ISIN_col = ISIN_col.bfill()
        # Attach the col to df
        df[ISIN] = ISIN_col
        # Drop non trade rows
        df = df.drop(index=df.index[df[TRADE_NO]==''])
        df.sort_values(by=QUANTITY, inplace=True, ascending=False)
        return df
    
    def set_header(self, df:pd.DataFrame)->pd.DataFrame:
        # Set the first row as the header
        df.columns = [c.replace('\n','').strip() for c in df.iloc[0]]
        df = df.drop(index=[0])
        return df

    def extract_transactions(self, dfs:list[pd.DataFrame])->pd.DataFrame:
        # trades table has 14 columns
        df = pd.concat([self.set_header(df) for df in dfs if df.shape[1]==14], ignore_index=True)
        return self.clean_df(df)
    
    def extract_dp_charges(self, dfs:list[pd.DataFrame])->Decimal | None:
        for df in dfs:
            if df.shape[1]!=6: continue
            df = self.set_header(df)
            if 'Transaction description' in df.columns and 'Total amount' in df.columns:
                return D(str(df.loc[df['Transaction description'].str.contains('DP Charges')]['Total amount'].values[0]))
    
    def extract_equity_net_price_and_brokerage(self, dfs:list[pd.DataFrame])->tuple[Decimal, Decimal]:
        """Extract the net price from the contract note's last table and total brokerage and taxes"""
        for df in dfs:
            if df.shape[1]==4 and set(df.iloc[0]) == set(['Description','Equity','Future & Options', 'Net Total']):
                df = df.T
                df.columns = [c.replace('\n',' ').strip() for c in df.iloc[0]]
                df = df.drop(index=[0])
                df = df.set_index('Description')
                payable = D(str(df.loc['Equity']['Net Amount Receivable / Payable By Client']))
                actual_value = D(str(df.loc['Equity']['Pay In / Pay Out Obligation']))
                # -ve payable means we bought stocks while +ve means we sold stocks
                # either way, the brokerage is always positive
                return payable, abs(payable-actual_value)
        raise ValueError(f"Could not find the table with the net amount of the equities. Total number of tables found: {len(dfs)} and tables with 4 columns {len([t for t in dfs if t.shape[1]==4])}")
    
    def extract_tables(self, f):
        # line scale helps detect small lines in lattice mode. removing it messes up the table detection
        tables = camelot.read_pdf(f.name, pages='all', flavor='lattice', password=self.password, line_scale=50)
        return tables
        
    def extract(self, f, existing_entries=None):
        # TODO: generate commodity directives,and check if existing directives in existing_entries before adding them
        entries = []
        # tables = camelot.read_pdf(f.name, pages='all', flavor='lattice', password=self.password, line_scale=50)
        tables = self.extract_tables(f)

        date = self.extract_trade_date(tables[0].df) # first table contains the trade date
        equity_net_cost, brokerage = self.extract_equity_net_price_and_brokerage([t.df for t in tables]) # last table contains the net cost of the equities
        dp_charges = self.extract_dp_charges([t.df for t in tables])
        df = self.extract_transactions([t.df for t in tables]) # all the tables
        txn_meta = data.new_metadata(f.name, 0)
        txn_meta['document'] = Path(f.name).name

        # Sanity Check
        # cost*quantity = total
        assert (df[COST].map(D)*df[QUANTITY].map(D)).sum() == -df[TOTAL].map(D).sum(), f"Sum of individual trades does not match the total in the contract note. Sum of individual trades: {(df[COST].map(D)*df[QUANTITY].map(D)).sum()} Total: {df[TOTAL].map(D).sum()}"

        # Sum of individual trades and brokerage should match the net cost of the contract note.
        # false could mean that the contract note contains buyback trades.
        # Need to fix these manually if the contract note could contain multiple transactions of which buybacks are unmarked,
        # else the transaction is marked as buyback and amount credited to buyback_account
        is_balanced = (df[COST].apply(D) * df[QUANTITY].apply(D)).sum() + brokerage == -1*equity_net_cost
        
        txn = data.Transaction(
            meta=txn_meta,
            date=date,
            flag=flags.FLAG_OKAY,
            payee=None,
            narration = f'brokerage for {date}',
            tags=set(['groww']),
            links=set(),
            postings=[],
        )
        
        # Difference between the net cost and the sum of the individual trades is the brokerage and taxes
        txn.postings.append(
            data.Posting(self.brokerage_account, amount.Amount(brokerage, 'INR'), None, None, None, None)
        )
        txn.postings.append(
            data.Posting(self.wallet, None, None, None, None, None)
        )
        entries.append(txn)
        
        if any(df[BUY_OR_SELL]=='S') and dp_charges is None:
            logger.warning(f"DP Charges not found in the contract note that contains selling trades: {f.name}")
            
        if dp_charges is not None:
            entries.append(
                data.Transaction(
                    meta=txn_meta.copy(),
                    date=date,
                    flag=flags.FLAG_OKAY,
                    payee=None,
                    narration = f'DP Charges',
                    tags=set(['groww']),
                    links=set(),
                    postings=[
                        data.Posting(self.wallet, amount.Amount(-dp_charges, 'INR'), None, None, None, None),
                        data.Posting(self.brokerage_account, amount.Amount(dp_charges, 'INR'), None, None, None, None),
                    ],
                )
            )

        # We group multiple trades of same cost and buy/sell type together
        for ((isin_number , buy_or_sell), trades_by_stock) in df.groupby([ISIN, BUY_OR_SELL]):
            # sum of all trades of the same stock
            net_price_of_stock = D(str(trades_by_stock[TOTAL].map(Decimal).sum()))
            is_buy = buy_or_sell=='B'
            ticker = self.bse_client.isin_to_ticker(isin_number)
            txn = data.Transaction(
                meta=txn_meta.copy(),
                date=date,
                flag=flags.FLAG_WARNING if (not is_balanced) and df.shape[0]>1 else flags.FLAG_OKAY, # if the contract contains single unbalanced trade, it is a buyback
                payee=None,
                narration = f'{trades_by_stock[QUANTITY].map(int).sum()} {ticker} @ {trades_by_stock[COST].map(Decimal).mean()} == {net_price_of_stock}',
                tags=set(['groww']),
                links=set(),
                postings=[],
            )
            # Total spent on the stock
            if (not is_balanced and df.shape[0]==1):
                cash_account = self.buyback_account
                txn.tags.add('buyback')
            else:
                cash_account = self.wallet
            txn.postings.append(
                data.Posting(cash_account, amount.Amount(net_price_of_stock, 'INR'), None, None, None, None)
            )
            # book capital gains if selling
            if (not is_buy):
                txn.postings.append(
                    data.Posting(f"{self.capital_gains_account}:{ticker}", None, None, None, None, {})
                )
            # now lets add the individual trade lots to the transaction
            for (cost, stock_trades_by_log) in trades_by_stock.groupby(COST):

                # sum of units of the same stock at the same cost
                quantity = D(str(stock_trades_by_log[QUANTITY].map(int).sum()))
                cost = D(str(cost)) # cost for cost basis tracking
                price = D(str(cost)) # price is used for selling
                # posting corresponding to each log at different cost.
                # note that two lots at same price are grouped together above in quantity
                posting = data.Posting(
                    self.holding_account+":"+ticker,
                    amount.Amount(D(str(quantity)), ticker),
                    # Use cost while buying keep cost { } for FIFO ambiguous match while selling
                    cost=data.CostSpec(cost, None, 'INR', date, None, merge=False) if is_buy else data.CostSpec(None, None, None, None, None, None),
                    # Price is used for selling.
                    price=None if is_buy else amount.Amount(price, 'INR'),
                    flag=None,
                    meta={})

                txn.postings.append(posting)
            entries.append(txn)


        return entries
