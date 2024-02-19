# Beancount importer for Groww contract notes.

* The importer imports all the trades of the day under a single account with the right ticker.
* Each ticker is of the form RELIANCE.BO  HDFC.BO , etc. where BO denotes Bombay Stock Exchange.
* Though the contract note denotes the exact exchange the stock was bought from (BSE/NSE), we always use BSE (.BO) for ease of price tracking.

## Transaction
```
2024-02-13 * "Trades from Groww Contract Note on 2024-02-13" #groww
  document: "2024-02-17.CONTRACT_NOTE_123445566.pdf"
  Assets:Wallet:Groww              -4583.38 INR
  Expenses:Brokerage:Groww             8.88 INR
  Assets:Investments:Stocks:Groww         7 INDNIPPON.BO {653.50 INR, 2024-02-13}
```

Note that we conserve the date and cost price of the lot above `{653.50 INR, 2024-02-13}`. This is important for calculating the right Capital Gains.

## Booking Method
You are expected to use [FIFO booking method](https://beancount.github.io/docs/how_inventories_work.html#fifo-and-lifo-booking), i.e., when you sell a stock, the oldest lot you hold is sold first.


## Calculating Capital Gains (and Taxes)
When you sell, the contract note does not mention which lot you sold. In India, the lots are not tracked separately but rather only FIFO method is allowed for booking.
Hence, if you set the FIFO booking method, selling the right stock is as simple as 
```
2024-02-15 * "Trades from Groww Contract Note on 2024-02-15" #groww
  document: "2024-02-19.CONTRACT_NOTE_123445567.pdf"
  Assets:Wallet:Groww                  4895 INR
  Expenses:Brokerage:Groww                5 INR
  Assets:Investments:Stocks:Groww        -7 INDNIPPON.BO {} @ 700 INR
  Income:Stocks:Capital-Gains
```
Meaning you sold 7 units of INDNIPPON at 700 INR per share. The `{ }` means we don't provide the cost criterion for matching which lot to sell, but due to FIFO method, the oldest of the 7 shares of INDNIPPON are matched and booked as sold, and the net profits are booked to `Income:Stocks:Capital-Gains` account.

I personally use the [beancount_reds_plugins/capital_gains_classifier](https://github.com/redstreet/beancount_reds_plugins/tree/main/beancount_reds_plugins/capital_gains_classifier#readme) to further differentiate the capital gains as STCG and LTCG on the fly. When you see the transactions on fava with this plugin activated, it would book the above gains under `Income:Stocks:Capital-Gains:Short` automatically.


