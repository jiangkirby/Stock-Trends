import yfinance as yf
import pandas as pd
import requests
from tqdm import tqdm

# Fetch list of S&P500 stocks 
sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
response = requests.get(sp500_url)
df = pd.read_html(response.text)[0]
stocks = df['Symbol'].tolist()

# Create an empty DataFrame to store all stock data
all_stock_data = pd.DataFrame(columns=["Date", "Symbol", "Open", "Close", "Low", "High", "Volume"])

# Iterate over each stock symbol with tqdm to create a progress bar
for stock_symbol in tqdm(stocks, desc="Fetching Stock Data"):
    # Fetch historical data for the stock
    stock_data = yf.download(stock_symbol, start="2017-04-11", end="2024-04-11")
    
    # Add the stock symbol to the DataFrame
    stock_data['Symbol'] = stock_symbol
    
    # Reset index to include date as a column
    stock_data.reset_index(inplace=True)

    # Concatenate the stock data to the all_stock_data DataFrame
    all_stock_data = pd.concat([all_stock_data, stock_data[['Date', 'Open', 'Close', 'Low', 'High', 'Volume', 'Symbol']].dropna()], ignore_index=True)

# Write all stock data to a CSV file
all_stock_data.to_csv('all_stock_data.csv', index=False)
