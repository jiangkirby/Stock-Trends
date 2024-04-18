import yfinance as yf
import pandas as pd
from tqdm import tqdm
import os

# Read list of S&P500 stocks from file
with open("scrape/snp500_formatted.txt", "r") as file:
    stocks = file.read().splitlines()

# Create an empty DataFrame to store all stock data
all_stock_data = pd.DataFrame(columns=["Date", "Symbol", "Open", "Close", "Low", "High", "Volume"])

# # Iterate over each stock symbol with tqdm to create a progress bar
# for stock_symbol in tqdm(stocks, desc="Fetching Stock Data"):
#     # Fetch historical data for the stock
#     stock_data = yf.download(stock_symbol, start="2017-04-11", end="2024-04-11")
    
#     # Add the stock symbol to the DataFrame
#     stock_data['Symbol'] = stock_symbol
    
#     # Reset index to include date as a column
#     stock_data.reset_index(inplace=True)

#     # Concatenate the stock data to the all_stock_data DataFrame
#     all_stock_data = pd.concat([all_stock_data, stock_data[['Date', 'Open', 'Close', 'Low', 'High', 'Volume', 'Symbol']].dropna()], ignore_index=True)


# Iterate over each stock symbol with tqdm to create a progress bar
stock_symbol = 'AAPL'
# Fetch historical data for the stock
stock_data = yf.download(stock_symbol, start="2017-04-11", end="2024-04-11")

# Add the stock symbol to the DataFrame
stock_data['Symbol'] = stock_symbol

# Reset index to include date as a column
stock_data.reset_index(inplace=True)

# Concatenate the stock data to the all_stock_data DataFrame
all_stock_data = pd.concat([all_stock_data, stock_data[['Date', 'Open', 'Close', 'Low', 'High', 'Volume', 'Symbol']].dropna()], ignore_index=True)


# Navigate to the root directory
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
output_file_path = os.path.join(root_dir,'data/stocks/','AAPL_prices.csv')
# Write all stock data to a CSV file
all_stock_data.to_csv(output_file_path, index=False)
