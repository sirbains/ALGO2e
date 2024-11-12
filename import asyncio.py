import asyncio
import aiohttp
import logging
from datetime import datetime

# API Constants and Configuration
API_KEY = {'X-API-key': 'MW0YJ28H'}
BASE_URL = 'http://localhost:9999/v1/'
MAX_POSITION_LIMIT = 25000
ORDER_SIZE = 2000  # Base order size for each trade
POSITION_BUFFER = 0.9  # Stop trading at 90% of max position

# Fee and Rebate Structure
FEE_REBATE_STRUCTURE = {
    'CNR': {'fee': 0.0027, 'rebate': 0.0023},
    'RY': {'fee': -0.0020, 'rebate': -0.0014},
    'AC': {'fee': 0.0015, 'rebate': 0.0011}
}

# Base Spread Threshold for Each Security (can be dynamically adjusted)
SPREAD_THRESHOLDS = {
    'CNR': 0.03,
    'RY': 0.025,
    'AC': 0.04
}

# Initialize Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Dynamic spread calculation based on volatility and market conditions
def calculate_dynamic_spread(ticker, bid_price, ask_price):
    base_spread = SPREAD_THRESHOLDS[ticker]
    # Adjust spread based on real-time market conditions
    market_spread = ask_price - bid_price
    return max(base_spread, market_spread)

# Check if trade is profitable based on fees, rebates, and spread
def is_trade_profitable(ticker, spread, bid_size, ask_size):
    fees = (bid_size + ask_size) * FEE_REBATE_STRUCTURE[ticker]['fee']
    rebates = (bid_size + ask_size) * FEE_REBATE_STRUCTURE[ticker]['rebate']
    return spread >= (fees - rebates + SPREAD_THRESHOLDS[ticker])

# Fetch data from RIT API
async def fetch_data(session, endpoint, params=None):
    async with session.get(BASE_URL + endpoint, headers=API_KEY, params=params) as response:
        return await response.json()

# Get market data for a specific ticker
async def get_market_data(session, ticker):
    return await fetch_data(session, 'securities/book', {'ticker': ticker})

# Get the current position for a ticker
async def get_position(session, ticker):
    data = await fetch_data(session, 'securities', {'ticker': ticker})
    return data[0]['position']

# Fetch current tick
async def get_tick(session):
    case_data = await fetch_data(session, 'case')
    return case_data['tick']

# Place limit orders for both buy and sell with optimal size and price
async def place_orders(session, ticker, bid_price, ask_price, bid_size, ask_size, position):
    over_limit = abs(position) + max(bid_size, ask_size) > MAX_POSITION_LIMIT * POSITION_BUFFER
    spread = ask_price - bid_price
    
    if is_trade_profitable(ticker, spread, bid_size, ask_size):
        if position < MAX_POSITION_LIMIT * POSITION_BUFFER:
            bid_payload = {
                'ticker': ticker, 'type': 'LIMIT', 'quantity': bid_size,
                'price': bid_price, 'action': 'BUY'
            }
            bid_response = await session.post(BASE_URL + 'orders', params=bid_payload, headers=API_KEY)
            if bid_response.status == 200:
                logging.info(f"Placed bid for {ticker} at {bid_price} for {bid_size} shares.")
            else:
                logging.error(f"Failed to place bid: {await bid_response.text()}")

        if position > -MAX_POSITION_LIMIT * POSITION_BUFFER:
            ask_payload = {
                'ticker': ticker, 'type': 'LIMIT', 'quantity': ask_size,
                'price': ask_price, 'action': 'SELL'
            }
            ask_response = await session.post(BASE_URL + 'orders', params=ask_payload, headers=API_KEY)
            if ask_response.status == 200:
                logging.info(f"Placed ask for {ticker} at {ask_price} for {ask_size} shares.")
            else:
                logging.error(f"Failed to place ask: {await ask_response.text()}")

# Individual security trading loop
async def trade_security(session, ticker):
    while True:
        tick = await get_tick(session)
        order_book = await get_market_data(session, ticker)
        position = await get_position(session, ticker)

        bid_price = order_book['bids'][0]['price']
        ask_price = order_book['asks'][0]['price']
        spread = ask_price - bid_price

        # Adjust order sizes dynamically based on liquidity and spread
        bid_size = ask_size = ORDER_SIZE

        # Place orders if spread and profitability conditions are met
        await place_orders(session, ticker, bid_price, ask_price, bid_size, ask_size, position)
        
        # Use adaptive speed bump
        await asyncio.sleep(0.2)

# Main trading loop that runs each security in parallel
async def main():
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            trade_security(session, 'CNR'),
            trade_security(session, 'RY'),
            trade_security(session, 'AC')
        )

# Run the algorithm
asyncio.run(main())
