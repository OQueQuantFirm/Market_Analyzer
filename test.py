import os
import ccxt
import time
import csv
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import json
from decimal_to_precision import decimal_to_precision, DECIMAL_PLACES, ROUND, TRUNCATE, NO_PADDING

class OHLCVAnalyzer:
    SYMBOL = 'TIA/USDT:USDT'
    TIMEFRAME = '15m'
    DATA_FILE = 'tia_usdt.csv'  # Updated file name
    LIMIT = 100
    LEVERAGE = 5
    ORDER_PERCENTAGE = 0.01  # 10% of the balance
    LOG_FILE = 'tia.log'

    BUY_RSI = 46
    SELL_RSI = 46
    
    # New constants for take profit and stop loss
    TAKE_PROFIT_MULTIPLIER = 1.25
    STOP_LOSS_MULTIPLIER = 0.85
    
    def __init__(self):
        # Load environment variables from the dotenv file
        load_dotenv()
        self.initialize_csv_file()
        
        # Add new attributes for dynamic imbalance thresholds
        self.dynamic_buy_imbalance = 0
        self.dynamic_sell_imbalance = 0

        # Call the method to calculate dynamic thresholds
        self.calculate_dynamic_thresholds()

        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                RotatingFileHandler(self.LOG_FILE, maxBytes=1024000, backupCount=5),
                logging.StreamHandler()
            ]
        )

        # Initialize the KuCoin Futures exchange instance
        self.exchange = ccxt.kucoinfutures({
            'apiKey': os.getenv('API_KEY'),
            'secret': os.getenv('SECRET_KEY'),
            'password': os.getenv('PASSPHRASE'),
            'enableRateLimit': True
        })

    def fetch_ohlcv_data(self):
        try:
            # Fetch OHLCV data
            ohlcv_data = self.exchange.fetch_ohlcv(self.SYMBOL, self.TIMEFRAME)
            return ohlcv_data
        except Exception as e:
            raise RuntimeError(f"Error fetching OHLCV data: {e}")

    def analyze_rsi(self, ohlcv_data):
        try:
            # Check if ohlcv_data is not empty
            if not ohlcv_data:
                raise ValueError("No data available for analysis.")

            # Create a DataFrame for better data handling
            df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

            # Check if there are enough data points for RSI calculation
            if len(df) < 14:
                raise ValueError("Not enough data points to calculate RSI.")

            # Convert timestamp to human-readable format
            timestamp = df['timestamp'].iloc[-1]
            timestamp_readable = datetime.utcfromtimestamp(timestamp / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

            # Calculate gains and losses
            delta = df['close'].diff()
            up = delta.copy()
            down = delta.copy()
            up[up < 0] = 0
            down[down > 0] = 0

            # Calculate EMAs of gains and losses
            _gain = up.ewm(alpha=1.0 / 14, adjust=True).mean()
            _loss = down.abs().ewm(alpha=1.0 / 14, adjust=True).mean()

            RS = _gain / _loss
            rsi = 100 - (100 / (1 + RS)).iloc[-1]

            # Debug statement
            print(f"Timestamp: {timestamp_readable}, Current Price: {df['close'].iloc[-1]}, RSI: {rsi}")

            return timestamp_readable, df['close'].iloc[-1], rsi

        except Exception as e:
            print(f"Error analyzing data: {e}")
            return None

    def fetch_order_book(self):
        try:
            # Fetch order book data
            order_book = self.exchange.fetch_order_book(self.SYMBOL, limit=self.LIMIT)
            return order_book
        except Exception as e:
            raise RuntimeError(f"Error fetching order book data: {e}")

    def calculate_order_book_imbalance(self, order_book):
        try:
            # Check if order_book is not empty
            if not order_book:
                raise ValueError("No order book data available for analysis.")

            # Calculate order book imbalance
            bids = order_book.get('bids', [])
            asks = order_book.get('asks', [])

            total_bids = sum(bid[1] for bid in bids)
            total_asks = sum(ask[1] for ask in asks)

            # Check if both total_bids and total_asks are non-zero before performing division
            if total_bids != 0 or total_asks != 0:
                imbalance = (total_bids - total_asks) / (total_bids + total_asks)
            else:
                # Handle the case where both total_bids and total_asks are zero
                imbalance = 0

            # Convert imbalance to percentage
            imbalance_percentage = imbalance * 100

            # Debug statement
            print(f"Order Book Imbalance: {imbalance_percentage}%")

            return imbalance_percentage

        except Exception as e:
            print(f"Error calculating order book imbalance: {e}")
            return None

    def calculate_dynamic_thresholds(self):
        # Load the CSV file into a DataFrame
        try:
            df = pd.read_csv(self.DATA_FILE)

            # Calculate the average values for positive and negative order book imbalance
            average_positive_imbalance = df[df['Order_Book_Imbalance'] > 0]['Order_Book_Imbalance'].mean()
            average_negative_imbalance = df[df['Order_Book_Imbalance'] < 0]['Order_Book_Imbalance'].mean()

            # Set dynamic thresholds based on historical data
            self.dynamic_buy_imbalance = average_positive_imbalance
            self.dynamic_sell_imbalance = average_negative_imbalance

            print(f"Dynamic Buy Order Book Imbalance Threshold: {self.dynamic_buy_imbalance}")
            print(f"Dynamic Sell Order Book Imbalance Threshold: {self.dynamic_sell_imbalance}")

        except Exception as e:
            print(f"Error calculating dynamic thresholds: {e}")

    def generate_signal(self, rsi, order_book_imbalance):
        try:
            # Use dynamic imbalance and RSI thresholds
            if order_book_imbalance >= self.dynamic_buy_imbalance and rsi <= self.BUY_RSI:
                return 'Buy'
            elif order_book_imbalance <= self.dynamic_sell_imbalance and rsi >= self.SELL_RSI:
                return 'Sell'
            else:
                return 'No Signal'

        except Exception as e:
            print(f"Error generating signal: {e}")

        return 'No Signal'
    
    def save_to_json(self, timestamp, price, rsi, order_book_imbalance, signal):
        try:
            # Save historical data to JSON file
            historical_data = {
                'timestamp': timestamp,
                'price': price,
                'rsi': rsi,
                'order_book_imbalance': order_book_imbalance,
                'signal': signal
            }
            with open(self.DATA_FILE, 'a') as json_file:
                json.dump(historical_data, json_file)
                json_file.write('\n')

        except Exception as e:
            print(f"Error saving data to JSON: {e}")
            
    def fetch_balance(self):
        try:
            # Fetch account balance
            balance = self.exchange.fetch_balance()

            # Check if 'info' and 'data' keys are present
            if 'info' in balance and 'data' in balance['info']:
                # Log the account balance
                logging.info("Account Balance: %s", balance)
                return balance

            # Raise an exception if 'info' or 'data' keys are missing
            raise RuntimeError("Error fetching balance: 'info' or 'data' keys are missing in the balance response")

        except Exception as e:
            raise RuntimeError(f"Error fetching balance: {e}")
            
    def load_markets(self):
        # Load markets information
        try:
            if not self.exchange.has['fetchMarkets']:
                return
            self.exchange.load_markets()
        except Exception as e:
            raise RuntimeError(f"Error loading markets: {e}")

    def handle_option_and_params(self, params, method, option):
        # Implement the logic for handling options and parameters
        # This function is specific to your use case and requirements

        # Placeholder logic, modify as needed
        paginate = True  # Example: Set to True for testing
        processed_params = params  # Example: No processing for now

        return paginate, processed_params

    def safe_value(self, dictionary, key, default=None):
        # Safely retrieve a value from a dictionary
        return dictionary[key] if key in dictionary else default

    def fetch_paginated_call_dynamic(self, method, symbol, since, limit, params):
        # Implement the logic for fetching paginated data dynamically
        # This function is specific to your use case and requirements
        pass

    def fetch_orders_by_status(self, status, symbol=None, since=None, limit=None, params={}):
        self.load_markets()
        paginate, params = self.handle_option_and_params(params, 'fetchOrdersByStatus', 'paginate')

        if paginate:
            return self.fetch_paginated_call_dynamic('fetchOrdersByStatus', symbol, since, limit, params)

        stop = self.safe_value(params, 'stop')
        until = self.safe_value(params, 'until') or self.safe_value(params, 'till')
        params = {key: value for key, value in params.items() if key not in ['stop', 'until', 'till']}

        if status == 'closed':
            status = 'done'
        elif status == 'open':
            status = 'active'

        request = {'status': status}

        if symbol is not None:
            market = self.exchange.market(symbol)
            request['symbol'] = market['id']

        if since is not None:
            request['startAt'] = since
        if until is not None:
            request['endAt'] = until

        response = None

        if stop:
            if status != 'active':
                raise ccxt.BadRequest(self.exchange.id + ' fetchOrdersByStatus() can only fetch untriggered stop orders')
            response = self.exchange.futuresPrivateGetStopOrders(self.exchange.extend(request, params))
        else:
            response = self.exchange.futuresPrivateGetOrders(self.exchange.extend(request, params))

        responseData = self.safe_value(response, 'data', {})
        orders = self.safe_value(responseData, 'items', [])

        # Convert orders to a pandas DataFrame
        df = pd.DataFrame(orders)

        return df
    
    def calculate_take_profit_stop_loss(self, entry_price, rounding_mode=ROUND):
        # Calculate take profit and stop loss levels
        precision = self.exchange.markets[self.SYMBOL]['precision']['price']

        # Ensure precision is greater than zero
        if precision <= 0:
            # Set a default precision value (you may adjust this based on your needs)
            precision = 8  # Example: Set to 8 decimal places

        precision = int(precision)
        print(f"Precision: {precision}, type: {type(precision)}")

        take_profit_level = entry_price * self.TAKE_PROFIT_MULTIPLIER
        stop_loss_level = entry_price * self.STOP_LOSS_MULTIPLIER

        # Explicitly set counting_mode to DECIMAL_PLACES
        counting_mode = DECIMAL_PLACES

        # Apply precision to levels
        take_profit_level = decimal_to_precision(take_profit_level, ROUND, precision, TRUNCATE, NO_PADDING, counting_mode)
        stop_loss_level = decimal_to_precision(stop_loss_level, ROUND, precision, TRUNCATE, NO_PADDING, counting_mode)

        return take_profit_level, stop_loss_level
    
    def create_order(self, side, entry_price):
        try:
            # Fetch open or active orders with the same symbol
            open_and_active_orders = self.fetch_orders_by_status(['open', 'active'], symbol=self.SYMBOL)

            # Check if there are existing open or active orders
            if open_and_active_orders is not None and not open_and_active_orders.empty:
                logging.info("There are existing open or active orders. Aborting order creation.")
                return
        
            # Fetch account balance
            balance = self.fetch_balance()

            # Log the account balance
            logging.info("Account Balance: %s", balance)

            # Calculate 5% of the equity in USDT
            equity = balance['info']['data']['accountEquity']
            percentage_of_equity = equity * self.ORDER_PERCENTAGE
            logging.info("5%% of Equity: %s USDT", percentage_of_equity)

            # Use the calculated value as the order amount
            self.amount = percentage_of_equity

            # Calculate take profit and stop loss levels
            take_profit_level, stop_loss_level = self.calculate_take_profit_stop_loss(entry_price, ROUND)

            # Convert precision to an integer before passing it to decimal_to_precision
            precision = int(self.PRECISION)  # Assuming self.PRECISION is where your precision value is stored

            take_profit_level = decimal_to_precision(take_profit_level, ROUND, precision, TRUNCATE, NO_PADDING)
            stop_loss_level = decimal_to_precision(stop_loss_level, ROUND, precision, TRUNCATE, NO_PADDING)

            # Create take profit order
            tp_order_params = {
                'symbol': self.SYMBOL,
                'type': 'limit',
                'side': 'sell' if side == 'buy' else 'buy',
                'amount': self.amount,
                'price': take_profit_level,
                'params': {
                    'postOnly': False,
                    'timeInForce': 'GTC',
                    'leverage': self.LEVERAGE
                }
            }
            tp_order = self.exchange.create_order(**tp_order_params)
            logging.info("Take Profit Order Created: %s", json.dumps(tp_order, indent=4))

            # Create stop loss order
            sl_order_params = {
                'symbol': self.SYMBOL,
                'type': 'limit',
                'side': 'sell' if side == 'buy' else 'buy',
                'amount': self.amount,
                'price': stop_loss_level,
                'params': {
                    'postOnly': False,
                    'timeInForce': 'GTC',
                    'leverage': self.LEVERAGE
                }
            }
            sl_order = self.exchange.create_order(**sl_order_params)
            logging.info("Stop Loss Order Created: %s", json.dumps(sl_order, indent=4))

            # Create entry order
            entry_order_params = {
                'symbol': self.SYMBOL,
                'type': 'limit',
                'side': side,
                'amount': self.amount,
                'price': entry_price,
                'params': {
                    'postOnly': False,
                    'timeInForce': 'GTC',
                    'leverage': self.LEVERAGE
                }
            }
            entry_order = self.exchange.create_order(**entry_order_params)
            logging.info("Entry Order Created: %s", json.dumps(entry_order, indent=4))

        except ccxt.NetworkError as e:
            # Handle network-related issues (e.g., connectivity problems)
            logging.error("Network error creating orders: %s", e, exc_info=True)
        except ccxt.ExchangeError as e:
            # Handle exchange-related issues (e.g., order creation failure)
            logging.error("Exchange error creating orders: %s", e, exc_info=True)
        except ccxt.BaseError as e:
            # Catch other ccxt-related errors
            logging.error("Error creating orders: %s", e, exc_info=True)
        except Exception as e:
            # Handle other exceptions and log the error
            logging.error("Unexpected error creating orders: %s", e, exc_info=True)
            
    def initialize_csv_file(self):
        if not os.path.exists(self.DATA_FILE):
            with open(self.DATA_FILE, 'w', newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(['Timestamp', 'Price', 'RSI', 'Order_Book_Imbalance', 'Signal'])

    def save_to_csv(self, timestamp, price, rsi, order_book_imbalance, signal):
        try:
            with open(self.DATA_FILE, 'a', newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow([timestamp, price, rsi, order_book_imbalance, signal])
        except Exception as e:
            print(f"Error saving data to CSV: {e}")

    def run_infinite_loop(self):
        try:
            while True:
                ohlcv_data = self.fetch_ohlcv_data()
                timestamp, price, rsi = self.analyze_rsi(ohlcv_data)

                order_book = self.fetch_order_book()
                order_book_imbalance = self.calculate_order_book_imbalance(order_book)

                signal = self.generate_signal(rsi, order_book_imbalance)

                # Print the signal for each iteration
                print(f"Signal: {signal}")
                print("-" * 40)

                if timestamp and price and rsi and order_book_imbalance:
                    if signal == 'Buy':
                        # Create a buy order
                        self.create_order('buy', price)
                    elif signal == 'Sell':
                        # Create a sell order
                        self.create_order('sell', price)

                # Save data to CSV
                self.save_to_csv(timestamp, price, rsi, order_book_imbalance, signal)

                time.sleep(10)

        except KeyboardInterrupt:
            print("Exiting the program.")
            
if __name__ == "__main__":
    # Create an instance of OHLCVAnalyzer and run the infinite loop
    analyzer = OHLCVAnalyzer()
    analyzer.run_infinite_loop()
