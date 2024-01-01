import ccxt
import os
import finta
import pandas as pd
import time
import csv
import logging
from datetime import datetime
import asyncio
from tele import TelegramHandler
import nest_asyncio

# Set up Telethon and allow nested event loops
nest_asyncio.apply()

# KuCoin Futures API credentials
API_KEY = os.getenv('API_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
PASSPHRASE = os.getenv('PASSPHRASE')

# Log file names
LOG_FILE_NAME = "RSI_report_log.csv"

# RSI configuration
RSI_PERIOD = 14
RSI_OVERBOUGHT = 80
RSI_OVERSOLD = 20

# Timeframe for fetching OHLCV data
TIMEFRAME = '15m'

# Telegram bot credentials
api_id = '26867899'  # Replace with your actual api_id
api_hash = 'c40fd4360d254fe72b2e0a186d38ccbe'  # Replace with your actual api_hash
bot_token = '6844088602:AAH50Cf5pdst-Lrx2ZGdEz4aIcsrINUulKM'  # Replace with your actual bot token
group_username = 'pquant_bot'  # Replace with the actual username of your public group

# Create the Telethon client
tele_handler = TelegramHandler(api_id, api_hash, bot_token, group_username, 'NewRSITelesession_name')

class OHLCVAnalyzer:
    def __init__(self):
        # Initialize the KuCoin Futures exchange instance
        self.exchange = ccxt.kucoinfutures({
            'apiKey': API_KEY,
            'secret': SECRET_KEY,
            'password': PASSPHRASE,
            'enableRateLimit': True
        })

        # Set up logging
        log_file_full_path = os.path.join(os.getcwd(), LOG_FILE_NAME)
        logging.basicConfig(filename=log_file_full_path, level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

        # Check if the CSV file exists, create it if not
        if not os.path.exists(log_file_full_path):
            with open(log_file_full_path, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['Timestamp', 'Symbol', 'Latest RSI', 'Support', 'Resistance', 'Latest MA', 'Order Book Imbalance', 'Error'])

    def calculate_ema_levels(self, df, period=20):
        try:
            df['ema'] = df['close'].ewm(span=period, adjust=False).mean()
            support = df['ema'].iloc[-1] - (df['high'].iloc[-1] - df['low'].iloc[-1])
            resistance = df['ema'].iloc[-1] + (df['high'].iloc[-1] - df['low'].iloc[-1])
            return support, resistance
        except Exception as e:
            print(f"Error calculating EMA support and resistance: {e}")
            return None, None

    def determine_trend(self, df, short_ema_period=10, long_ema_period=50):
        # Calculate short-term EMA
        df['short_ema'] = df['close'].ewm(span=short_ema_period, adjust=False).mean()

        # Calculate long-term EMA
        df['long_ema'] = df['close'].ewm(span=long_ema_period, adjust=False).mean()

        # Get the current closing price and EMAs
        current_close = df['close'].iloc[-1]
        current_short_ema = df['short_ema'].iloc[-1]
        current_long_ema = df['long_ema'].iloc[-1]

        # Determine the trend
        if current_close > current_short_ema > current_long_ema:
            trend = 'Strong Bullish'
        elif current_close > current_short_ema and current_close > current_long_ema:
            trend = 'Bullish'
        elif current_close < current_short_ema < current_long_ema:
            trend = 'Strong Bearish'
        elif current_close < current_short_ema and current_close < current_long_ema:
            trend = 'Bearish'
        else:
            trend = 'Neutral'

        # Check for potential trend reversal
        potential_reversal = current_short_ema > current_long_ema

        # Convert potential reversal to Bullish or Bearish
        reversal_label = 'Bullish' if potential_reversal else 'Bearish'

        return current_close, trend, reversal_label

    def fetch_order_book(self, symbol, limit=100):
        try:
            order_book = self.exchange.fetch_order_book(symbol, limit=limit)
            if order_book:
                print(f"Successfully fetched order book for {symbol}")
            else:
                print(f"Order book data is None for {symbol}")
            return order_book
        except Exception as e:
            print(f"Error fetching order book data for {symbol}: {e}")
            return None

    def calculate_order_book_imbalance(self, order_book):
        try:
            if not order_book:
                raise ValueError("No order book data available for analysis.")

            bids = order_book.get('bids', [])
            asks = order_book.get('asks', [])

            total_bids = sum(bid[1] for bid in bids)
            total_asks = sum(ask[1] for ask in asks)

            if total_bids != 0 or total_asks != 0:
                imbalance = (total_bids - total_asks) / (total_bids + total_asks)
            else:
                imbalance = 0

            imbalance_percentage = imbalance * 100

            print(f"Successfully calculated order book imbalance: {imbalance_percentage}%")

            return imbalance_percentage

        except Exception as e:
            print(f"Error calculating order book imbalance: {e}")
            return None

    def log_data(self, timestamp, symbol, latest_rsi, support, resistance, latest_ma, order_book_imbalance, error):
        # Log data to the CSV file
        log_file_full_path = os.path.join(os.getcwd(), LOG_FILE_NAME)
        with open(log_file_full_path, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([timestamp, symbol, latest_rsi, support, resistance, latest_ma, order_book_imbalance, error])

    def analyze_volume(self, df):
        # Calculate average volume
        average_volume = df['volume'].mean()

        # Get the latest volume
        latest_volume = df['volume'].iloc[-1]

        # Calculate relative volume strength
        relative_strength = latest_volume / average_volume

        # Interpretation based on volume
        if relative_strength > 1:
            interpretation = "High volume, indicating strong market participation."
        elif relative_strength < 1:
            interpretation = "Low volume, suggesting cautious market participation."
        else:
            interpretation = "Volume is at the average level."

        # Print the volume analysis
        print(f"Latest Volume: {latest_volume}, Average Volume: {average_volume}")
        print(f"Relative Volume Strength: {relative_strength}")
        print(f"Volume Interpretation: {interpretation}")

        # Return the relative strength for further use
        return relative_strength, interpretation

    def analyze_volume_trend_direction(self, df):
        
        # Get the latest closing price and EMAs
        latest_close = df['close'].iloc[-1]
        current_short_ema = df['short_ema'].iloc[-1]
        current_long_ema = df['long_ema'].iloc[-1]

        # Interpretation based on volume and trend direction
        if latest_close > current_short_ema > current_long_ema:
            interpretation = "High volume, strong uptrend."
        elif latest_close < current_short_ema < current_long_ema:
            interpretation = "High volume, strong downtrend."
        else:
            interpretation = "High volume, neutral trend direction."

        # Print the trend direction analysis
        print(f"Latest Close: {latest_close}, Current Short EMA: {current_short_ema}, Current Long EMA: {current_long_ema}")
        print(f"Trend Direction Interpretation: {interpretation}")

    async def fetch_and_analyze_symbols(self):
        try:
            logging.info("Script started.")
            while True:
                try:
                    # Fetch all symbols from the exchange using environment variables
                    symbols = self.exchange.load_markets().keys()

                    # Inside your fetch_and_analyze_symbols method
                    for symbol in symbols:
                        ohlcv_data = self.exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=100)

                        if ohlcv_data:
                            df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                            df['rsi'] = finta.TA.RSI(df, period=RSI_PERIOD)

                            latest_rsi = df['rsi'].iloc[-1]

                            # Corrected RSI condition
                            if latest_rsi > RSI_OVERBOUGHT or latest_rsi < RSI_OVERSOLD:
                                timestamp = pd.to_datetime(df['timestamp'].iloc[-1], unit='ms')

                                # Calculate EMA Support and Resistance
                                support, resistance = self.calculate_ema_levels(df)

                                # Determine Trend using EMA and check for potential reversal
                                current_close, trend, reversal_label = self.determine_trend(df, short_ema_period=10, long_ema_period=50)

                                # Fetch Order Book and Calculate Imbalance
                                order_book = self.fetch_order_book(symbol)
                                order_book_imbalance = self.calculate_order_book_imbalance(order_book)

                                # Analyze Volume
                                relative_strength, volume_interpretation = self.analyze_volume(df)

                               # Analyze Volume Trend Direction
                                self.analyze_volume_trend_direction(df)

                                message = (
                                    f"📈 **Symbol:** {symbol}\n"
                                    f"📚 **Order Book Imbalance:** {order_book_imbalance:.2f}%\n"
                                    f"📊 **Latest RSI:** {latest_rsi:.2f}\n"
                                    f"🔔 **Support:** {support:.2f}, **Resistance:** {resistance:.2f}\n"
                                    f"🔍 **Trend Direction Interpretation:** {volume_interpretation}\n"  # Correct variable name here
                                    f"📉 **Trend:** {trend}\n"
                                    f"🔍 **Relative Volume Strength:** {relative_strength:.2%}"
                                )

                                print(message)
                                await tele_handler.send_message(message)
                                self.log_data(timestamp, symbol, latest_rsi, support, resistance, current_close, order_book_imbalance, None)

                    # Sleep for 10 seconds before the next iteration
                    await asyncio.sleep(10)

                except Exception as e:
                    timestamp = pd.to_datetime('now')
                    print(f"Error fetching and analyzing symbols: {e}")
                    logging.error(f"Error fetching and analyzing symbols: {e}")
                    self.log_data(timestamp, None, None, None, None, None, None, str(e))
        except KeyboardInterrupt:
            logging.info("Script interrupted.")
        except Exception as ex:
            logging.exception(f"Unexpected error: {ex}")

# Example usage
analyzer = OHLCVAnalyzer()
asyncio.run(analyzer.fetch_and_analyze_symbols())
