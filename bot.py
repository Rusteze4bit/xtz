import os
import requests
import pandas as pd
import numpy as np
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("deriv_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DerivAnalyzer")

class DerivMarketAnalyzer:
    def __init__(self, deriv_app_id: Optional[str] = None):
        """
        Initialize the Deriv market analyzer
        
        Args:
            deriv_app_id: Deriv application ID (optional for public API access)
        """
        self.deriv_app_id = deriv_app_id or "1089"  # Default public API ID
        self.api_url = "https://api.deriv.com/api/v1"
        self.markets_data = {}
        
    def fetch_markets(self) -> List[Dict]:
        """
        Fetch available derived indices markets from Deriv API
        
        Returns:
            List of market dictionaries
        """
        try:
            response = requests.get(
                f"{self.api_url}/active_symbols",
                params={
                    "active_symbols": "brief",
                    "product_type": "basic",
                    "landing_company": "virtual"
                },
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            
            # Filter for derived indices
            derived_markets = [
                market for market in data.get("active_symbols", [])
                if market.get("market_display_name") in ["Volatility Indices", "Step Index", "Range Break"]
            ]
            
            logger.info(f"Fetched {len(derived_markets)} derived markets")
            return derived_markets
            
        except Exception as e:
            logger.error(f"Error fetching markets: {e}")
            return []
    
    def fetch_market_quotes(self, symbol: str, count: int = 100) -> Optional[pd.DataFrame]:
        """
        Fetch historical quotes for a specific market symbol
        
        Args:
            symbol: Market symbol to fetch data for
            count: Number of data points to retrieve
            
        Returns:
            DataFrame with market data or None if failed
        """
        try:
            response = requests.get(
                f"{self.api_url}/ticks",
                params={
                    "ticks_history": symbol,
                    "count": count,
                    "style": "ticks"
                },
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            
            if "history" not in data:
                return None
                
            ticks = data["history"]["ticks"]
            df = pd.DataFrame(ticks)
            df['quote'] = df['quote'].astype(float)
            df['epoch'] = pd.to_datetime(df['epoch'], unit='s')
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching quotes for {symbol}: {e}")
            return None
    
    def analyze_market(self, symbol: str, market_name: str) -> Optional[Dict]:
        """
        Analyze a specific market for trading opportunities
        
        Args:
            symbol: Market symbol
            market_name: Display name of the market
            
        Returns:
            Dictionary with analysis results or None if analysis failed
        """
        try:
            # Fetch market data
            df = self.fetch_market_quotes(symbol)
            if df is None or len(df) < 50:
                return None
            
            # Calculate indicators
            df['sma_20'] = df['quote'].rolling(window=20).mean()
            df['sma_50'] = df['quote'].rolling(window=50).mean()
            df['price_change'] = df['quote'].pct_change()
            df['volatility'] = df['quote'].rolling(window=20).std()
            
            current_price = df['quote'].iloc[-1]
            sma_20 = df['sma_20'].iloc[-1]
            sma_50 = df['sma_50'].iloc[-1]
            volatility = df['volatility'].iloc[-1]
            
            # Calculate recovery metrics
            recent_low = df['quote'].tail(10).min()
            recent_high = df['quote'].tail(10).max()
            recovery_from_low = ((current_price - recent_low) / recent_low) * 100
            recovery_from_high = ((current_price - recent_high) / recent_high) * 100
            
            # Determine signal based on criteria
            signal = None
            trade_type = None
            
            # Check for over 1 and recovery between 5 and 8
            if (abs(recovery_from_low) > 1 and 
                5 <= abs(recovery_from_low) <= 8):
                signal = "BUY" if recovery_from_low > 0 else "SELL"
                trade_type = f"Recovery between 5-8% ({recovery_from_low:.2f}%)"
            
            # Check for under 5 recovery
            elif (abs(recovery_from_high) > 1 and 
                  abs(recovery_from_high) < 5):
                signal = "BUY" if recovery_from_high < 0 else "SELL"
                trade_type = f"Recovery under 5% ({recovery_from_high:.2f}%)"
            
            # Prepare analysis result
            analysis_result = {
                "symbol": symbol,
                "market_name": market_name,
                "current_price": current_price,
                "sma_20": sma_20,
                "sma_50": sma_50,
                "volatility": volatility,
                "recovery_from_low": recovery_from_low,
                "recovery_from_high": recovery_from_high,
                "signal": signal,
                "trade_type": trade_type,
                "timestamp": datetime.now().isoformat()
            }
            
            return analysis_result
            
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {e}")
            return None
    
    def analyze_all_markets(self) -> List[Dict]:
        """
        Analyze all available derived markets
        
        Returns:
            List of analysis results with potential signals
        """
        markets = self.fetch_markets()
        signals = []
        
        for market in markets:
            symbol = market.get("symbol")
            market_name = market.get("market_display_name", "Unknown")
            
            logger.info(f"Analyzing {symbol} ({market_name})")
            analysis = self.analyze_market(symbol, market_name)
            
            if analysis and analysis.get("signal"):
                signals.append(analysis)
                logger.info(f"Signal found for {symbol}: {analysis['signal']}")
            
            # Be respectful to the API
            time.sleep(0.5)
        
        return signals


class TelegramBot:
    def __init__(self, bot_token: str, channel_id: str):
        """
        Initialize Telegram bot
        
        Args:
            bot_token: Telegram bot token from BotFather
            channel_id: Telegram channel ID (with @ for public channels or -100 for private)
        """
        self.bot_token = bot_token
        self.channel_id = channel_id
        self.api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    def send_signal(self, analysis: Dict) -> bool:
        """
        Send trading signal to Telegram channel
        
        Args:
            analysis: Market analysis dictionary
            
        Returns:
            True if message was sent successfully, False otherwise
        """
        try:
            # Format the message
            message = self.format_signal_message(analysis)
            
            # Send to Telegram
            response = requests.post(
                self.api_url,
                json={
                    "chat_id": self.channel_id,
                    "text": message,
                    "parse_mode": "HTML"
                },
                timeout=10
            )
            response.raise_for_status()
            
            logger.info(f"Signal sent to Telegram for {analysis['symbol']}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")
            return False
    
    def format_signal_message(self, analysis: Dict) -> str:
        """
        Format analysis results into a readable Telegram message
        
        Args:
            analysis: Market analysis dictionary
            
        Returns:
            Formatted HTML message string
        """
        symbol = analysis["symbol"]
        market = analysis["market_name"]
        price = analysis["current_price"]
        signal = analysis["signal"]
        trade_type = analysis["trade_type"]
        recovery_low = analysis["recovery_from_low"]
        recovery_high = analysis["recovery_from_high"]
        volatility = analysis["volatility"]
        timestamp = datetime.fromisoformat(analysis["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
        
        message = f"""
🚀 <b>Deriv Trading Signal</b> 🚀

<b>Market:</b> {market} ({symbol})
<b>Signal:</b> <code>{signal}</code>
<b>Type:</b> {trade_type}
<b>Current Price:</b> {price:.4f}
<b>Volatility:</b> {volatility:.4f}
<b>Recovery from Low:</b> {recovery_low:.2f}%
<b>Recovery from High:</b> {recovery_high:.2f}%

<b>Timestamp:</b> {timestamp}

⚠️ <i>Disclaimer: This is not financial advice. Trade at your own risk.</i>
        """
        
        return message


def main():
    """
    Main function to run the Deriv market analysis and Telegram signaling
    """
    # Get configuration from environment variables
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID")
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        logger.error("Missing required environment variables: TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID")
        return
    
    # Initialize components
    analyzer = DerivMarketAnalyzer()
    telegram_bot = TelegramBot(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID)
    
    logger.info("Starting Deriv market analysis...")
    
    # Run analysis in a loop
    while True:
        try:
            signals = analyzer.analyze_all_markets()
            
            if signals:
                logger.info(f"Found {len(signals)} signals")
                for signal in signals:
                    telegram_bot.send_signal(signal)
            else:
                logger.info("No signals found in this iteration")
            
            # Wait before next analysis (e.g., 15 minutes)
            logger.info("Waiting for next analysis cycle...")
            time.sleep(900)  # 15 minutes
            
        except KeyboardInterrupt:
            logger.info("Analysis stopped by user")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            time.sleep(300)  # Wait 5 minutes before retrying after error


if __name__ == "__main__":
    main()
