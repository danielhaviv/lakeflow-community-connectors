import requests
import time
from datetime import datetime, timedelta
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    ArrayType,
)
from typing import Dict, List, Tuple, Iterator, Any


class LakeflowConnect:
    """
    MarketData.app Connector for Lakeflow.
    
    Provides access to financial market data including:
    - Stock quotes (real-time prices)
    - Stock candles (OHLCV historical data)
    - Stock earnings
    - Options chains
    - Index quotes
    - Market status
    """

    def __init__(self, options: dict) -> None:
        """
        Initialize the MarketData connector with API credentials.

        Args:
            options: Dictionary containing:
                - api_token: MarketData.app API token
                - base_url (optional): Override for API base URL (default: https://api.marketdata.app/v1)
        """
        self.api_token = options.get("api_token")
        if not self.api_token:
            raise ValueError("MarketData connector requires 'api_token' in options")
        
        self.base_url = options.get("base_url", "https://api.marketdata.app/v1").rstrip("/")
        
        # Configure session with proper headers
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {self.api_token}",
            "Accept": "application/json",
        })

        # Cache for schemas
        self._schema_cache = {}

        # Centralized object metadata configuration
        self._object_config = {
            "stocks_quotes": {
                "primary_keys": ["symbol", "timestamp"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "endpoint": "stocks/quotes",
                "description": "Real-time stock quotes",
            },
            "stocks_candles": {
                "primary_keys": ["symbol", "timestamp"],
                "cursor_field": "timestamp",
                "ingestion_type": "append",
                "endpoint": "stocks/candles",
                "description": "Historical OHLCV candle data for stocks",
            },
            "stocks_earnings": {
                "primary_keys": ["symbol", "fiscal_year", "fiscal_quarter"],
                "cursor_field": "report_date",
                "ingestion_type": "append",
                "endpoint": "stocks/earnings",
                "description": "Stock earnings reports",
            },
            "options_chain": {
                "primary_keys": ["option_symbol"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "endpoint": "options/chain",
                "description": "Options chain data",
            },
            "options_quotes": {
                "primary_keys": ["option_symbol", "timestamp"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "endpoint": "options/quotes",
                "description": "Real-time options quotes",
            },
            "indices_quotes": {
                "primary_keys": ["symbol", "timestamp"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "endpoint": "indices/quotes",
                "description": "Index quotes (S&P 500, NASDAQ, etc.)",
            },
            "indices_candles": {
                "primary_keys": ["symbol", "timestamp"],
                "cursor_field": "timestamp",
                "ingestion_type": "append",
                "endpoint": "indices/candles",
                "description": "Historical candle data for indices",
            },
            "markets_status": {
                "primary_keys": ["date"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "endpoint": "markets/status",
                "description": "Market open/close status",
            },
        }

        # Schema definitions
        self._schema_config = {
            "stocks_quotes": StructType([
                StructField("symbol", StringType(), False),
                StructField("timestamp", LongType(), False),
                StructField("ask", DoubleType(), True),
                StructField("ask_size", LongType(), True),
                StructField("bid", DoubleType(), True),
                StructField("bid_size", LongType(), True),
                StructField("mid", DoubleType(), True),
                StructField("last", DoubleType(), True),
                StructField("change", DoubleType(), True),
                StructField("change_percent", DoubleType(), True),
                StructField("high_52_week", DoubleType(), True),
                StructField("low_52_week", DoubleType(), True),
                StructField("volume", LongType(), True),
                StructField("updated", LongType(), True),
            ]),
            "stocks_candles": StructType([
                StructField("symbol", StringType(), False),
                StructField("timestamp", LongType(), False),
                StructField("open", DoubleType(), True),
                StructField("high", DoubleType(), True),
                StructField("low", DoubleType(), True),
                StructField("close", DoubleType(), True),
                StructField("volume", LongType(), True),
                StructField("resolution", StringType(), True),
            ]),
            "stocks_earnings": StructType([
                StructField("symbol", StringType(), False),
                StructField("fiscal_year", LongType(), False),
                StructField("fiscal_quarter", LongType(), False),
                StructField("date", StringType(), True),
                StructField("report_date", StringType(), True),
                StructField("report_time", StringType(), True),
                StructField("currency", StringType(), True),
                StructField("reported_eps", DoubleType(), True),
                StructField("estimated_eps", DoubleType(), True),
                StructField("surprise_eps", DoubleType(), True),
                StructField("surprise_eps_percent", DoubleType(), True),
                StructField("reported_revenue", DoubleType(), True),
                StructField("estimated_revenue", DoubleType(), True),
                StructField("updated", LongType(), True),
            ]),
            "options_chain": StructType([
                StructField("option_symbol", StringType(), False),
                StructField("underlying", StringType(), True),
                StructField("expiration", StringType(), True),
                StructField("side", StringType(), True),
                StructField("strike", DoubleType(), True),
                StructField("first_traded", StringType(), True),
                StructField("dte", LongType(), True),
                StructField("ask", DoubleType(), True),
                StructField("ask_size", LongType(), True),
                StructField("bid", DoubleType(), True),
                StructField("bid_size", LongType(), True),
                StructField("mid", DoubleType(), True),
                StructField("last", DoubleType(), True),
                StructField("open_interest", LongType(), True),
                StructField("volume", LongType(), True),
                StructField("in_the_money", BooleanType(), True),
                StructField("intrinsic_value", DoubleType(), True),
                StructField("extrinsic_value", DoubleType(), True),
                StructField("underlying_price", DoubleType(), True),
                StructField("iv", DoubleType(), True),
                StructField("delta", DoubleType(), True),
                StructField("gamma", DoubleType(), True),
                StructField("theta", DoubleType(), True),
                StructField("vega", DoubleType(), True),
                StructField("rho", DoubleType(), True),
                StructField("updated", LongType(), True),
            ]),
            "options_quotes": StructType([
                StructField("option_symbol", StringType(), False),
                StructField("timestamp", LongType(), False),
                StructField("ask", DoubleType(), True),
                StructField("ask_size", LongType(), True),
                StructField("bid", DoubleType(), True),
                StructField("bid_size", LongType(), True),
                StructField("mid", DoubleType(), True),
                StructField("last", DoubleType(), True),
                StructField("volume", LongType(), True),
                StructField("open_interest", LongType(), True),
                StructField("underlying_price", DoubleType(), True),
                StructField("iv", DoubleType(), True),
                StructField("delta", DoubleType(), True),
                StructField("gamma", DoubleType(), True),
                StructField("theta", DoubleType(), True),
                StructField("vega", DoubleType(), True),
                StructField("updated", LongType(), True),
            ]),
            "indices_quotes": StructType([
                StructField("symbol", StringType(), False),
                StructField("timestamp", LongType(), False),
                StructField("last", DoubleType(), True),
                StructField("change", DoubleType(), True),
                StructField("change_percent", DoubleType(), True),
                StructField("high_52_week", DoubleType(), True),
                StructField("low_52_week", DoubleType(), True),
                StructField("updated", LongType(), True),
            ]),
            "indices_candles": StructType([
                StructField("symbol", StringType(), False),
                StructField("timestamp", LongType(), False),
                StructField("open", DoubleType(), True),
                StructField("high", DoubleType(), True),
                StructField("low", DoubleType(), True),
                StructField("close", DoubleType(), True),
                StructField("resolution", StringType(), True),
            ]),
            "markets_status": StructType([
                StructField("date", StringType(), False),
                StructField("status", StringType(), True),
                StructField("open_time", StringType(), True),
                StructField("close_time", StringType(), True),
                StructField("is_open", BooleanType(), True),
                StructField("next_open", StringType(), True),
                StructField("next_close", StringType(), True),
            ]),
        }

    def list_tables(self) -> list[str]:
        """
        List available MarketData tables/objects.

        Returns:
            List of supported table names
        """
        return list(self._object_config.keys())

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Get the Spark schema for a MarketData table.

        Args:
            table_name: Name of the table
            table_options: Table-specific options

        Returns:
            StructType representing the table schema
        """
        if table_name not in self._schema_config:
            raise ValueError(f"Unsupported table: {table_name}")
        return self._schema_config[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Get metadata for a MarketData table.

        Args:
            table_name: Name of the table
            table_options: Table-specific options

        Returns:
            Dictionary with primary_keys, cursor_field, and ingestion_type
        """
        if table_name not in self._object_config:
            raise ValueError(f"Unsupported table: {table_name}")
        
        config = self._object_config[table_name]
        metadata = {
            "primary_keys": config["primary_keys"],
            "ingestion_type": config["ingestion_type"],
        }
        if config.get("cursor_field"):
            metadata["cursor_field"] = config["cursor_field"]
        return metadata

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[Dict], Dict]:
        """
        Read data from a MarketData table.

        Args:
            table_name: Name of the table to read
            start_offset: Dictionary containing cursor information for incremental reads
            table_options: Table-specific options including:
                - symbol/symbols: Stock symbol(s) to fetch (comma-separated for multiple)
                - resolution: Candle resolution (1, 5, 15, 30, 60, D, W, M)
                - from_date: Start date for historical data (YYYY-MM-DD)
                - to_date: End date for historical data (YYYY-MM-DD)
                - expiration: Options expiration date (YYYY-MM-DD)
                - side: Options side (call/put)

        Returns:
            Tuple of (records iterator, new_offset)
        """
        if table_name not in self._object_config:
            raise ValueError(f"Unsupported table: {table_name}")

        # Route to appropriate handler
        if table_name == "stocks_quotes":
            return self._read_stocks_quotes(start_offset, table_options)
        elif table_name == "stocks_candles":
            return self._read_stocks_candles(start_offset, table_options)
        elif table_name == "stocks_earnings":
            return self._read_stocks_earnings(start_offset, table_options)
        elif table_name == "options_chain":
            return self._read_options_chain(start_offset, table_options)
        elif table_name == "options_quotes":
            return self._read_options_quotes(start_offset, table_options)
        elif table_name == "indices_quotes":
            return self._read_indices_quotes(start_offset, table_options)
        elif table_name == "indices_candles":
            return self._read_indices_candles(start_offset, table_options)
        elif table_name == "markets_status":
            return self._read_markets_status(start_offset, table_options)
        else:
            raise ValueError(f"Unsupported table: {table_name}")

    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """
        Make an API request to MarketData.app.

        Args:
            endpoint: API endpoint path
            params: Query parameters

        Returns:
            JSON response as dictionary
        """
        url = f"{self.base_url}/{endpoint}"
        response = self._session.get(url, params=params, timeout=30)
        
        if response.status_code == 429:
            # Rate limited - wait and retry
            time.sleep(1)
            response = self._session.get(url, params=params, timeout=30)
        
        # MarketData.app returns 200 or 203 for successful responses
        if response.status_code not in (200, 203):
            raise RuntimeError(
                f"MarketData API error: {response.status_code} {response.text}"
            )
        
        return response.json()

    def _parse_symbols(self, table_options: Dict[str, str]) -> List[str]:
        """
        Parse symbol(s) from table options.
        
        Args:
            table_options: Table options containing 'symbol' or 'symbols'
            
        Returns:
            List of symbols
        """
        symbols = table_options.get("symbols", table_options.get("symbol", ""))
        if not symbols:
            raise ValueError("table_options must include 'symbol' or 'symbols'")
        
        if isinstance(symbols, str):
            return [s.strip().upper() for s in symbols.split(",")]
        return [s.upper() for s in symbols]

    def _read_stocks_quotes(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[Dict], Dict]:
        """Read real-time stock quotes."""
        symbols = self._parse_symbols(table_options)
        
        records = []
        for symbol in symbols:
            try:
                data = self._make_request(f"stocks/quotes/{symbol}")
                
                # Handle response format (can be single or array)
                if data.get("s") == "ok":
                    quote = {
                        "symbol": symbol,
                        "timestamp": data.get("updated", [0])[0] if isinstance(data.get("updated"), list) else data.get("updated", 0),
                        "ask": self._safe_float(data.get("ask", [None])),
                        "ask_size": self._safe_int(data.get("askSize", [None])),
                        "bid": self._safe_float(data.get("bid", [None])),
                        "bid_size": self._safe_int(data.get("bidSize", [None])),
                        "mid": self._safe_float(data.get("mid", [None])),
                        "last": self._safe_float(data.get("last", [None])),
                        "change": self._safe_float(data.get("change", [None])),
                        "change_percent": self._safe_float(data.get("changepct", [None])),
                        "high_52_week": self._safe_float(data.get("52weekHigh", [None])),
                        "low_52_week": self._safe_float(data.get("52weekLow", [None])),
                        "volume": self._safe_int(data.get("volume", [None])),
                        "updated": self._safe_int(data.get("updated", [None])),
                    }
                    records.append(quote)
            except Exception as e:
                # Log error but continue with other symbols
                print(f"Error fetching quote for {symbol}: {e}")
            
            time.sleep(0.1)  # Rate limiting
        
        return iter(records), {}

    def _read_stocks_candles(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[Dict], Dict]:
        """Read historical OHLCV candle data for stocks."""
        symbols = self._parse_symbols(table_options)
        resolution = table_options.get("resolution", "D")
        
        # Date range handling
        to_date = table_options.get("to_date", datetime.now().strftime("%Y-%m-%d"))
        
        # Check for incremental read
        if start_offset and start_offset.get("last_timestamp"):
            from_date = datetime.fromtimestamp(start_offset["last_timestamp"]).strftime("%Y-%m-%d")
        else:
            from_date = table_options.get("from_date", (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        
        records = []
        latest_timestamp = 0
        
        for symbol in symbols:
            try:
                params = {
                    "resolution": resolution,
                    "from": from_date,
                    "to": to_date,
                }
                data = self._make_request(f"stocks/candles/{resolution}/{symbol}", params)
                
                if data.get("s") == "ok":
                    timestamps = data.get("t", [])
                    opens = data.get("o", [])
                    highs = data.get("h", [])
                    lows = data.get("l", [])
                    closes = data.get("c", [])
                    volumes = data.get("v", [])
                    
                    for i in range(len(timestamps)):
                        ts = timestamps[i] if i < len(timestamps) else None
                        if ts and ts > latest_timestamp:
                            latest_timestamp = ts
                        
                        candle = {
                            "symbol": symbol,
                            "timestamp": ts,
                            "open": opens[i] if i < len(opens) else None,
                            "high": highs[i] if i < len(highs) else None,
                            "low": lows[i] if i < len(lows) else None,
                            "close": closes[i] if i < len(closes) else None,
                            "volume": volumes[i] if i < len(volumes) else None,
                            "resolution": resolution,
                        }
                        records.append(candle)
            except Exception as e:
                print(f"Error fetching candles for {symbol}: {e}")
            
            time.sleep(0.1)
        
        # Return offset for incremental sync
        offset = {"last_timestamp": latest_timestamp} if latest_timestamp > 0 else {}
        return iter(records), offset

    def _read_stocks_earnings(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[Dict], Dict]:
        """Read stock earnings reports."""
        symbols = self._parse_symbols(table_options)
        
        # Date range for earnings
        from_date = table_options.get("from_date", (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"))
        to_date = table_options.get("to_date", datetime.now().strftime("%Y-%m-%d"))
        
        records = []
        latest_report_date = None
        
        for symbol in symbols:
            try:
                params = {
                    "from": from_date,
                    "to": to_date,
                }
                data = self._make_request(f"stocks/earnings/{symbol}", params)
                
                if data.get("s") == "ok":
                    # Parse earnings data
                    fiscal_years = data.get("fiscalYear", [])
                    fiscal_quarters = data.get("fiscalQuarter", [])
                    dates = data.get("date", [])
                    report_dates = data.get("reportDate", [])
                    report_times = data.get("reportTime", [])
                    currencies = data.get("currency", [])
                    reported_eps_list = data.get("reportedEPS", [])
                    estimated_eps_list = data.get("estimatedEPS", [])
                    surprise_eps_list = data.get("surpriseEPS", [])
                    surprise_eps_pct_list = data.get("surpriseEPSpct", [])
                    reported_revenue_list = data.get("reportedRevenue", [])
                    estimated_revenue_list = data.get("estimatedRevenue", [])
                    updated_list = data.get("updated", [])
                    
                    for i in range(len(fiscal_years)):
                        report_date = report_dates[i] if i < len(report_dates) else None
                        if report_date and (latest_report_date is None or report_date > latest_report_date):
                            latest_report_date = report_date
                        
                        earning = {
                            "symbol": symbol,
                            "fiscal_year": fiscal_years[i] if i < len(fiscal_years) else None,
                            "fiscal_quarter": fiscal_quarters[i] if i < len(fiscal_quarters) else None,
                            "date": dates[i] if i < len(dates) else None,
                            "report_date": report_date,
                            "report_time": report_times[i] if i < len(report_times) else None,
                            "currency": currencies[i] if i < len(currencies) else None,
                            "reported_eps": reported_eps_list[i] if i < len(reported_eps_list) else None,
                            "estimated_eps": estimated_eps_list[i] if i < len(estimated_eps_list) else None,
                            "surprise_eps": surprise_eps_list[i] if i < len(surprise_eps_list) else None,
                            "surprise_eps_percent": surprise_eps_pct_list[i] if i < len(surprise_eps_pct_list) else None,
                            "reported_revenue": reported_revenue_list[i] if i < len(reported_revenue_list) else None,
                            "estimated_revenue": estimated_revenue_list[i] if i < len(estimated_revenue_list) else None,
                            "updated": updated_list[i] if i < len(updated_list) else None,
                        }
                        records.append(earning)
            except Exception as e:
                print(f"Error fetching earnings for {symbol}: {e}")
            
            time.sleep(0.1)
        
        offset = {"last_report_date": latest_report_date} if latest_report_date else {}
        return iter(records), offset

    def _read_options_chain(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[Dict], Dict]:
        """Read options chain data."""
        symbol = table_options.get("symbol")
        if not symbol:
            raise ValueError("table_options must include 'symbol' for options_chain")
        symbol = symbol.upper()
        
        expiration = table_options.get("expiration")
        side = table_options.get("side")  # 'call' or 'put'
        
        params = {}
        if expiration:
            params["expiration"] = expiration
        if side:
            params["side"] = side
        
        records = []
        try:
            data = self._make_request(f"options/chain/{symbol}", params)
            
            if data.get("s") == "ok":
                option_symbols = data.get("optionSymbol", [])
                underlyings = data.get("underlying", [])
                expirations = data.get("expiration", [])
                sides = data.get("side", [])
                strikes = data.get("strike", [])
                first_traded_list = data.get("firstTraded", [])
                dte_list = data.get("dte", [])
                asks = data.get("ask", [])
                ask_sizes = data.get("askSize", [])
                bids = data.get("bid", [])
                bid_sizes = data.get("bidSize", [])
                mids = data.get("mid", [])
                lasts = data.get("last", [])
                open_interests = data.get("openInterest", [])
                volumes = data.get("volume", [])
                in_the_money_list = data.get("inTheMoney", [])
                intrinsic_values = data.get("intrinsicValue", [])
                extrinsic_values = data.get("extrinsicValue", [])
                underlying_prices = data.get("underlyingPrice", [])
                ivs = data.get("iv", [])
                deltas = data.get("delta", [])
                gammas = data.get("gamma", [])
                thetas = data.get("theta", [])
                vegas = data.get("vega", [])
                rhos = data.get("rho", [])
                updated_list = data.get("updated", [])
                
                for i in range(len(option_symbols)):
                    option = {
                        "option_symbol": option_symbols[i] if i < len(option_symbols) else None,
                        "underlying": underlyings[i] if i < len(underlyings) else symbol,
                        "expiration": expirations[i] if i < len(expirations) else None,
                        "side": sides[i] if i < len(sides) else None,
                        "strike": strikes[i] if i < len(strikes) else None,
                        "first_traded": first_traded_list[i] if i < len(first_traded_list) else None,
                        "dte": dte_list[i] if i < len(dte_list) else None,
                        "ask": asks[i] if i < len(asks) else None,
                        "ask_size": ask_sizes[i] if i < len(ask_sizes) else None,
                        "bid": bids[i] if i < len(bids) else None,
                        "bid_size": bid_sizes[i] if i < len(bid_sizes) else None,
                        "mid": mids[i] if i < len(mids) else None,
                        "last": lasts[i] if i < len(lasts) else None,
                        "open_interest": open_interests[i] if i < len(open_interests) else None,
                        "volume": volumes[i] if i < len(volumes) else None,
                        "in_the_money": in_the_money_list[i] if i < len(in_the_money_list) else None,
                        "intrinsic_value": intrinsic_values[i] if i < len(intrinsic_values) else None,
                        "extrinsic_value": extrinsic_values[i] if i < len(extrinsic_values) else None,
                        "underlying_price": underlying_prices[i] if i < len(underlying_prices) else None,
                        "iv": ivs[i] if i < len(ivs) else None,
                        "delta": deltas[i] if i < len(deltas) else None,
                        "gamma": gammas[i] if i < len(gammas) else None,
                        "theta": thetas[i] if i < len(thetas) else None,
                        "vega": vegas[i] if i < len(vegas) else None,
                        "rho": rhos[i] if i < len(rhos) else None,
                        "updated": updated_list[i] if i < len(updated_list) else None,
                    }
                    records.append(option)
        except Exception as e:
            print(f"Error fetching options chain for {symbol}: {e}")
        
        return iter(records), {}

    def _read_options_quotes(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[Dict], Dict]:
        """Read real-time options quotes."""
        option_symbol = table_options.get("option_symbol")
        if not option_symbol:
            raise ValueError("table_options must include 'option_symbol' for options_quotes")
        
        records = []
        try:
            data = self._make_request(f"options/quotes/{option_symbol}")
            
            if data.get("s") == "ok":
                quote = {
                    "option_symbol": option_symbol,
                    "timestamp": self._safe_int(data.get("updated", [None])),
                    "ask": self._safe_float(data.get("ask", [None])),
                    "ask_size": self._safe_int(data.get("askSize", [None])),
                    "bid": self._safe_float(data.get("bid", [None])),
                    "bid_size": self._safe_int(data.get("bidSize", [None])),
                    "mid": self._safe_float(data.get("mid", [None])),
                    "last": self._safe_float(data.get("last", [None])),
                    "volume": self._safe_int(data.get("volume", [None])),
                    "open_interest": self._safe_int(data.get("openInterest", [None])),
                    "underlying_price": self._safe_float(data.get("underlyingPrice", [None])),
                    "iv": self._safe_float(data.get("iv", [None])),
                    "delta": self._safe_float(data.get("delta", [None])),
                    "gamma": self._safe_float(data.get("gamma", [None])),
                    "theta": self._safe_float(data.get("theta", [None])),
                    "vega": self._safe_float(data.get("vega", [None])),
                    "updated": self._safe_int(data.get("updated", [None])),
                }
                records.append(quote)
        except Exception as e:
            print(f"Error fetching options quote for {option_symbol}: {e}")
        
        return iter(records), {}

    def _read_indices_quotes(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[Dict], Dict]:
        """Read real-time index quotes."""
        symbols = self._parse_symbols(table_options)
        
        records = []
        for symbol in symbols:
            try:
                data = self._make_request(f"indices/quotes/{symbol}")
                
                if data.get("s") == "ok":
                    quote = {
                        "symbol": symbol,
                        "timestamp": self._safe_int(data.get("updated", [None])),
                        "last": self._safe_float(data.get("last", [None])),
                        "change": self._safe_float(data.get("change", [None])),
                        "change_percent": self._safe_float(data.get("changepct", [None])),
                        "high_52_week": self._safe_float(data.get("52weekHigh", [None])),
                        "low_52_week": self._safe_float(data.get("52weekLow", [None])),
                        "updated": self._safe_int(data.get("updated", [None])),
                    }
                    records.append(quote)
            except Exception as e:
                print(f"Error fetching index quote for {symbol}: {e}")
            
            time.sleep(0.1)
        
        return iter(records), {}

    def _read_indices_candles(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[Dict], Dict]:
        """Read historical candle data for indices."""
        symbols = self._parse_symbols(table_options)
        resolution = table_options.get("resolution", "D")
        
        to_date = table_options.get("to_date", datetime.now().strftime("%Y-%m-%d"))
        
        if start_offset and start_offset.get("last_timestamp"):
            from_date = datetime.fromtimestamp(start_offset["last_timestamp"]).strftime("%Y-%m-%d")
        else:
            from_date = table_options.get("from_date", (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        
        records = []
        latest_timestamp = 0
        
        for symbol in symbols:
            try:
                params = {
                    "resolution": resolution,
                    "from": from_date,
                    "to": to_date,
                }
                data = self._make_request(f"indices/candles/{resolution}/{symbol}", params)
                
                if data.get("s") == "ok":
                    timestamps = data.get("t", [])
                    opens = data.get("o", [])
                    highs = data.get("h", [])
                    lows = data.get("l", [])
                    closes = data.get("c", [])
                    
                    for i in range(len(timestamps)):
                        ts = timestamps[i] if i < len(timestamps) else None
                        if ts and ts > latest_timestamp:
                            latest_timestamp = ts
                        
                        candle = {
                            "symbol": symbol,
                            "timestamp": ts,
                            "open": opens[i] if i < len(opens) else None,
                            "high": highs[i] if i < len(highs) else None,
                            "low": lows[i] if i < len(lows) else None,
                            "close": closes[i] if i < len(closes) else None,
                            "resolution": resolution,
                        }
                        records.append(candle)
            except Exception as e:
                print(f"Error fetching index candles for {symbol}: {e}")
            
            time.sleep(0.1)
        
        offset = {"last_timestamp": latest_timestamp} if latest_timestamp > 0 else {}
        return iter(records), offset

    def _read_markets_status(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[Dict], Dict]:
        """Read market status information."""
        date = table_options.get("date", datetime.now().strftime("%Y-%m-%d"))
        
        records = []
        try:
            data = self._make_request("markets/status", {"date": date})
            
            if data.get("s") == "ok":
                dates = data.get("date", [date])
                statuses = data.get("status", [])
                
                for i in range(len(dates) if isinstance(dates, list) else 1):
                    status_record = {
                        "date": dates[i] if isinstance(dates, list) and i < len(dates) else dates,
                        "status": statuses[i] if isinstance(statuses, list) and i < len(statuses) else data.get("status"),
                        "open_time": None,
                        "close_time": None,
                        "is_open": data.get("status") == "open",
                        "next_open": None,
                        "next_close": None,
                    }
                    records.append(status_record)
        except Exception as e:
            print(f"Error fetching market status: {e}")
        
        return iter(records), {}

    def _safe_float(self, value: Any) -> float:
        """Safely extract a float value from API response."""
        if isinstance(value, list):
            value = value[0] if value else None
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> int:
        """Safely extract an int value from API response."""
        if isinstance(value, list):
            value = value[0] if value else None
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def test_connection(self) -> dict:
        """
        Test the connection to MarketData.app API.

        Returns:
            Dictionary with status and message
        """
        try:
            # Test with a simple quote request
            response = self._session.get(
                f"{self.base_url}/stocks/quotes/AAPL",
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("s") == "ok":
                    return {"status": "success", "message": "Connection successful"}
                else:
                    return {"status": "error", "message": f"API returned: {data.get('s')}"}
            else:
                return {
                    "status": "error",
                    "message": f"API error: {response.status_code} {response.text}",
                }
        except Exception as e:
            return {"status": "error", "message": f"Connection failed: {str(e)}"}

