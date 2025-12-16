# Lakeflow MarketData Connector

The Lakeflow MarketData Connector allows you to extract financial market data from [MarketData.app](https://www.marketdata.app/) and load it into your data lake or warehouse. This connector supports real-time quotes, historical candle data, options chains, earnings data, and market status information.

## Set up

### Prerequisites
- Access to a MarketData.app account with API access
- MarketData.app API token

### Required Parameters

To configure the MarketData connector, you'll need to provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `api_token` | string | Yes | MarketData.app API token | `your_api_token_here` |
| `base_url` | string | No | Override for API base URL | `https://api.marketdata.app/v1` |

### Getting Your API Token

1. Sign up for an account at [MarketData.app](https://www.marketdata.app/)
2. Navigate to your account dashboard
3. Generate or copy your API token
4. **Important**: Keep your API token secure - never share it publicly

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:
1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.

The connection can also be created using the standard Unity Catalog API.

## Objects Supported

The MarketData connector supports the following tables/objects:

### Stock Data

#### stocks_quotes
- **Primary Keys**: `symbol`, `timestamp`
- **Ingestion Type**: Snapshot
- **Description**: Real-time stock quotes including bid/ask, last price, and 52-week high/low
- **Required Options**: `symbol` or `symbols` (comma-separated list)
- **Schema Fields**:
  - `symbol`: Stock ticker symbol
  - `timestamp`: Quote timestamp (Unix)
  - `ask`, `bid`, `mid`, `last`: Price fields
  - `ask_size`, `bid_size`: Quote sizes
  - `change`, `change_percent`: Price change
  - `high_52_week`, `low_52_week`: 52-week range
  - `volume`: Trading volume

#### stocks_candles
- **Primary Keys**: `symbol`, `timestamp`
- **Ingestion Type**: Append (incremental)
- **Cursor Field**: `timestamp`
- **Description**: Historical OHLCV (Open, High, Low, Close, Volume) candle data
- **Required Options**: `symbol` or `symbols`
- **Optional Options**:
  - `resolution`: Candle resolution (1, 5, 15, 30, 60, D, W, M). Default: `D`
  - `from_date`: Start date (YYYY-MM-DD)
  - `to_date`: End date (YYYY-MM-DD)
- **Schema Fields**:
  - `symbol`: Stock ticker symbol
  - `timestamp`: Candle timestamp (Unix)
  - `open`, `high`, `low`, `close`: OHLC prices
  - `volume`: Trading volume
  - `resolution`: Candle timeframe

#### stocks_earnings
- **Primary Keys**: `symbol`, `fiscal_year`, `fiscal_quarter`
- **Ingestion Type**: Append (incremental)
- **Cursor Field**: `report_date`
- **Description**: Quarterly earnings reports including EPS and revenue
- **Required Options**: `symbol` or `symbols`
- **Optional Options**:
  - `from_date`: Start date for earnings (YYYY-MM-DD)
  - `to_date`: End date for earnings (YYYY-MM-DD)
- **Schema Fields**:
  - `symbol`: Stock ticker symbol
  - `fiscal_year`, `fiscal_quarter`: Fiscal period
  - `report_date`, `report_time`: When earnings were reported
  - `reported_eps`, `estimated_eps`, `surprise_eps`: EPS data
  - `reported_revenue`, `estimated_revenue`: Revenue data

### Options Data

#### options_chain
- **Primary Keys**: `option_symbol`
- **Ingestion Type**: Snapshot
- **Description**: Full options chain with Greeks and pricing
- **Required Options**: `symbol` (underlying stock)
- **Optional Options**:
  - `expiration`: Specific expiration date (YYYY-MM-DD)
  - `side`: Filter by `call` or `put`
- **Schema Fields**:
  - `option_symbol`: Full option symbol
  - `underlying`: Underlying stock
  - `expiration`: Option expiration date
  - `side`: Call or Put
  - `strike`: Strike price
  - `dte`: Days to expiration
  - `ask`, `bid`, `mid`, `last`: Pricing
  - `open_interest`, `volume`: Market activity
  - `iv`: Implied volatility
  - `delta`, `gamma`, `theta`, `vega`, `rho`: Greeks

#### options_quotes
- **Primary Keys**: `option_symbol`, `timestamp`
- **Ingestion Type**: Snapshot
- **Description**: Real-time quotes for specific options
- **Required Options**: `option_symbol` (full option symbol like `AAPL250117C00150000`)
- **Schema Fields**:
  - `option_symbol`: Full option symbol
  - `timestamp`: Quote timestamp
  - `ask`, `bid`, `mid`, `last`: Pricing
  - `iv`, `delta`, `gamma`, `theta`, `vega`: Greeks

### Index Data

#### indices_quotes
- **Primary Keys**: `symbol`, `timestamp`
- **Ingestion Type**: Snapshot
- **Description**: Real-time index quotes (S&P 500, NASDAQ, etc.)
- **Required Options**: `symbol` or `symbols` (e.g., `SPX`, `NDX`, `DJI`)
- **Schema Fields**:
  - `symbol`: Index symbol
  - `timestamp`: Quote timestamp
  - `last`: Current value
  - `change`, `change_percent`: Value change
  - `high_52_week`, `low_52_week`: 52-week range

#### indices_candles
- **Primary Keys**: `symbol`, `timestamp`
- **Ingestion Type**: Append (incremental)
- **Cursor Field**: `timestamp`
- **Description**: Historical candle data for indices
- **Required Options**: `symbol` or `symbols`
- **Optional Options**:
  - `resolution`: Candle resolution (D, W, M)
  - `from_date`, `to_date`: Date range
- **Schema Fields**:
  - `symbol`: Index symbol
  - `timestamp`: Candle timestamp
  - `open`, `high`, `low`, `close`: OHLC values
  - `resolution`: Candle timeframe

### Market Status

#### markets_status
- **Primary Keys**: `date`
- **Ingestion Type**: Snapshot
- **Description**: Market open/close status information
- **Optional Options**:
  - `date`: Specific date to check (YYYY-MM-DD)
- **Schema Fields**:
  - `date`: Date
  - `status`: Market status
  - `is_open`: Boolean indicating if market is open

## Usage Examples

### Reading Stock Quotes

```python
options = {"api_token": "your_token"}
connector = LakeflowConnect(options)

table_options = {"symbols": "AAPL,MSFT,GOOGL"}
records, offset = connector.read_table("stocks_quotes", None, table_options)

for record in records:
    print(f"{record['symbol']}: ${record['last']}")
```

### Reading Historical Candles

```python
table_options = {
    "symbols": "AAPL",
    "resolution": "D",
    "from_date": "2024-01-01",
    "to_date": "2024-12-01"
}
records, offset = connector.read_table("stocks_candles", None, table_options)
```

### Reading Options Chain

```python
table_options = {
    "symbol": "AAPL",
    "side": "call",
    "expiration": "2025-01-17"
}
records, offset = connector.read_table("options_chain", None, table_options)
```

## Incremental Sync Strategy

### Append Tables
For tables with `append` ingestion type (`stocks_candles`, `stocks_earnings`, `indices_candles`):

1. **Initial Sync**: Fetches all data within the specified date range
2. **Subsequent Syncs**: Uses the returned offset (last timestamp) to fetch only new data
3. **Cursor Tracking**: Automatically tracks the latest timestamp for efficient incremental updates

### Snapshot Tables
For tables with `snapshot` ingestion type (`stocks_quotes`, `options_chain`, `indices_quotes`, `markets_status`):

1. Each sync fetches the current state of the data
2. No incremental tracking - always returns the latest data

## Rate Limiting

- The connector implements automatic rate limiting with 0.1-second delays between requests
- If rate limited (429 response), the connector waits 1 second before retrying
- MarketData.app's rate limits vary by subscription tier

## Data Types

| MarketData Type | Spark Type | Notes |
|-----------------|------------|-------|
| Symbol | StringType | Stock/option symbols |
| Timestamp | LongType | Unix timestamps |
| Price | DoubleType | Prices, percentages |
| Count | LongType | Volumes, sizes, counts |
| Boolean | BooleanType | Flags like `in_the_money` |

## Troubleshooting

### Common Issues

**Authentication Errors**:
- Verify API token is correct and properly formatted
- Ensure token has appropriate permissions for the data you're requesting
- Check if your subscription tier supports the requested data

**No Data Returned**:
- Verify the symbol format is correct (e.g., `AAPL` not `$AAPL`)
- Check if the market is open for real-time data
- Verify date ranges for historical data

**Rate Limiting**:
- Reduce sync frequency
- Implement longer delays between requests
- Consider upgrading your MarketData.app subscription

**Invalid Symbol**:
- Use standard ticker symbols without prefixes
- For options, use the full OCC option symbol format

### Error Handling
The connector includes built-in error handling for:
- API authentication errors
- Rate limiting (automatic retry)
- Network connectivity issues
- Invalid table names

## API Documentation

For complete MarketData.app API documentation, see:
- [MarketData.app API Documentation](https://www.marketdata.app/docs/api)
- [Stock Quotes](https://www.marketdata.app/docs/api/stocks/quotes)
- [Stock Candles](https://www.marketdata.app/docs/api/stocks/candles)
- [Options Chain](https://www.marketdata.app/docs/api/options/chain)
- [Indices](https://www.marketdata.app/docs/api/indices)

## Support

For issues or questions:
- Check MarketData.app API documentation
- Review connector logs in Databricks
- Verify API token permissions in MarketData.app dashboard

