# Trading Analytics Example

A trading analytics system built with DuckDB/MotherDuck and dbt for tracking and analyzing trading performance across paper and live accounts.

## Features

- **Trade Execution Tracking**: Logs orders, executions, stop orders, and account snapshots
- **Performance Analytics**: Calculates risk/reward ratios, win rates, return percentages, and more
- **Time-based Aggregations**: Daily, weekly, and monthly performance summaries
- **Multi-Account Support**: Track both paper and live trading accounts
- **Cloud-Native**: Built on MotherDuck (cloud DuckDB) for scalable analytics

## Tech Stack

- **Database**: DuckDB / MotherDuck
- **Data Transformation**: dbt-core with dbt-duckdb adapter
- **Data Processing**: Polars with PyArrow backend
- **Package Management**: uv

## Installation

```bash
# Install dependencies
uv sync

# Set up environment variables
cp .env.example .env
# Add your MOTHERDUCK_TOKEN to .env
```

## Usage

```bash
# Load sample data and create database schema
uv run trading-analytics

# Run dbt transformations
cd src/trading_analytics/trading_analytics_example_dbt
dbt run
```

## Project Structure

```
src/trading_analytics/
├── db/              # Database connector and schema setup
├── data/            # Sample data and utilities
└── trading_analytics_example_dbt/
    └── models/      # dbt analytical models
        ├── intermediate/   # Trade execution transformations
        ├── trades_view.sql # Core trade metrics
        ├── daily_performance.sql
        ├── weekly_performance.sql
        └── monthly_performance.sql
```

## Key Metrics Calculated

- **Risk/Reward Ratio**: Exit vs entry price relative to stop distance
- **Risk per Trade**: Position size relative to account equity
- **Return %**: Portfolio return percentage per trade
- **Win Rate (Accuracy)**: Percentage of profitable trades
- **Avg Win/Loss**: Average risk/reward for winning vs losing trades
- **Duration**: Days held per trade

## Data Model

### Core Tables
- `accounts`: Trading account information (paper/live, currency)
- `executions`: Order fills with price, quantity, timestamps
- `stop_orders`: Stop loss orders linked to trades
- `account_snapshots`: Daily equity snapshots

### Analytical Views
- `trades_view`: Complete trade metrics and calculations
- `daily_performance`, `weekly_performance`, `monthly_performance`: Time-aggregated stats
- `avg_performance_day`, `avg_performance_week`, `avg_performance_month`: Average performance by time period

## License

This project is licensed under the MIT License.
