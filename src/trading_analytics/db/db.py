import os

import duckdb
import polars as pl
from dotenv import load_dotenv

load_dotenv()


class DuckDB:
    def __init__(self):
        self._setup_motherduck_token()
        self.db_name = "trading_analytics"
        self.conn = duckdb.connect("md:")
        self.create_and_link_db()
        self.setup_schema()

    def __enter__(self):
        self.conn.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.conn.close()
        return False

    def _setup_motherduck_token(self) -> None:
        """Set up MotherDuck token if available."""
        token = os.getenv("MOTHERDUCK_TOKEN")
        if token:
            os.environ["motherduck_token"] = token

    def create_and_link_db(self) -> None:
        self.conn.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
        self.conn.execute(f"USE {self.db_name}")

    def setup_schema(self) -> None:
        print("Dropping tables in dependency order...")
        self.conn.execute("DROP TABLE IF EXISTS trades_stop_orders CASCADE;")
        self.conn.execute("DROP TABLE IF EXISTS trades_executions CASCADE;")
        self.conn.execute("DROP TABLE IF EXISTS trades CASCADE;")
        self.conn.execute("DROP TABLE IF EXISTS executions CASCADE;")
        self.conn.execute("DROP TABLE IF EXISTS stop_orders CASCADE;")
        self.conn.execute("DROP TABLE IF EXISTS account_snapshots CASCADE;")
        self.conn.execute("DROP TABLE IF EXISTS accounts CASCADE;")

        print("Dropping sequences...")
        self.conn.execute("DROP SEQUENCE IF EXISTS executions_seq;")
        self.conn.execute("DROP SEQUENCE IF EXISTS accounts_seq;")
        self.conn.execute("DROP SEQUENCE IF EXISTS stop_orders_seq;")

        print("Creating sequences...")
        self.conn.execute("CREATE SEQUENCE executions_seq START 1;")
        self.conn.execute("CREATE SEQUENCE accounts_seq START 1;")
        self.conn.execute("CREATE SEQUENCE stop_orders_seq START 1;")

        print("Creating tables...")

        # Accounts table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY DEFAULT nextval('accounts_seq'),
                account_number VARCHAR UNIQUE,
                currency VARCHAR,
                type VARCHAR CHECK (type IN ('paper', 'live'))
            );
        """)

        # Executions table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS executions (
                id INTEGER PRIMARY KEY DEFAULT nextval('executions_seq'),
                order_id VARCHAR NOT NULL,
                execution_id VARCHAR NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                filled_at TIMESTAMPTZ NOT NULL,
                filled_avg_price DOUBLE,
                filled_qty INTEGER,
                status VARCHAR NOT NULL,
                symbol VARCHAR NOT NULL,
                side VARCHAR NOT NULL,
                position_intent VARCHAR NOT NULL,
                account_id INTEGER NOT NULL,
                trade_id INTEGER NOT NULL,
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            );
        """)

        # Account snapshots table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS account_snapshots (
                account_id INTEGER,
                equity DOUBLE,
                date DATE,
                PRIMARY KEY (account_id, date),
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            );
        """)

        # Stop orders table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stop_orders (
                id INTEGER PRIMARY KEY DEFAULT nextval('stop_orders_seq'),
                created_at TIMESTAMP NOT NULL,
                stop_price DOUBLE NOT NULL,
                qty INTEGER NOT NULL,
                symbol VARCHAR NOT NULL,
                side VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                account_id INTEGER NOT NULL,
                trade_id INTEGER NOT NULL,
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            );
        """)

        print("Database setup complete!")

    def log_stop_orders(self, df: pl.DataFrame) -> None:
        """
        Add stop orders to the database.
        """

        self.conn.execute(
            """INSERT INTO stop_orders (
                created_at,
                stop_price,
                qty,
                symbol,
                side,
                type,
                account_id,
                trade_id
            )
            SELECT * FROM df
            """
        )

    def log_executions(self, df: pl.DataFrame) -> None:
        """
        Add executions to the database.
        """

        self.conn.execute(
            """INSERT INTO executions (
                order_id,
                execution_id,
                created_at,
                filled_at,
                filled_avg_price,
                filled_qty,
                status,
                symbol,
                side,
                position_intent,
                account_id,
                trade_id
            )
            SELECT * FROM df
            """
        )

    def log_account_info(self, df: pl.DataFrame) -> None:
        """
        Add account info to the database.
        """

        self.conn.execute(
            """
            INSERT INTO accounts (account_number, currency, type)
            SELECT account_number, currency, type
            FROM df
            ON CONFLICT (account_number) DO NOTHING
            """
        )

    def log_account_snapshots(self, df: pl.DataFrame) -> None:
        """
        Add account snapshots to the database.
        """

        self.conn.execute(
            """
            INSERT INTO account_snapshots (
                account_id,
                equity,
                date
            )
            SELECT account_id, equity, date
            FROM df
            ON CONFLICT (account_id, date) DO NOTHING
            """
        )
