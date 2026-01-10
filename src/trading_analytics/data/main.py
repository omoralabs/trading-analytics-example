from trading_analytics.data.utils import get_df_from_json
from trading_analytics.db.db import DuckDB


def get_dict_with_jsons() -> dict:
    return {
        "accounts": "src/trading_analytics/data/sample/accounts.json",
        "account_snapshots": "src/trading_analytics/data/sample/account_snapshots.json",
        "executions": "src/trading_analytics/data/sample/executions.json",
        "stop_orders": "src/trading_analytics/data/sample/stop_orders.json",
    }


def create_db_and_insert_sample_data() -> None:
    sample_data = get_dict_with_jsons()

    accounts_df = get_df_from_json(sample_data["accounts"])
    account_snapshots_df = get_df_from_json(sample_data["account_snapshots"])
    executions_df = get_df_from_json(sample_data["executions"])
    stop_orders_df = get_df_from_json(sample_data["stop_orders"])

    with DuckDB() as db:
        print("Inserting sample data...")
        db.log_account_info(accounts_df)
        db.log_account_snapshots(account_snapshots_df)
        db.log_executions(executions_df)
        db.log_stop_orders(stop_orders_df)
        print("Sample data inserted!")
