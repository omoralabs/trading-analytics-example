import json

import polars as pl


def get_json(path: str) -> dict:
    with open(f"{path}") as f:
        return json.load(f)


def get_df_from_json(path: str) -> pl.DataFrame:
    json_file = get_json(path)
    return pl.DataFrame(json_file)
