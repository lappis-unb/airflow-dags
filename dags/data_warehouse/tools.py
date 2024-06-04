from datetime import datetime, timedelta

import pandas as pd


def add_temporal_columns(df: pd.DataFrame, execution_date: datetime) -> pd.DataFrame:
    """
    Adds temporal columns to the DataFrame based on the execution date.

    Args:
    ----
        df (pd.DataFrame): The original DataFrame without temporal columns.
        execution_date (datetime): The execution date to base the temporal columns on.

    Returns:
    -------
        pd.DataFrame: The DataFrame with added temporal columns.
    """
    event_day_id = int(execution_date.strftime("%Y%m%d"))
    available_day = execution_date + timedelta(days=1)
    available_day_id = int(available_day.strftime("%Y%m%d"))
    available_month_id = int(available_day.strftime("%Y%m"))
    available_year_id = int(available_day.strftime("%Y"))
    writing_day_id = int(datetime.now().strftime("%Y%m%d"))

    # Add the temporal columns to the DataFrame
    df["event_day_id"] = event_day_id
    df["available_day_id"] = available_day_id
    df["available_month_id"] = available_month_id
    df["available_year_id"] = available_year_id
    df["writing_day_id"] = writing_day_id

    return df
