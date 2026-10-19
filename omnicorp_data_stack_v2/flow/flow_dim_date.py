"""Raw date dimension in Landing; parsing and calendar derivation in Staging."""
import pandas as pd
from prefect import flow
from utils.quality import clean_dataframe, clean_table
from utils.schema import MODEL_COLUMNS


def build_landing(fact):
    """Preserve raw date values; calendar attributes are populated only in Staging."""
    result = pd.DataFrame({"date_id": fact["order_date"].drop_duplicates().reset_index(drop=True)})
    for column in MODEL_COLUMNS["dim_date"][1:]:
        result[column] = pd.NA
    return result


def stage_dataframe(dataframe):
    parsed, date_summary = clean_dataframe(dataframe[["date_id"]], "dim_date")
    dates = pd.to_datetime(parsed["date_id"], errors="coerce")
    result = pd.DataFrame({"date_id": dates.dt.date})
    for column, values in {
        "calendar_year": dates.dt.year,
        "calendar_month": dates.dt.month,
        "calendar_day": dates.dt.day,
        "quarter": dates.dt.quarter,
        "day_of_week": dates.dt.dayofweek,
        "day_name": dates.dt.day_name(),
        "month_name": dates.dt.month_name(),
    }.items():
        result[column] = values
    # Different raw date representations can legitimately identify the same day.
    result = result[~(result["date_id"].notna() & result["date_id"].duplicated())]
    result = result.sort_values("date_id", na_position="last").reset_index(drop=True)
    cleaned, summary = clean_dataframe(result, "dim_date")
    return cleaned, date_summary + [item for item in summary if item["col"] != "date_id"]


@flow(name="flow_dim_date", log_prints=True, persist_result=False)
def flow_dim_date(source, storage):
    return clean_table(source, "dim_date", storage, processor=stage_dataframe)
