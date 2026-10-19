"""Parquet paths and atomic file operations for one run."""
from dataclasses import dataclass
from datetime import date
import glob
import json
import os
import tempfile

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from utils.config import INGRESS_TABLES, ZONE_CODES


@dataclass(frozen=True)
class LakeStorage:
    obs_dir: str
    year: str
    month: str
    day: str
    run_hhmmss: str = "000000"

    def hive_dir(self, zone, table):
        path = f"{self.obs_dir}/{zone}/{table}/year={self.year}/month={self.month}/day={self.day}"
        os.makedirs(path, exist_ok=True)
        return path


    def parquet_path(self, zone, source, table):
        filename = f"{ZONE_CODES[zone]}.{source}.{table}.{self.run_hhmmss}.parquet"
        return f"{self.hive_dir(zone, table)}/{filename}"


    def integration_path(self, table):
        return self.parquet_path("integration", "conformed", table)


    def read_ingress(self, source, table):
        raw_table = INGRESS_TABLES[source][table]
        path = self.latest_parquet("ingress", source, raw_table)
        con = duckdb.connect()
        try:
            return con.execute(f"SELECT * FROM read_parquet('{path}', hive_partitioning=false)").df()
        finally:
            con.close()


    def latest_parquet(self, zone, source, table):
        current = self.parquet_path(zone, source, table)
        if os.path.exists(current):
            return current
        pattern = f"{self.obs_dir}/{zone}/{table}/year={self.year}/month={self.month}/day={self.day}/{ZONE_CODES[zone]}.{source}.{table}.*.parquet"
        files = sorted(glob.glob(pattern))
        if not files:
            raise FileNotFoundError(f"No {zone} file for {source}.{table}")
        return files[-1]


    def write_parquet(self, dataframe, path, quality_summary=None, quality_context=None, relationship_summary=None):
        table = pa.Table.from_pandas(dataframe, preserve_index=False)
        for column in ("date_id", "order_date", "hire_date"):
            if column not in dataframe.columns:
                continue
            values = dataframe[column].dropna()
            if pd.api.types.is_datetime64_any_dtype(dataframe[column]) or (
                not values.empty and isinstance(values.iloc[0], date)
            ):
                dates = pd.to_datetime(dataframe[column], format="mixed", errors="coerce").dt.date
                table = table.set_column(
                    table.schema.get_field_index(column), column, pa.array(dates, type=pa.date32())
                )
        if quality_summary is not None:
            metadata = dict(table.schema.metadata or {})
            metadata[b"quality_summary"] = json.dumps(quality_summary, ensure_ascii=False).encode("utf-8")
            if quality_context is not None:
                metadata[b"quality_context"] = json.dumps(quality_context, ensure_ascii=False).encode("utf-8")
            if relationship_summary is not None:
                metadata[b"relationship_summary"] = json.dumps(relationship_summary, ensure_ascii=False).encode("utf-8")
            table = table.replace_schema_metadata(metadata)
        # Publish a complete file or leave the previous file intact.
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".parquet-", dir=directory)
        try:
            with os.fdopen(fd, "wb") as stream:
                pq.write_table(table, stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            directory_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
