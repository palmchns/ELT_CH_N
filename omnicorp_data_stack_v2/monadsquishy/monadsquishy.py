# -*- coding: utf-8 -*-
"""
Monasquishy Core Engine - Monad-based Data Quality Framework
"""

import os
import json
import datetime
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

try:
    import altair as alt
except ImportError:
    alt = None

# --- Settings ---
try:
    from pandarallel import pandarallel
    pandarallel.initialize(progress_bar=True, verbose=0)
    HAS_PARALLEL = True
except ImportError:
    HAS_PARALLEL = False

def in_notebook():
    try:
        from IPython import get_ipython
        if 'IPKernelApp' not in get_ipython().config:
            return False
    except (ImportError, AttributeError):
        return False
    return True

if in_notebook() and alt is not None:
    alt.renderers.enable('default')

# --- Type Specifications ---
TYPE_SPECS = {
    "string":   {"default": pd.NA,  "dtype": pa.string(),        "coerce": lambda x: x.astype("string")},
    "date":     {"default": pd.NaT, "dtype": pa.date32(),        "coerce": lambda x: pd.to_datetime(x, errors="coerce").dt.date},
    "float":    {"default": np.nan, "dtype": pa.float64(),       "coerce": lambda x: pd.to_numeric(x, errors='coerce')},
    "datetime": {"default": pd.NaT, "dtype": pa.timestamp('ns'), "coerce": lambda x: pd.to_datetime(x, errors='coerce')},
    "int":      {"default": pd.NA,  "dtype": pa.int64(),         "coerce": lambda x: pd.to_numeric(x, errors='coerce').astype("Int64")},
}

# --- Decorators ---
def validator(f):
    """Mark function as a validator (checks data, raises on failure)."""
    f._role = 'validator'
    return f

def transformer(f):
    """Mark function as a transformer (converts data, raises on failure)."""
    f._role = 'transformer'
    return f

# --- Monad Core ---
class Monad:
    """
    Monad ที่ห่อหุ้มค่าเดี่ยว (single cell value) แล้ววิ่งผ่าน Pipeline ของ validators/transformers
    ใช้ operator | เพื่อ chain ฟังก์ชัน: Monad(value) | func1 | func2 | ...
    """
    __slots__ = ['value', 'input_row', 'output_column', 'status', 'final_value', 'logs', 'stopped', 'step']

    def __init__(self, value, row, col):
        self.value = value
        self.input_row = row
        self.output_column = col
        self.status = 'pending'
        self.final_value = None
        self.logs = []
        self.stopped = False
        self.step = 0

    def _log(self, func, status, details=None):
        role = getattr(func, '_role', 'validator')
        name = getattr(func, '__name__', 'unknown')
        self.logs.append({
            'row': self.input_row,
            'col': self.output_column,
            'step': self.step,
            'role_type': role,
            'role': name,
            'status': status,
            'value': str(self.value),
            'details': details
        })

    def __or__(self, func):
        if self.stopped:
            self.step += 1
            self._log(func, 'skipped')
            return self
        self.step += 1
        role = getattr(func, '_role', 'validator')
        try:
            res = func(self.value)
            if role == 'validator':
                self.value = res
                self.status = 'valid'
                self._log(func, 'passed')
            elif role == 'transformer':
                self.value = res
                self.status = 'success'
                self.final_value = res
                self.stopped = True
                self._log(func, 'passed')
        except Exception as e:
            if role == 'validator':
                self.status = 'dirty'
                self.final_value = None
                self.stopped = True
                self._log(func, 'failed', str(e))
            elif role == 'transformer':
                self._log(func, 'failed', str(e))
        return self

    def apply(self, pipeline):
        transformer_seen = False
        for func in pipeline:
            if getattr(func, '_role', 'validator') == 'transformer':
                transformer_seen = True
            self | func

        if self.status == 'success':
            pass
        elif self.status == 'dirty':
            self.final_value = None
        elif self.status in ['valid', 'pending']:
            if transformer_seen:
                self.status = 'dirty'
                self.final_value = None
                self.logs.append({
                    'row': self.input_row, 'col': self.output_column,
                    'step': self.step + 1, 'role_type': 'system',
                    'role': 'chain_exhausted', 'status': 'failed',
                    'value': str(self.value),
                    'details': 'All transformers failed'
                })
            else:
                self.status = 'success'
                self.final_value = self.value
        return self


# --- Squishy Engine ---
class SquishyEngine:
    """
    Engine หลักที่รับ config (list of column definitions) และ DataFrame
    แล้ววิ่ง Monad Pipeline ทุกแถว ทุกคอลัมน์ ให้อัตโนมัติ
    """
    def __init__(self, config_list, source_df):
        self.config = config_list
        self.df = source_df
        self.logs = []
        self.final_df = None

    def run(self):
        print(f"Processing {len(self.df)} rows...")
        final_df = self.df.copy()
        all_logs = []

        for col_def in self.config:
            target = col_def['target']
            source = col_def.get('source', target)
            pipeline = col_def['pipeline']
            spec = TYPE_SPECS.get(col_def.get('type', 'string'))
            if source not in self.df.columns:
                continue

            def process(row):
                m = Monad(row[source], row.name, target).apply(pipeline)
                return (m.final_value if m.status == 'success' else spec['default'], m.logs)

            results = None
            if HAS_PARALLEL:
                try:
                    results = self.df.parallel_apply(process, axis=1)
                except Exception:
                    print("Parallel apply failed. Falling back to standard apply...")
                    results = None

            if results is None:
                results = self.df.apply(process, axis=1)

            final_df[target] = results.apply(lambda x: x[0])
            for log_list in results.apply(lambda x: x[1]):
                all_logs.extend(log_list)

            final_df[target] = spec['coerce'](final_df[target])

        self.final_df = final_df
        self.logs = pd.DataFrame(all_logs)
        return final_df

    def run_batch(self):
        """Run supported rules by column batches and keep aggregated quality logs."""
        final_df = self.df.copy()
        summaries = []

        def add_summary(column, step, rule, role_type, status, count, values=None):
            if count:
                summary = {
                    "col": column,
                    "step": step,
                    "role": rule,
                    "role_type": role_type,
                    "status": status,
                    "count": int(count),
                    "top_failed_values": "",
                }
                if status == "failed" and values is not None:
                    summary["top_failed_values"] = ", ".join(
                        f"'{value}' ({count})"
                        for value, count in values.astype("string").fillna("<NULL>").value_counts().head(5).items()
                    )
                summaries.append(summary)

        for col_def in self.config:
            target = col_def["target"]
            source = col_def.get("source", target)
            series = self.df[source].copy()
            spec = TYPE_SPECS.get(col_def.get("type", "string"))

            for step, func in enumerate(col_def["pipeline"], start=1):
                name = getattr(func, "__name__", "unknown")
                role = getattr(func, "_role", "validator")
                # Validate the value produced by the preceding cleansing step.
                text = series.astype("string").str.strip()
                missing = series.isna() | text.str.lower().isin(
                    ["", "n/a", "null", "nan", "none", "<na>", "-", "ไม่ระบุ"]
                )

                if name == "must_exist":
                    series = series.mask(missing, pd.NA)
                    add_summary(target, step, name, role, "failed", missing.sum(), text[missing])
                    add_summary(target, step, name, role, "passed", (~missing).sum())
                elif name == "normalize_whitespace":
                    series = text.str.replace(r"\s+", " ", regex=True).str.strip().mask(missing, pd.NA)
                    add_summary(target, step, name, role, "failed", missing.sum(), text[missing])
                    add_summary(target, step, name, role, "passed", (~missing).sum())
                elif name == "normalize_optional_whitespace":
                    series = series.astype("string").str.replace(r"\s+", " ", regex=True).str.strip().mask(missing, pd.NA)
                    add_summary(target, step, name, role, "skipped", missing.sum())
                    add_summary(target, step, name, role, "passed", (~missing).sum())
                elif name == "transform_to_uppercase":
                    series = text.str.strip().str.upper().mask(missing, pd.NA)
                    add_summary(target, step, name, role, "skipped", missing.sum())
                    add_summary(target, step, name, role, "passed", (~missing).sum())
                elif name == "validate_contact_number":
                    valid = text.str.fullmatch(r"[+()0-9.\s/-]+", na=False) & text.str.replace(r"\D", "", regex=True).str.len().between(7, 15)
                    invalid = (~missing) & (~valid)
                    series = series.mask(invalid, pd.NA)
                    add_summary(target, step, name, role, "skipped", missing.sum())
                    add_summary(target, step, name, role, "failed", invalid.sum(), text[invalid])
                    add_summary(target, step, name, role, "passed", ((~missing) & valid).sum())
                elif name == "validate_email":
                    valid = text.str.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", na=False)
                    invalid = (~missing) & (~valid)
                    series = series.mask(invalid, pd.NA)
                    add_summary(target, step, name, role, "skipped", missing.sum())
                    add_summary(target, step, name, role, "failed", invalid.sum(), text[invalid])
                    add_summary(target, step, name, role, "passed", ((~missing) & valid).sum())
                elif name == "validate_optional_whole_number":
                    number = pd.to_numeric(series, errors="coerce")
                    valid = (~missing) & number.notna() & (number > 0) & (number % 1 == 0)
                    invalid = (~missing) & (~valid)
                    series = number.mask(invalid, pd.NA)
                    add_summary(target, step, name, role, "skipped", missing.sum())
                    add_summary(target, step, name, role, "failed", invalid.sum(), text[invalid])
                    add_summary(target, step, name, role, "passed", valid.sum())
                elif name == "validate_numeric_only":
                    valid = (~missing) & text.str.match(r"^-?\d+(\.\d+)?$", na=False)
                    series = series.mask(~valid, pd.NA)
                    add_summary(target, step, name, role, "failed", (~valid).sum(), text[~valid])
                    add_summary(target, step, name, role, "passed", valid.sum())
                elif name == "to_float":
                    converted = pd.to_numeric(series, errors="coerce")
                    failed = (~missing) & converted.isna()
                    series = converted
                    add_summary(target, step, name, role, "failed", failed.sum(), text[failed])
                    add_summary(target, step, name, role, "skipped", missing.sum())
                    add_summary(target, step, name, role, "passed", (~failed & converted.notna()).sum())
                elif name in {"validate_positive", "validate_non_negative", "validate_unit_interval"}:
                    number = pd.to_numeric(series, errors="coerce")
                    if name == "validate_positive":
                        valid = number > 0
                    elif name == "validate_non_negative":
                        valid = number >= 0
                    else:
                        valid = number.between(0, 1)
                    invalid = (~missing) & (~valid)
                    series = series.mask(invalid, pd.NA)
                    add_summary(target, step, name, role, "skipped", missing.sum())
                    add_summary(target, step, name, role, "failed", invalid.sum(), text[invalid])
                    add_summary(target, step, name, role, "passed", ((~missing) & valid).sum())
                elif name == "flexible_date_parser":
                    def parse(value):
                        try:
                            return func(value)
                        except (ValueError, TypeError, OverflowError):
                            return pd.NA

                    # Parse each distinct value once using the actual rule.
                    parsed = {value: parse(value) for value in text[~missing].unique()}
                    converted = text.map(parsed)
                    failed = (~missing) & converted.isna()
                    series = converted.mask(missing, pd.NA)
                    add_summary(target, step, name, role, "failed", failed.sum(), text[failed])
                    add_summary(target, step, name, role, "skipped", missing.sum())
                    add_summary(target, step, name, role, "passed", (~failed & series.notna()).sum())
                else:
                    series = series.map(lambda value: func(value) if pd.notna(value) else pd.NA)

            final_df[target] = spec["coerce"](series)

        self.final_df = final_df
        self.logs = pd.DataFrame(summaries)
        return final_df


# --- Helpers ---
def _truncate(val, max_len=40):
    s = str(val)
    return s if len(s) <= max_len else s[:max_len] + "..."


# --- DataLake Export (Embed quality summary in Parquet Footer) ---
def export_to_datalake(engine, base_dir="datalake/", run_date=None):
    """Export final data with embedded quality metadata in Parquet footer."""
    if engine.final_df is None:
        print("No data to save.")
        return

    os.makedirs(base_dir, exist_ok=True)

    current_run_date = run_date or datetime.datetime.now().strftime('%Y-%m-%d')

    export_data = engine.final_df.copy()
    export_data['run_date'] = current_run_date

    # Aggregate detailed logs into a lightweight JSON summary
    if not engine.logs.empty:
        summary_df = engine.logs.groupby(
            ['col', 'step', 'role', 'role_type', 'status']
        ).size().reset_index(name='count')

        failed_logs = engine.logs[engine.logs['status'] == 'failed']
        top_failed_list = []

        for keys, group in failed_logs.groupby(['col', 'step', 'role', 'role_type', 'status']):
            top_vals = group['value'].value_counts().head(5)
            top_str = ", ".join([f"'{_truncate(v)}' ({c})" for v, c in top_vals.items()])
            top_failed_list.append({
                'col': keys[0], 'step': keys[1], 'role': keys[2],
                'role_type': keys[3], 'status': keys[4],
                'top_failed_values': top_str
            })

        if top_failed_list:
            top_failed_df = pd.DataFrame(top_failed_list)
            summary_df = summary_df.merge(
                top_failed_df,
                on=['col', 'step', 'role', 'role_type', 'status'],
                how='left'
            )
        else:
            summary_df['top_failed_values'] = ""

        summary_df['top_failed_values'] = summary_df['top_failed_values'].fillna('')
        summary_df['run_date'] = current_run_date
        summary_json = summary_df.to_json(orient='records')
    else:
        summary_json = "[]"

    # Convert to PyArrow Table to inject metadata into the schema/footer
    table = pa.Table.from_pandas(export_data)
    custom_metadata = table.schema.metadata or {}
    custom_metadata[b'quality_summary'] = summary_json.encode('utf-8')
    table = table.replace_schema_metadata(custom_metadata)

    print(f"[DE] Saving Clean Data (with embedded Quality Summary) for {current_run_date}...")
    pq.write_to_dataset(
        table,
        root_path=os.path.join(base_dir, "clean_data"),
        partition_cols=['run_date']
    )
    print(f"[DE] Data successfully written to {base_dir}/clean_data")


# --- QA Visualization ---
def qa_load_and_visualize(datalake_dir="datalake/"):
    """QA Engineer Task: Extract metadata from Parquet footers and generate an interactive dashboard."""
    if alt is None:
        print("[QA] Cannot generate dashboard. Altair is missing.")
        return

    data_path = os.path.join(datalake_dir, "clean_data")
    if not os.path.exists(data_path):
        print(f"[QA] No data found in {data_path}")
        return

    print("\n[QA] Scanning Parquet file footers for embedded Quality Summaries...")
    all_summaries = []
    seen_summaries = set()

    for root, dirs, files in os.walk(data_path):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                schema = pq.read_schema(file_path)

                if schema.metadata and b'quality_summary' in schema.metadata:
                    summary_json = schema.metadata[b'quality_summary'].decode('utf-8')
                    if summary_json not in seen_summaries:
                        seen_summaries.add(summary_json)
                        all_summaries.extend(json.loads(summary_json))

    if not all_summaries:
        print("[QA] No quality summary metadata found in Parquet footers.")
        return

    print("[QA] Generating Interactive Vega-Lite Dashboard...")

    df_summary = pd.DataFrame(all_summaries).drop_duplicates()

    if 'top_failed_values' not in df_summary.columns:
        df_summary['top_failed_values'] = ""

    run_dates = [None] + list(df_summary['run_date'].dropna().unique())
    labels = ['All Partitions'] + list(df_summary['run_date'].dropna().unique())

    partition_dropdown = alt.binding_select(
        options=run_dates, labels=labels,
        name="Filter by Run Date: "
    )
    partition_selection = alt.selection_point(fields=['run_date'], bind=partition_dropdown)

    chart_chain = alt.Chart(
        df_summary, title="Pipeline Operation Flow (Pass/Fail/Skip)"
    ).mark_bar().encode(
        x=alt.X('count:Q', stack='normalize'),
        y=alt.Y('role:N', sort=alt.EncodingSortField(field="step", order="ascending"), title="Pipeline Step"),
        color=alt.Color('status:N', scale=alt.Scale(
            domain=['passed', 'failed', 'skipped'],
            range=['#2ca02c', '#d62728', '#c7c7c7']
        )),
        row=alt.Row('col:N', header=alt.Header(titleOrient="top", labelOrient="top")),
        tooltip=['col', 'role', 'status', 'count',
                 alt.Tooltip('top_failed_values:N', title='Top Failed Values')]
    ).add_params(partition_selection).transform_filter(
        partition_selection
    ).properties(width=600, height=100).resolve_scale(y='independent')

    df_stats = df_summary[
        df_summary['role_type'] == 'transformer'
    ].copy() if 'role_type' in df_summary.columns else pd.DataFrame()

    chart_rec = alt.Chart(
        df_stats, title="AI Recommender (Transformer Pass Volume)"
    ).mark_bar().encode(
        x=alt.X('sum(count):Q', title="Total Passed"),
        y=alt.Y('role:N', sort='-x'),
        color=alt.Color('col:N'),
        tooltip=['col', 'role', 'sum(count)']
    ).transform_filter(
        alt.datum.status == 'passed'
    ).add_params(partition_selection).transform_filter(
        partition_selection
    ).properties(width=600, height=150)

    final_dashboard = alt.vconcat(chart_chain, chart_rec).resolve_scale(color='independent')

    if in_notebook():
        from IPython.display import display
        display(final_dashboard)
    else:
        out_file = 'qa_dashboard.html'
        final_dashboard.save(out_file)
        print(f"[QA] Dashboard exported locally to {out_file} (Open in your browser)")
