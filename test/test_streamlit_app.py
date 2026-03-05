import datetime
import importlib.util
from pathlib import Path

import pytest


class FakeCursor:
    def __init__(self, *, fail_minute_metric_query=False, include_last_second_data=False):
        self.fail_minute_metric_query = fail_minute_metric_query
        self.include_last_second_data = include_last_second_data
        self.last_query = ""

    def execute(self, query, params=None):
        normalized = " ".join(str(query).lower().split())
        self.last_query = normalized

        if self.fail_minute_metric_query and normalized.startswith(
            "select metric, sensor_id, min(value) as min_v, max(value) as max_v"
        ):
            raise RuntimeError("simulated metric query failure")

        return self

    def fetchall(self):
        query = self.last_query

        if "from sys.tables where name like 'dim_%'" in query:
            return []
        if "select status, count(*) from gold_central group by status" in query:
            return []
        if "select distinct sensor_id, metric from gold_central" in query:
            return []
        if query.startswith(
            "select metric, sensor_id, min(value) as min_v, max(value) as max_v"
        ):
            return [("temperature", "sensor-1", 10.0, 13.5)]
        if query.startswith(
            "select ts, sensor_id, value from gold_central where metric = ?"
        ):
            return [(datetime.datetime(2026, 1, 1, 0, 0, 0), "sensor-1", 10.0)]
        if query.startswith(
            "select metric, ts, sensor_id, value from gold_central where ts >= dateadd(second, -1, sysutcdatetime())"
        ):
            if not self.include_last_second_data:
                return []
            return [
                ("temperature", datetime.datetime(2026, 1, 1, 0, 0, 0), "sensor-1", 10.0),
                ("temperature", datetime.datetime(2026, 1, 1, 0, 0, 0), "sensor-2", 12.0),
                ("temperature", datetime.datetime(2026, 1, 1, 0, 0, 1), "sensor-1", 11.0),
            ]

        return []

    def fetchone(self):
        query = self.last_query

        if "information_schema.columns" in query:
            return None
        if "count(*)" in query:
            return (0,)
        if query.startswith("select top 1 pipeline_latency"):
            return None
        if query.startswith("select top 1 last_ts"):
            return None
        if query.startswith("select avg(value)"):
            return (None,)
        if query.startswith("select max("):
            return (None,)

        return None


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


@pytest.fixture(scope="module")
def streamlit_module():
    module_path = Path(__file__).resolve().parents[1] / "app" / "streamlit_app.py"
    spec = importlib.util.spec_from_file_location("streamlit_app_under_test", module_path)
    assert spec is not None and spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(autouse=True)
def clear_fetch_cache(streamlit_module):
    clear = getattr(streamlit_module.fetch_metrics, "clear", None)
    if callable(clear):
        clear()


def test_fetch_metrics_handles_metric_query_failure(streamlit_module, monkeypatch):
    conn = FakeConnection(FakeCursor(fail_minute_metric_query=True))
    monkeypatch.setattr(streamlit_module, "get_connection", lambda: conn)

    result = streamlit_module.fetch_metrics(reload_counter=1)

    assert result["metric_series"] == {}
    assert result["metric_variation"].empty
    assert result["metric_series_1s"] == {}
    assert result["metric_variation_1s"] == {}
    assert conn.closed is True


def test_fetch_metrics_builds_last_second_variation(streamlit_module, monkeypatch):
    conn = FakeConnection(FakeCursor(include_last_second_data=True))
    monkeypatch.setattr(streamlit_module, "get_connection", lambda: conn)

    result = streamlit_module.fetch_metrics(reload_counter=2)

    assert "temperature" in result["metric_series_1s"]
    assert "temperature" in result["metric_variation_1s"]

    variation_df = result["metric_variation_1s"]["temperature"]
    assert list(variation_df.columns) == ["ts", "variation"]
    assert variation_df["variation"].max() == pytest.approx(2.0)
