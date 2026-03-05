import datetime

import pytest

from medallion.gold.dim import _parse_ts, ensure_dim_table
from medallion.gold.transform_gold import evaluate_status
from medallion.silver.silver import normalize_unit, parse_timestamp, parse_value


def test_normalize_unit_mappings():
    assert normalize_unit("thermostat") == "Celsius"
    assert normalize_unit("Celsius") == "Celsius"
    assert normalize_unit("c") == "Celsius"
    assert normalize_unit("humidity") == "Percentage"
    assert normalize_unit("percent") == "Percentage"
    assert normalize_unit("%") == "Percentage"
    assert normalize_unit("Volt") == "Volt"
    assert normalize_unit(None) is None


def test_parse_value_cases():
    assert parse_value(None) == (None, "missing")
    assert parse_value("12.4") == (12.4, "ok")
    assert parse_value(7) == (7.0, "ok")

    value, status = parse_value("not-a-number")
    assert value is None
    assert status == "bad_type"


def test_parse_timestamp_cases():
    ts, status = parse_timestamp("2026-03-05 12:13:14")
    assert status == "ok"
    assert ts == datetime.datetime(2026, 3, 5, 12, 13, 14)

    ts_iso, status_iso = parse_timestamp("2026-03-05T12:13:14")
    assert status_iso == "ok"
    assert ts_iso == datetime.datetime(2026, 3, 5, 12, 13, 14)

    assert parse_timestamp(None) == (None, "missing")
    assert parse_timestamp("bad-date") == (None, "bad_ts")


def test_evaluate_status_thresholds_and_unknown():
    assert evaluate_status("vent", 14.9) == "idle"
    assert evaluate_status("vent", 15.0) == "ok"
    assert evaluate_status("vent", 30.0) == "ok"
    assert evaluate_status("vent", 30.1) == "warning"

    assert evaluate_status("humidifier", 19.0) == "idle"
    assert evaluate_status("thermometer", 31.0) == "warning"
    assert evaluate_status("unknown_metric", 20.0) == "unknown"
    assert evaluate_status("vent", None) == "unknown"


def test_dim_parse_ts_accepts_datetime_and_iso():
    now = datetime.datetime.now(datetime.UTC)
    assert _parse_ts(now) == now
    assert _parse_ts("2026-03-05T12:13:14") == datetime.datetime(2026, 3, 5, 12, 13, 14)
    assert _parse_ts(None) is None


def test_ensure_dim_table_requires_non_empty_metric():
    with pytest.raises(ValueError, match="metric must be non-empty"):
        ensure_dim_table(cursor=None, metric="")
