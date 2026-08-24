"""
Unit tests for src/streaming/transformations.py.

Uses a local (non-streaming) Spark session and plain DataFrames -- the
transformation functions are pure DataFrame -> DataFrame functions with no
dependency on streaming APIs, so they can be tested exactly like batch code.
This is an intentional design benefit called out in transformations.py.
"""

from decimal import Decimal

import pytest
from pyspark.sql import Row, SparkSession

from src.streaming.transformations import (
    add_derived_fields,
    cast_and_clean_types,
    deduplicate_events,
    filter_invalid_records,
    transform_events,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("transformations-tests")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


def _raw_row(**overrides):
    base = dict(
        event_id="evt-1",
        user_id="10",
        product_id="5",
        event_type="purchase",
        event_timestamp="2026-01-01T10:00:00",
        product_name="Widget",
        quantity="2",
        price="19.99",
    )
    base.update(overrides)
    return Row(**base)


def test_cast_and_clean_types_converts_numeric_strings(spark):
    df = spark.createDataFrame([_raw_row()])
    result = cast_and_clean_types(df).collect()[0]
    assert result["user_id"] == 10
    assert result["product_id"] == 5
    assert result["quantity"] == 2
    # Exact equality, not a tolerance: price is a Decimal, so 19.99 is
    # represented precisely rather than approximated.
    assert result["price"] == Decimal("19.99")
    assert result["event_timestamp"] is not None


def test_cast_and_clean_types_null_on_bad_value(spark):
    df = spark.createDataFrame([_raw_row(price="not_a_number")])
    result = cast_and_clean_types(df).collect()[0]
    assert result["price"] is None


def test_filter_invalid_records_drops_unknown_event_type(spark):
    df = spark.createDataFrame([_raw_row(event_type="not_a_real_type")])
    cleaned = cast_and_clean_types(df)
    filtered = filter_invalid_records(cleaned)
    assert filtered.count() == 0


def test_filter_invalid_records_drops_negative_price(spark):
    df = spark.createDataFrame([_raw_row(price="-5.00")])
    cleaned = cast_and_clean_types(df)
    filtered = filter_invalid_records(cleaned)
    assert filtered.count() == 0


def test_filter_invalid_records_drops_purchase_with_zero_quantity(spark):
    df = spark.createDataFrame([_raw_row(event_type="purchase", quantity="0")])
    cleaned = cast_and_clean_types(df)
    filtered = filter_invalid_records(cleaned)
    assert filtered.count() == 0


def test_filter_invalid_records_keeps_valid_view_with_zero_quantity(spark):
    df = spark.createDataFrame([_raw_row(event_type="view", quantity="0")])
    cleaned = cast_and_clean_types(df)
    filtered = filter_invalid_records(cleaned)
    assert filtered.count() == 1


def test_deduplicate_events_removes_duplicate_ids(spark):
    df = spark.createDataFrame([_raw_row(), _raw_row()])
    deduped = deduplicate_events(df)
    assert deduped.count() == 1


def test_add_derived_fields_computes_total_amount_for_purchase(spark):
    df = spark.createDataFrame([_raw_row(quantity="3", price="10.00")])
    cleaned = cast_and_clean_types(df)
    derived = add_derived_fields(cleaned).collect()[0]
    assert derived["total_amount"] == Decimal("30.00")


def test_add_derived_fields_zero_total_amount_for_view(spark):
    df = spark.createDataFrame([_raw_row(event_type="view", quantity="0")])
    cleaned = cast_and_clean_types(df)
    derived = add_derived_fields(cleaned).collect()[0]
    assert derived["total_amount"] == Decimal("0.00")


def test_transform_events_end_to_end(spark):
    df = spark.createDataFrame(
        [
            _raw_row(event_id="evt-1"),
            _raw_row(event_id="evt-2", event_type="bad_type"),  # filtered out
            _raw_row(event_id="evt-1"),  # duplicate of evt-1
        ]
    )
    result = transform_events(df)
    assert result.count() == 1
    row = result.collect()[0]
    assert row["event_id"] == "evt-1"
    assert "total_amount" in result.columns
    assert "ingested_at" in result.columns
