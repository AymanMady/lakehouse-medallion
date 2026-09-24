"""PHASE 10 - The data quality framework (spark/common/data_quality.py)."""

from __future__ import annotations

import logging

import pytest
from pyspark.sql import functions as F

from spark.common.data_quality import (
    ERROR_COL,
    QualityReport,
    Rule,
    apply_rules,
    assert_quality,
    count_failures,
    split_valid_invalid,
)


@pytest.fixture
def rules(spark):
    # A Column can only be built once a SparkContext exists, hence a fixture.
    return [
        Rule("quantity_not_positive", F.col("quantity") > 0),
        Rule("price_negative", F.col("price") >= 0),
    ]


@pytest.fixture
def rows(spark):
    return spark.createDataFrame(
        [(1, 2, 10.0), (2, 0, 10.0), (3, None, 10.0), (4, -1, -5.0)],
        "id INT, quantity INT, price DOUBLE",
    )


def _errors(df) -> dict[int, list[str]]:
    return {r["id"]: sorted(r[ERROR_COL]) for r in df.collect()}


def test_a_null_violates_the_rule_instead_of_slipping_through(rows, rules):
    """`NULL > 0` is NULL, not False. Without the coalesce, a missing quantity
    would trigger no rule at all and pass as valid."""
    assert _errors(apply_rules(rows, rules))[3] == ["quantity_not_positive"]


def test_every_violated_rule_is_listed_not_just_the_first(rows, rules):
    assert _errors(apply_rules(rows, rules))[4] == ["price_negative", "quantity_not_positive"]


def test_a_valid_row_carries_an_empty_error_list(rows, rules):
    assert _errors(apply_rules(rows, rules))[1] == []


def test_split_keeps_every_row_and_drops_the_technical_column(rows, rules):
    valid, invalid = split_valid_invalid(apply_rules(rows, rules))
    assert valid.count() + invalid.count() == rows.count()
    assert ERROR_COL not in valid.columns
    assert ERROR_COL in invalid.columns, "a reject must keep its reason"
    assert [r["id"] for r in valid.collect()] == [1]


def test_failures_are_counted_per_rule(rows, rules):
    counts = count_failures(apply_rules(rows, rules), rules)
    assert counts == {"quantity_not_positive": 3, "price_negative": 1}


def test_a_rule_that_never_fires_is_reported_with_zero(spark, rules):
    """Absent from the report and "never fired" must not look the same."""
    clean = spark.createDataFrame([(1, 1, 1.0)], "id INT, quantity INT, price DOUBLE")
    assert count_failures(apply_rules(clean, rules), rules) == {
        "quantity_not_positive": 0, "price_negative": 0}


def test_the_gate_stops_the_pipeline_below_the_threshold():
    log = logging.getLogger("test")
    report = QualityReport("orders", rows_read=100, rows_valid=79, rows_invalid=21)
    with pytest.raises(ValueError, match="orders"):
        assert_quality(report, 0.80, log)
    assert_quality(QualityReport("orders", rows_read=100, rows_valid=80, rows_invalid=20),
                   0.80, log)


def test_duplicates_do_not_drag_the_ratio_down():
    """Re-running the pipeline on the same data doubles what is read, not what
    is wrong: the ratio must not move."""
    first = QualityReport("orders", rows_read=1_000, rows_valid=960, rows_invalid=40)
    rerun = QualityReport("orders", rows_read=2_000, rows_valid=960, rows_invalid=40,
                          rows_duplicated=1_000)
    assert rerun.valid_ratio == first.valid_ratio == 0.96


def test_an_empty_dataset_has_a_zero_ratio_not_a_division_error():
    assert QualityReport("orders").valid_ratio == 0.0
