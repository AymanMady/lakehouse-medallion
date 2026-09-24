"""
Data quality framework (phase 6).

The principle the whole layer rests on: an invalid row is NEVER deleted
silently. It is counted per rule, isolated in the quarantine with its reason,
and reported. A `.filter()` that makes 12,000 rows disappear without a word is
the most efficient way to make a gap unexplainable three months later.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

ERROR_COL = "_dq_errors"


@dataclass(frozen=True)
class Rule:
    """A named condition that must be TRUE for the row to be valid."""
    name: str
    condition: Column
    description: str = ""


def _safe(condition: Column) -> Column:
    """NULL -> False.

    In SQL, `NULL > 0` is not False, it is NULL. Without this coalesce, a
    missing quantity would trigger NO rule at all and would pass as valid.
    It is the single most common way broken data reaches a dashboard.
    """
    return F.coalesce(condition, F.lit(False))


def apply_rules(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Add ERROR_COL: the array of violated rules, empty when the row is valid.

    One pass over the data, whatever the number of rules. Evaluating them one
    at a time would mean as many scans as there are rules.
    """
    flags = [
        F.when(~_safe(rule.condition), F.lit(rule.name)).otherwise(F.lit(None))
        for rule in rules
    ]
    return df.withColumn(ERROR_COL, F.array_compact(F.array(*flags)))


def split_valid_invalid(annotated: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(valid, invalid). The technical column does not follow the valid rows."""
    valid = annotated.filter(F.size(ERROR_COL) == 0).drop(ERROR_COL)
    invalid = annotated.filter(F.size(ERROR_COL) > 0)
    return valid, invalid


def count_failures(annotated: DataFrame, rules: list[Rule]) -> dict[str, int]:
    """How many rows each rule rejected.

    Note the totals do not add up to the number of invalid rows: one row can
    violate several rules. That is deliberate - "how many rows do we lose?"
    and "what is the dominant problem?" are different questions.
    """
    counts = {rule.name: 0 for rule in rules}
    rows = (annotated.select(F.explode(ERROR_COL).alias("rule"))
            .groupBy("rule").count().collect())
    for row in rows:
        counts[row["rule"]] = row["count"]
    return counts


@dataclass
class QualityReport:
    dataset: str
    rows_read: int = 0
    rows_valid: int = 0
    rows_invalid: int = 0
    rows_duplicated: int = 0
    failures: dict[str, int] = field(default_factory=dict)

    @property
    def valid_ratio(self) -> float:
        return (self.rows_valid / self.rows_read) if self.rows_read else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "rows_read": self.rows_read,
            "rows_valid": self.rows_valid,
            "rows_invalid": self.rows_invalid,
            "rows_duplicated": self.rows_duplicated,
            "valid_ratio": round(self.valid_ratio, 6),
            "failures_by_rule": self.failures,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


def log_report(log, report: QualityReport) -> None:
    log.info(f"  {report.dataset:<14} read {report.rows_read:>7,} | "
             f"valid {report.rows_valid:>7,} ({report.valid_ratio:.2%}) | "
             f"quarantined {report.rows_invalid:>6,} | "
             f"duplicates removed {report.rows_duplicated:>5,}")
    for name, count in sorted(report.failures.items(), key=lambda kv: -kv[1]):
        if count:
            share = count / report.rows_read if report.rows_read else 0
            log.info(f"      {name:<34} {count:>7,}  ({share:.2%})")


def assert_quality(report: QualityReport, minimum: float, log) -> None:
    """Stop the pipeline if quality collapses.

    A job that always succeeds is not a reliable job, it is a job that checks
    nothing. Better a loud failure than a dashboard showing a wrong number.
    """
    if report.valid_ratio < minimum:
        message = (f"{report.dataset}: valid ratio {report.valid_ratio:.2%} "
                   f"< threshold {minimum:.2%} -> pipeline stopped")
        log.error(f"  {message}")
        raise ValueError(message)
