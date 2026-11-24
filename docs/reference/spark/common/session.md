# `spark/common/session.py`

> SparkSession factory (phases 4 to 7).

Reference for the public API of `spark/common/session.py` (55 lines),
extracted from the source code: signatures and docstrings.

## Imports

- Third-party: `pyspark`
- Standard library: `logging`, `os`, `sys`

## Functions

### `get_logger`

```python
get_logger(stage: str) -> logging.Logger
```

Logger prefixed by the pipeline stage: [BRONZE], [SILVER], [GOLD]...

### `get_spark`

```python
get_spark(app_name: str, extra_conf: dict[str, str] | None=None) -> SparkSession
```
