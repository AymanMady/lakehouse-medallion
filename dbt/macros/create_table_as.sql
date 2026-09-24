{#
  Override of dbt-spark's spark__create_table_as.

  WHY THIS EXISTS. Three facts about this stack, each reproduced outside dbt
  with plain spark-sql:

    CREATE TABLE gold.t USING delta AS SELECT 1          -- works (virgin path)
    CREATE OR REPLACE TABLE gold.t USING delta AS ...    -- ALWAYS fails:
        AnalysisException: Table t does not support truncate in batch mode
    CREATE TABLE gold.t ... after DROP TABLE gold.t      -- fails:
        DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION

  The first failure is a Spark/Delta problem: Spark falls back to a
  non-atomic OverwriteByExpression instead of taking Delta's staged-replace
  path, and that fallback needs a TRUNCATE the table does not offer. The
  table really is Delta (DESCRIBE DETAIL confirms it), the extensions really
  are loaded, and the versions are the ones that go together (Spark 3.5.9,
  delta-spark 3.3.2).

  The second failure is an object-store problem: when S3A deletes the last
  object under a prefix it recreates a zero-byte "fake directory" marker, so
  the location is no longer virgin and Delta refuses to create a table there.
  DROP-then-CREATE therefore works exactly once per path.

  Consequence, and it drives the whole dbt configuration: the marts are
  materialised as INCREMENTAL with insert_overwrite (see dbt_project.yml).
  They are created once on a virgin path and then overwritten in place -
  never dropped, never replaced. INSERT OVERWRITE works fine.

  This macro only removes the `or replace`, so that the FIRST creation uses a
  statement this stack can actually run.

  LIMITATION, stated plainly: `dbt run --full-refresh` cannot work here. It
  would try to recreate a table whose path is no longer virgin. To rebuild
  from scratch, wipe the lake with scripts/reset_data.sh instead.
#}

{% macro spark__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- if language == 'sql' -%}
    {%- if temporary -%}
      {{ create_temporary_view(relation, compiled_code) }}
    {%- else -%}
      create table {{ relation }}
      {{ file_format_clause() }}
      {{ options_clause() }}
      {{ partition_cols(label="partitioned by") }}
      {{ clustered_cols(label="clustered by") }}
      {{ location_clause() }}
      {{ comment_clause() }}
      {{ tblproperties_clause() }}
      as
      {{ compiled_code }}
    {%- endif -%}
  {%- elif language == 'python' -%}
    {{ py_write_table(compiled_code=compiled_code, target_relation=relation) }}
  {%- endif -%}
{%- endmacro -%}
