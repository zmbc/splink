from __future__ import annotations

from functools import wraps

from .dialect_base_classes import _dialect_base_factory

from ..blocking_rules_library import (
    exact_match_rule,
    _block_on
)


def auto_docstring_generator(docstring):
    def decorator(func):
        func.__doc__ = docstring
        return func
    return decorator


class _blocking_rule_library_factory:

    def __init__(self, dialect):
        self.base_class = _dialect_base_factory(dialect)
        self.dialect = dialect

        # Define our exact match rule class
        self._exact_match_rule = type(
            "ExactMatchRule",
            (self.base_class, exact_match_rule),
            {},
        )

    @property
    def exact_match_rule(self):
        return self._exact_match_rule

    def block_on(self, col_names: list[str], salting_partitions: int = 1):
        """The `block_on` function generates blocking rules that facilitate
            efficient equi-joins based on the columns or SQL statements
            specified in the col_names argument. When multiple columns or
            SQL snippets are provided, the function generates a compound
            blocking rule, connecting individual match conditions with
            "AND" clauses.

            This function is designed for scenarios where you aim to achieve
            efficient yet straightforward blocking conditions based on one
            or more columns or SQL snippets.

            For more information on the intended use cases of `block_on`, please see
            [the following discussion](https://github.com/moj-analytical-services/splink/issues/1376).

            Further information on equi-join conditions can be found
            [here](https://moj-analytical-services.github.io/splink/topic_guides/blocking/performance.html)

            This function acts as a shorthand alias for the `brl.and_` syntax:
            ```py
            import splink.duckdb.blocking_rule_library as brl
            brl.and_(brl.exact_match_rule, brl.exact_match_rule, ...)
            ```

            Args:
                col_names (list[str]): A list of input columns or sql conditions
                    you wish to create blocks on.
                salting_partitions (optional, int): Whether to add salting
                    to the blocking rule. More information on salting can
                    be found within the docs. Salting is only valid for Spark.

            Examples:
                === ":simple-duckdb: DuckDB"
                    ``` python
                    from splink.duckdb.blocking_rule_library import block_on
                    block_on("first_name")  # check for exact matches on first name
                    sql = "substr(surname,1,2)"
                    block_on([sql, "surname"])
                    ```
                === ":simple-apachespark: Spark"
                    ``` python
                    from splink.spark.blocking_rule_library import block_on
                    block_on("first_name")  # check for exact matches on first name
                    sql = "substr(surname,1,2)"
                    block_on([sql, "surname"], salting_partitions=1)
                    ```
                === ":simple-amazonaws: Athena"
                    ``` python
                    from splink.athena.blocking_rule_library import block_on
                    block_on("first_name")  # check for exact matches on first name
                    sql = "substr(surname,1,2)"
                    block_on([sql, "surname"])
                    ```
                === ":simple-sqlite: SQLite"
                    ``` python
                    from splink.sqlite.blocking_rule_library import block_on
                    block_on("first_name")  # check for exact matches on first name
                    sql = "substr(surname,1,2)"
                    block_on([sql, "surname"])
                    ```
                === "PostgreSQL"
                    ``` python
                    from splink.postgres.blocking_rule_library import block_on
                    block_on("first_name")  # check for exact matches on first name
                    sql = "substr(surname,1,2)"
                    block_on([sql, "surname"])
                    ```
            """  # noqa: E501

        return _block_on(
            self.exact_match_rule,
            col_names=col_names,
            salting_partitions=salting_partitions
        )

# Copy the docstring from _block_on to block_on
# _blocking_rule_library_factory.block_on.__doc__ = _block_on.__doc__
