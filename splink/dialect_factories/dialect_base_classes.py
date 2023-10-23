from .dialect_base import DialectBase

from .dialect_bases.athena_base import AthenaBase
from .dialect_bases.duckdb_base import DuckDBBase
from .dialect_bases.spark_base import SparkBase
from .dialect_bases.postgres_base import PostgresBase
from .dialect_bases.sqlite_base import SqliteBase


def _dialect_base_factory(dialect) -> DialectBase:
    """Constructs an exporter factory based on the user's preference."""

    factories = {
        "athena": AthenaBase,
        "duckdb": DuckDBBase,
        "postgres": PostgresBase,
        "spark": SparkBase,
        "sqlite": SqliteBase,
    }

    if dialect in factories:
        return factories[dialect]
    else:
        raise ValueError(
            "Dialect not recognised."
        )
