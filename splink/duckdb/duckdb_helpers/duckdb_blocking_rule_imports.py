from __future__ import annotations

from ...dialect_factories.blocking_library_factories import _blocking_rule_library_factory

# We can also grab the dialect from the file path
_blocking_rule_functions = _blocking_rule_library_factory("duckdb")

exact_match_rule = _blocking_rule_functions.exact_match_rule
block_on = _blocking_rule_functions.block_on
