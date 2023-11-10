from functools import wraps
from typing import Callable

from .dialects import SplinkDialect


def column_wrapper(modify_function: Callable):
    def decorator(cls):
        original_cols_l_r = cls.cols_l_r

        @wraps(original_cols_l_r)
        def new_cols_l_r(self, sql_dialect: str) -> Callable:
            col_l, col_r = original_cols_l_r(self, sql_dialect)
            return modify_function(self, col_l, col_r, sql_dialect)

        cls.cols_l_r = new_cols_l_r
        return cls

    return decorator

# Specific modification functions
def modify_for_regex_extract(
    instance: Callable,
    col_l: str,
    col_r: str,
    sql_dialect: SplinkDialect,
) -> [str, str]:

    cols = [col_l, col_r]
    if instance._regex_extract_str:
        regex_fn = sql_dialect.regex_extract
        regex = instance._regex_extract_str
        cols = [regex_fn.format(col=c, regex=regex) for c in cols]
    return cols


def modify_for_lowercase(
    instance: Callable,
    col_l: str,
    col_r: str,
    sql_dialect: SplinkDialect,
) -> [str, str]:

    if instance._set_to_lowercase:
        return f"lowercase({col_l})", f"lowercase({col_r})"
    return col_l, col_r


regex_extract_col = column_wrapper(modify_for_regex_extract)
lowercase_col = column_wrapper(modify_for_lowercase)
