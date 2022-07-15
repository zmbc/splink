from splink.duckdb.duckdb_linker import DuckDBLinker
import pandas as pd
from time import time

from tests.basic_settings import get_settings_dict


def setup_linker(n):

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    for i in range(n):
        df = pd.concat([df, df], axis=0)

    df.unique_id = range(len(df))

    linker = DuckDBLinker(
        df,
        get_settings_dict(),
        output_schema="splink_in_duckdb",
        input_table_aliases="testing",
    )

    linker._initialise_df_concat_with_tf(materialise=True)

    ### Ignore this section...
    import logging
    from copy import deepcopy
    from typing import TYPE_CHECKING

    from splink.blocking import block_using_rules_sql
    from splink.comparison_vector_values import compute_comparison_vector_values_sql
    from splink.expectation_maximisation import compute_new_parameters_sql

    from splink.m_u_records_to_parameters import (
        m_u_records_to_lookup_dict,
        append_u_probability_to_comparison_level_trained_probabilities,
    )

    target_rows = 3e8

    def _num_target_rows_to_rows_to_sample(target_rows):
        sample_rows = 0.5 * ((8 * target_rows + 1) ** 0.5 + 1)
        return sample_rows

    original_settings_obj = linker._settings_obj

    training_linker = deepcopy(linker)

    training_linker._train_u_using_random_sample_mode = True

    settings_obj = training_linker._settings_obj
    settings_obj._retain_matching_columns = False
    settings_obj._retain_intermediate_calculation_columns = False
    settings_obj._training_mode = True
    for cc in settings_obj.comparisons:
        for cl in cc.comparison_levels:
            cl._level_dict["tf_adjustment_column"] = None

    sql = """
    select count(*) as count
    from __splink__df_concat_with_tf
    """
    dataframe = training_linker._sql_to_splink_dataframe(
        sql, "__splink__df_concat_count"
    )
    result = dataframe.as_record_dict()
    dataframe.drop_table_from_database()
    count_rows = result[0]["count"]

    if settings_obj._link_type in ["dedupe_only", "link_and_dedupe"]:
        sample_size = _num_target_rows_to_rows_to_sample(target_rows)
        proportion = sample_size / count_rows

    if settings_obj._link_type == "link_only":
        sample_size = target_rows**0.5
        proportion = sample_size / count_rows

    if proportion >= 1.0:
        proportion = 1.0

    if sample_size > count_rows:
        sample_size = count_rows

    sql = f"""
    select *
    from __splink__df_concat_with_tf
    {training_linker._random_sample_sql(proportion, sample_size)}
    """

    df_sample = training_linker._sql_to_splink_dataframe(
        sql,
        "__splink__df_concat_with_tf_sample",
        transpile=False,
    )

    settings_obj._blocking_rules_to_generate_predictions = []
    # Next sections are all
    df_concat_with_sample = df_sample.physical_name

    return linker, df_concat_with_sample


from benchmarking.sql_strings import (
    regular_query,
    without_where_sql,
    pandas_way,
)


def run_original(linker, df_concat_with_sample):
    linker._con.execute(regular_query(df_concat_with_sample))


def run_without_where(linker, df_concat_with_sample):
    linker._con.execute(without_where_sql(df_concat_with_sample))


def run_pandas_way(linker, df_concat_with_sample):
    data = linker._con.execute(pandas_way(df_concat_with_sample)).fetch_df()
    data = data[data.comparison_vector_value != -1]
    data["u_probability"] = data["u_probability"] / data.groupby("output_column_name")[
        "u_probability"
    ].transform("sum")


linker_3, df_concat_with_sample_3 = setup_linker(n=3)


def test_range_3_regular(benchmark):

    benchmark.pedantic(
        run_original,
        args=(
            linker_3,
            df_concat_with_sample_3,
        ),
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


def test_range_3_without_where(benchmark):

    benchmark.pedantic(
        run_without_where,
        args=(
            linker_3,
            df_concat_with_sample_3,
        ),
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


def test_range_3_pandas(benchmark):

    benchmark.pedantic(
        run_pandas_way,
        args=(
            linker_3,
            df_concat_with_sample_3,
        ),
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


# linker_5, df_concat_with_sample_5 = setup_linker(n=5)

# def test_range_5_regular(benchmark):

#     benchmark.pedantic(
#         run_original,
#         args=(linker_5, df_concat_with_sample_5,),
#         rounds=1,
#         iterations=1,
#     )


# def test_range_5_without_where(benchmark):

#     benchmark.pedantic(
#         run_without_where,
#         args=(linker_5, df_concat_with_sample_5,),
#         rounds=1,
#         iterations=1,
#     )


# def test_range_5_pandas(benchmark):

#     benchmark.pedantic(
#         run_pandas_way,
#         args=(linker_5, df_concat_with_sample_5,),
#         rounds=1,
#         iterations=1,
#     )


linker_7, df_concat_with_sample_7 = setup_linker(n=7)


def test_range_7_regular(benchmark):

    benchmark.pedantic(
        run_original,
        args=(
            linker_7,
            df_concat_with_sample_7,
        ),
        rounds=1,
        iterations=1,
    )


def test_range_7_without_where(benchmark):

    benchmark.pedantic(
        run_without_where,
        args=(
            linker_7,
            df_concat_with_sample_7,
        ),
        rounds=1,
        iterations=1,
    )


def test_range_7_pandas(benchmark):

    benchmark.pedantic(
        run_pandas_way,
        args=(
            linker_7,
            df_concat_with_sample_7,
        ),
        rounds=1,
        iterations=1,
    )
