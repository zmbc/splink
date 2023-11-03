import time

import splink.duckdb.comparison_library as cl
from splink.datasets import splink_datasets
from splink.duckdb.linker import DuckDBLinker

settings = {
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        {"blocking_rule": "l.first_name = r.first_name", "salting_partitions": 8},
        {"blocking_rule": "l.surname = r.surname", "salting_partitions": 8},
    ],
    "comparisons": [
        cl.jaro_at_thresholds("first_name", term_frequency_adjustments=True),
        cl.jaro_at_thresholds("surname", term_frequency_adjustments=True),
        cl.exact_match("postcode_fake"),
        cl.exact_match("occupation"),
        cl.levenshtein_at_thresholds("dob", 2),
    ],
    "retain_intermediate_calculation_columns": True,
}

df = splink_datasets.historical_50k
df = df.head(20000)

# linker = DuckDBLinker(df, settings, optimize=True)
# c = linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(
#     "l.surname = r.surname"
# )
# print(f"{c:,.0f}")


def run_linker(optimize):
    start_time = time.time()

    linker = DuckDBLinker(df, settings, optimize=optimize)
    _ = linker.predict().as_pandas_dataframe()

    end_time = time.time()
    return end_time - start_time


# Calculate average time over 5 runs for optimize=True
times_optimize_true = [run_linker(True) for _ in range(2)]
average_time_optimize_true = sum(times_optimize_true) / len(times_optimize_true)

# Calculate average time over 5 runs for optimize=False
times_optimize_false = [run_linker(False) for _ in range(2)]
average_time_optimize_false = sum(times_optimize_false) / len(times_optimize_false)

print(
    f"Average execution time with optimize=True: {average_time_optimize_true} seconds"
)
print(
    f"Average execution time with optimize=False: {average_time_optimize_false} seconds"
)
