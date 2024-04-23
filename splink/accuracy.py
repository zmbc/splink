from copy import deepcopy
from typing import TYPE_CHECKING

from .block_from_labels import block_from_labels
from .blocking import BlockingRule
from .comparison_vector_values import compute_comparison_vector_values_sql
from .misc import calculate_cartesian
from .pipeline import CTEPipeline
from .predict import predict_from_comparison_vectors_sqls_using_settings
from .sql_transform import move_l_r_table_prefix_to_column_suffix
from .vertically_concatenate import (
    compute_df_concat,
    compute_df_concat_with_tf,
    enqueue_df_concat,
)

if TYPE_CHECKING:
    from .linker import Linker


def truth_space_table_from_labels_with_predictions_sqls(
    threshold_actual=0.5,
    match_weight_round_to_nearest=None,
    true_positives_to_add: int = 0,
    positives_not_captured_by_blocking_rules_scored_as_zero=True,
):
    # Round to match_weight_round_to_nearest.
    # e.g. if it's 0.25, 1.27 gets rounded to 1.25
    if match_weight_round_to_nearest is not None:
        truth_thres_expr = f"""
            cast({match_weight_round_to_nearest} as float) *
            (round(match_weight/{match_weight_round_to_nearest}))
        """
    else:
        truth_thres_expr = "match_weight"

    sqls = []
    sql = f"""
    select
    *,
    {truth_thres_expr} as truth_threshold,
    case when clerical_match_score >= {threshold_actual} then 1
    else 0
    end
    as clerical_positive,
    case when clerical_match_score >= {threshold_actual} then 0
    else 1
    end
    as clerical_negative
    from __splink__labels_with_predictions
    order by match_weight
    """

    sql_info = {"sql": sql, "output_table_name": "__splink__labels_with_pos_neg"}
    sqls.append(sql_info)

    if positives_not_captured_by_blocking_rules_scored_as_zero:
        truth_threshold = """CASE
                            WHEN found_by_blocking_rules then truth_threshold
                            ELSE cast(-999 as float8)
                            END"""
    else:
        truth_threshold = "truth_threshold"
    sql = f"""
    select
        {truth_threshold} as truth_threshold,
        count(*) as num_records_in_row,
        sum(clerical_positive) as clerical_positive,
        sum(clerical_negative) as clerical_negative
    from
    __splink__labels_with_pos_neg
    group by {truth_threshold}
    order by {truth_threshold}
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__labels_with_pos_neg_grouped",
    }
    sqls.append(sql_info)

    sql = f"""
    select
    truth_threshold,

    (sum(clerical_positive) over (order by truth_threshold desc))  as cum_clerical_P,
    (sum(clerical_negative) over (order by truth_threshold))
        + cast({true_positives_to_add} as float8)
        - clerical_negative
        as cum_clerical_N,

    (select sum(clerical_positive) from __splink__labels_with_pos_neg_grouped)
        as total_clerical_P,
    (select sum(clerical_negative) from __splink__labels_with_pos_neg_grouped)
        + cast({true_positives_to_add} as float8)
        as total_clerical_N,

    (select sum(num_records_in_row) from __splink__labels_with_pos_neg_grouped)
        + cast({true_positives_to_add} as float8)
        as row_count,

    -num_records_in_row + sum(num_records_in_row) over (order by truth_threshold)
        + cast({true_positives_to_add} as float8)
        as N_labels,

    sum(num_records_in_row) over (order by truth_threshold desc) as P_labels
    from __splink__labels_with_pos_neg_grouped
    order by  truth_threshold
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__labels_with_pos_neg_grouped_with_stats",
    }
    sqls.append(sql_info)

    sql = """
    select
    truth_threshold,
    row_count,
    total_clerical_P as P,
    total_clerical_N as N,

    P_labels - cum_clerical_P as FP,
    cum_clerical_P as TP,

    N_labels - cum_clerical_N as FN,
    cum_clerical_N as TN

    from __splink__labels_with_pos_neg_grouped_with_stats
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__labels_with_pos_neg_grouped_with_truth_stats",
    }
    sqls.append(sql_info)

    sql = """
    select
        truth_threshold,
        power(2, truth_threshold) / (1 + power(2, truth_threshold))
            as match_probability,
        cast(row_count as float8) as row_count,
        cast(P as float8) as p,
        cast(N as float8) as n,
        cast(TP as float8) as tp,
        cast(TN as float8) as tn,
        cast(FP as float8) as fp,
        cast(FN as float8) as fn,
        cast(P/row_count as float8) as P_rate,
        cast(N as float)/row_count as N_rate,
        cast(TP as float8)/P as tp_rate,
        cast(TN as float8)/N as tn_rate,
        cast(FP as float8)/N as fp_rate,
        cast(FN as float8)/P as fn_rate,
        case when TP+FP=0 then 1 else cast(TP as float8)/(TP+FP) end as precision,
        cast(TP as float8)/P as recall,
        cast(TN as float8)/N as specificity,
        case when TN+FN=0 then 1 else cast(TN as float8)/(TN+FN) end as npv,
        cast(TP+TN as float8)/(P+N) as accuracy,
        cast(2.0*TP/(2*TP + FN + FP) as float8) as f1,
        cast(5.0*TP/(5*TP + 4*FN + FP) as float8) as f2,
        cast(1.25*TP/(1.25*TP + 0.25*FN + FP) as float8) as f0_5,
        cast(4.0*TP*TN/((4.0*TP*TN) + ((TP + TN)*(FP + FN))) as float8) as p4,
        case when TN+FN=0 or TP+FP=0 or P=0 or N=0 then 0
            else cast((TP*TN)-(FP*FN) as float8)/sqrt((TP+FP)*P*N*(TN+FN)) end as phi

    from __splink__labels_with_pos_neg_grouped_with_truth_stats
    """

    sql_info = {"sql": sql, "output_table_name": "__splink__truth_space_table"}
    sqls.append(sql_info)
    return sqls


def _select_found_by_blocking_rules(linker: "Linker"):
    brs = linker._settings_obj._blocking_rules_to_generate_predictions

    if brs:
        br_strings = [
            move_l_r_table_prefix_to_column_suffix(b.blocking_rule_sql) for b in brs
        ]
        wrapped_br_strings = [f"(coalesce({b}, false))" for b in br_strings]
        full_br_string = " OR ".join(wrapped_br_strings)
        br_col = f" ({full_br_string}) "
    else:
        br_col = " 1=1 "

    return f"{br_col} as found_by_blocking_rules"


def truth_space_table_from_labels_table(
    linker, labels_tablename, threshold_actual=0.5, match_weight_round_to_nearest=None
):
    pipeline = CTEPipeline()

    nodes_with_tf = compute_df_concat_with_tf(linker, pipeline)
    pipeline = CTEPipeline([nodes_with_tf])

    sqls = predictions_from_sample_of_pairwise_labels_sql(linker, labels_tablename)
    pipeline.enqueue_list_of_sqls(sqls)

    # c_P and c_N are clerical positive and negative, respectively
    sqls = truth_space_table_from_labels_with_predictions_sqls(
        threshold_actual, match_weight_round_to_nearest
    )
    pipeline.enqueue_list_of_sqls(sqls)

    df_truth_space_table = linker.db_api.sql_pipeline_to_splink_dataframe(pipeline)

    return df_truth_space_table


def truth_space_table_from_labels_column(
    linker: "Linker",
    label_colname,
    threshold_actual=0.5,
    match_weight_round_to_nearest=None,
    positives_not_captured_by_blocking_rules_scored_as_zero=True,
):

    # # First we need to calculate the number of implicit true negatives
    # # That is, any pair of records which have a different ID in the labels
    # # column are a negative
    # link_type = linker.settings_obj._link_type
    # if link_type == "dedupe_only":
    #     group_by_statement = ""
    # else:
    #     group_by_statement = "group by source_dataset"

    # concat = compute_df_concat(linker, pipeline)

    # pipeline = CTEPipeline([concat])

    # sql = f"""
    #     select count(*) as count
    #     from {concat.physical_name}
    #     {group_by_statement}
    # """

    # pipeline.enqueue_sql(sql, "__splink__cartesian_product")
    # cartesian_count = linker.db_api.sql_pipeline_to_splink_dataframe(pipeline)
    # row_count_df = cartesian_count.as_record_dict()
    # cartesian_count.drop_table_from_database_and_remove_from_cache()

    # total_comparisons = calculate_cartesian(row_count_df, link_type)

    # # Now we want to compute the total number of rows generated by the blocking rules

    # linker.count_num_comparisons_from_blocking_rules_for_prediction()

    if positives_not_captured_by_blocking_rules_scored_as_zero:
        settings = linker._settings_obj
        add_cols = list(settings._additional_col_names_to_retain).copy()

        if label_colname not in add_cols:
            settings._additional_col_names_to_retain.append(label_colname)

        df_predict = linker.predict()
        settings._additional_col_names_to_retain = add_cols
        sql = f"""
        select
        case
            when ({label_colname}_l = {label_colname}_r)
            then cast(1.0 as float8) else cast(0.0 as float8)
        end AS clerical_match_score,
        true
            as found_by_blocking_rules,
        *
        from {df_predict.physical_name}
        """

    else:
        new_matchkey = len(linker._settings_obj._blocking_rules_to_generate_predictions)

        df_predict = _predict_from_label_column_sql(
            linker,
            label_colname,
        )

        sql = f"""
        select
        case
            when ({label_colname}_l = {label_colname}_r)
            then cast(1.0 as float8) else cast(0.0 as float8)
        end AS clerical_match_score,
        not (cast(match_key as int) = {new_matchkey})
            as found_by_blocking_rules,
        *
        from {df_predict.physical_name}
        """

    pipeline = CTEPipeline()

    pipeline.enqueue_sql(sql, "__splink__labels_with_predictions")

    sqls = truth_space_table_from_labels_with_predictions_sqls(
        threshold_actual, match_weight_round_to_nearest, 10
    )
    pipeline.enqueue_list_of_sqls(sqls)

    df_truth_space_table = linker.db_api.sql_pipeline_to_splink_dataframe(pipeline)

    return df_truth_space_table


def predictions_from_sample_of_pairwise_labels_sql(linker, labels_tablename):
    sqls = block_from_labels(
        linker, labels_tablename, include_clerical_match_score=True
    )

    sql_info = {
        "sql": compute_comparison_vector_values_sql(
            linker._settings_obj._columns_to_select_for_comparison_vector_values,
            include_clerical_match_score=True,
        ),
        "output_table_name": "__splink__df_comparison_vectors",
    }

    sqls.append(sql_info)

    sqls_2 = predict_from_comparison_vectors_sqls_using_settings(
        linker._settings_obj,
        include_clerical_match_score=True,
        sql_infinity_expression=linker._infinity_expression,
    )

    sqls.extend(sqls_2)
    br_col = _select_found_by_blocking_rules(linker)

    sql = f"""
    select *, {br_col}
    from __splink__df_predict
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__labels_with_predictions",
    }

    # Clearer name than just df_predict
    sqls.append(sql_info)

    return sqls


def prediction_errors_from_labels_table(
    linker,
    labels_tablename,
    include_false_positives=True,
    include_false_negatives=True,
    threshold=0.5,
):
    pipeline = CTEPipeline()
    nodes_with_tf = compute_df_concat_with_tf(linker, pipeline)
    pipeline = CTEPipeline([nodes_with_tf])

    sqls = predictions_from_sample_of_pairwise_labels_sql(linker, labels_tablename)

    pipeline.enqueue_list_of_sqls(sqls)

    false_positives = f"""
    (clerical_match_score < {threshold} and
    match_probability > {threshold})
    """

    false_negatives = f"""
    (clerical_match_score > {threshold} and
    match_probability < {threshold})
    """

    where_conditions = []
    if include_false_positives:
        where_conditions.append(false_positives)

    if include_false_negatives:
        where_conditions.append(false_negatives)

    where_condition = " OR ".join(where_conditions)

    sql = f"""
    select *,
    case
    when {false_positives} then 'FP'
    when {false_negatives} then 'FN'
    END as truth_status

    from __splink__labels_with_predictions
    where
    {where_condition}
    """

    pipeline.enqueue_sql(sql, "__splink__labels_with_fp_fn_status")

    return linker.db_api.sql_pipeline_to_splink_dataframe(pipeline)


def _predict_from_label_column_sql(linker, label_colname):
    # In the case of labels, we use them to block
    # In the case we have a label column, we want to apply the model's blocking rules
    # but add in blocking on the label colname
    linker = deepcopy(linker)
    settings = linker._settings_obj
    brs = settings._blocking_rules_to_generate_predictions

    label_blocking_rule = BlockingRule(f"l.{label_colname} = r.{label_colname}")
    label_blocking_rule.preceding_rules = brs.copy()
    brs.append(label_blocking_rule)

    # Need the label colname to be in additional columns to retain

    add_cols = settings._additional_column_names_to_retain

    if label_colname not in add_cols:
        settings._additional_column_names_to_retain.append(label_colname)

    # Now we want to create predictions
    df_predict = linker.predict()

    return df_predict


def prediction_errors_from_label_column(
    linker,
    label_colname,
    include_false_positives=True,
    include_false_negatives=True,
    threshold=0.5,
):
    df_predict = _predict_from_label_column_sql(
        linker,
        label_colname,
    )

    # Clerical match score is 1 where the label_colname is equal else zero

    # _predict_from_label_column_sql will add a match key for matching on labels
    new_matchkey = len(linker._settings_obj._blocking_rules_to_generate_predictions)
    pipeline = CTEPipeline()
    sql = f"""
    select
    case
        when ({label_colname}_l = {label_colname}_r)
        then cast(1.0 as float8) else cast(0.0 as float8)
    end AS clerical_match_score,
    not (cast(match_key as int) = {new_matchkey})
        as found_by_blocking_rules,
    *
    from {df_predict.physical_name}
    """

    # found_by_blocking_rules

    pipeline.enqueue_sql(sql, "__splink__predictions_from_label_column")

    false_positives = f"""
    (clerical_match_score < {threshold} and
    match_probability > {threshold})
    """

    false_negatives = f"""
    ((clerical_match_score > {threshold} and
    match_probability < {threshold})
    or
    (clerical_match_score > {threshold} and
     found_by_blocking_rules = False)
    )
    """

    where_conditions = []
    if include_false_positives:
        where_conditions.append(false_positives)

    if include_false_negatives:
        where_conditions.append(false_negatives)

    where_condition = " OR ".join(where_conditions)

    sql = f"""
    select * from __splink__predictions_from_label_column
    where {where_condition}
    """

    pipeline.enqueue_sql(sql, "__splink__predictions_from_label_column_fp_fn_only")

    predictions = linker.db_api.sql_pipeline_to_splink_dataframe(pipeline)

    return predictions
