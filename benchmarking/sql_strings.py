def regular_query(df_concat_with_sample):
    sql = f"""
    WITH __splink__df_concat_with_tf_sample AS (
    SELECT
        *
    FROM {df_concat_with_sample}
    ), __splink__df_blocked AS (
    SELECT
        l.unique_id AS unique_id_l,
        r.unique_id AS unique_id_r,
        l.first_name AS first_name_l,
        r.first_name AS first_name_r,
        l.surname AS surname_l,
        r.surname AS surname_r,
        l.dob AS dob_l,
        r.dob AS dob_r,
        l.email as email_l,
        r.email as email_r,
        l.city as city_l,
        r.city as city_r
    FROM __splink__df_concat_with_tf_sample AS l
    INNER JOIN __splink__df_concat_with_tf_sample AS r
        ON 1 = 1
    WHERE
        l.unique_id < r.unique_id
    ), __splink__df_comparison_vectors AS (
    SELECT
        unique_id_l,
        unique_id_r,
        CASE
        WHEN first_name_l IS NULL
            OR first_name_r IS NULL
        THEN -1
        WHEN first_name_l = first_name_r
        THEN 2
        WHEN LEVENSHTEIN(first_name_l, first_name_r) <= 2
        THEN 1
        ELSE 0
        END AS gamma_first_name,

        CASE
        WHEN surname_l IS NULL
            OR surname_r IS NULL
        THEN -1
        WHEN surname_l = surname_r
        THEN 1
        ELSE 0
        END AS gamma_surname,
        CASE
        WHEN dob_l IS NULL
            OR dob_r IS NULL
        THEN -1
        WHEN dob_l = dob_r
        THEN 1
        ELSE 0
        END AS gamma_dob,

        CASE
        WHEN email_l IS NULL
            OR email_r IS NULL
        THEN -1
        WHEN email_l = email_r
        THEN 1
        ELSE 0
        END AS gamma_email,

        CASE
        WHEN city_l IS NULL
            OR city_r IS NULL
        THEN -1
        WHEN city_l = city_r
        THEN 1
        ELSE 0
        END AS gamma_city
    FROM __splink__df_blocked
    ), __splink__df_predict AS (
    SELECT
        *,
        CAST(0.0 AS DOUBLE) AS match_probability
    FROM __splink__df_comparison_vectors
    )
    SELECT
    gamma_first_name AS comparison_vector_value,
    SUM(match_probability) / (
        SELECT
        SUM(match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_first_name <> -1
    ) AS m_probability,
    SUM(1 - match_probability) / (
        SELECT
        SUM(1 - match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_first_name <> -1
    ) AS u_probability,
    'first_name' AS output_column_name
    FROM __splink__df_predict
    WHERE
    gamma_first_name <> -1
    GROUP BY
    gamma_first_name
    UNION ALL
    SELECT
    gamma_surname AS comparison_vector_value,
    SUM(match_probability) / (
        SELECT
        SUM(match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_surname <> -1
    ) AS m_probability,
    SUM(1 - match_probability) / (
        SELECT
        SUM(1 - match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_surname <> -1
    ) AS u_probability,
    'surname' AS output_column_name
    FROM __splink__df_predict
    WHERE
    gamma_surname <> -1
    GROUP BY
    gamma_surname
    UNION ALL
    SELECT
    gamma_dob AS comparison_vector_value,
    SUM(match_probability) / (
        SELECT
        SUM(match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_dob <> -1
    ) AS m_probability,
    SUM(1 - match_probability) / (
        SELECT
        SUM(1 - match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_dob <> -1
    ) AS u_probability,
    'dob' AS output_column_name
    FROM __splink__df_predict
    WHERE
    gamma_dob <> -1
    GROUP BY
    gamma_dob

    UNION ALL
    SELECT
    gamma_city AS comparison_vector_value,
    SUM(match_probability) / (
        SELECT
        SUM(match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_city <> -1
    ) AS m_probability,
    SUM(1 - match_probability) / (
        SELECT
        SUM(1 - match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_city <> -1
    ) AS u_probability,
    'city' AS output_column_name
    FROM __splink__df_predict
    WHERE
    gamma_city <> -1
    GROUP BY
    gamma_city

        UNION ALL
    SELECT
    gamma_email AS comparison_vector_value,
    SUM(match_probability) / (
        SELECT
        SUM(match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_email <> -1
    ) AS m_probability,
    SUM(1 - match_probability) / (
        SELECT
        SUM(1 - match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_email <> -1
    ) AS u_probability,
    'email' AS output_column_name
    FROM __splink__df_predict
    WHERE
    gamma_email <> -1
    GROUP BY
    gamma_email

    UNION ALL
    SELECT
    0 AS comparison_vector_value,
    AVG(match_probability) AS m_probability,
    AVG(1 - match_probability) AS u_probability,
    '_probability_two_random_records_match' AS output_column_name
    FROM __splink__df_predict
    """

    return sql


def without_where_sql(df_concat_with_sample):
    sql = f"""
    WITH __splink__df_concat_with_tf_sample AS (
    SELECT
        *
    FROM {df_concat_with_sample}
    ), __splink__df_blocked AS (
    SELECT
        l.unique_id AS unique_id_l,
        r.unique_id AS unique_id_r,
        l.first_name AS first_name_l,
        r.first_name AS first_name_r,
        l.surname AS surname_l,
        r.surname AS surname_r,
        l.dob AS dob_l,
        r.dob AS dob_r,
        l.email as email_l,
        r.email as email_r,
        l.city as city_l,
        r.city as city_r
    FROM __splink__df_concat_with_tf_sample AS l
    INNER JOIN __splink__df_concat_with_tf_sample AS r
        ON 1 = 1
    WHERE
        l.unique_id < r.unique_id
    ), __splink__df_comparison_vectors AS (
    SELECT
        unique_id_l,
        unique_id_r,
        CASE
        WHEN first_name_l IS NULL
            OR first_name_r IS NULL
        THEN -1
        WHEN first_name_l = first_name_r
        THEN 2
        WHEN LEVENSHTEIN(first_name_l, first_name_r) <= 2
        THEN 1
        ELSE 0
        END AS gamma_first_name,

        CASE
        WHEN surname_l IS NULL
            OR surname_r IS NULL
        THEN -1
        WHEN surname_l = surname_r
        THEN 1
        ELSE 0
        END AS gamma_surname,
        CASE
        WHEN dob_l IS NULL
            OR dob_r IS NULL
        THEN -1
        WHEN dob_l = dob_r
        THEN 1
        ELSE 0
        END AS gamma_dob,

        CASE
        WHEN email_l IS NULL
            OR email_r IS NULL
        THEN -1
        WHEN email_l = email_r
        THEN 1
        ELSE 0
        END AS gamma_email,

        CASE
        WHEN city_l IS NULL
            OR city_r IS NULL
        THEN -1
        WHEN city_l = city_r
        THEN 1
        ELSE 0
        END AS gamma_city
    FROM __splink__df_blocked
    ), __splink__df_predict AS (
    SELECT
        *,
        CAST(0.0 AS DOUBLE) AS match_probability
    FROM __splink__df_comparison_vectors
    )
    SELECT
    gamma_first_name AS comparison_vector_value,
    SUM(match_probability) / (
        SELECT
        SUM(match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_first_name <> -1
    ) AS m_probability,
    SUM(1 - match_probability) / (
        SELECT
        SUM(1 - match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_first_name <> -1
    ) AS u_probability,
    'first_name' AS output_column_name
    FROM __splink__df_predict
    GROUP BY
    gamma_first_name
    UNION ALL
    SELECT
    gamma_surname AS comparison_vector_value,
    SUM(match_probability) / (
        SELECT
        SUM(match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_surname <> -1
    ) AS m_probability,
    SUM(1 - match_probability) / (
        SELECT
        SUM(1 - match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_surname <> -1
    ) AS u_probability,
    'surname' AS output_column_name
    FROM __splink__df_predict
    GROUP BY
    gamma_surname
    UNION ALL
    SELECT
    gamma_dob AS comparison_vector_value,
    SUM(match_probability) / (
        SELECT
        SUM(match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_dob <> -1
    ) AS m_probability,
    SUM(1 - match_probability) / (
        SELECT
        SUM(1 - match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_dob <> -1
    ) AS u_probability,
    'dob' AS output_column_name
    FROM __splink__df_predict
    GROUP BY
    gamma_dob

    UNION ALL
    SELECT
    gamma_city AS comparison_vector_value,
    SUM(match_probability) / (
        SELECT
        SUM(match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_city <> -1
    ) AS m_probability,
    SUM(1 - match_probability) / (
        SELECT
        SUM(1 - match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_city <> -1
    ) AS u_probability,
    'city' AS output_column_name
    FROM __splink__df_predict
    GROUP BY
    gamma_city

        UNION ALL
    SELECT
    gamma_email AS comparison_vector_value,
    SUM(match_probability) / (
        SELECT
        SUM(match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_email <> -1
    ) AS m_probability,
    SUM(1 - match_probability) / (
        SELECT
        SUM(1 - match_probability)
        FROM __splink__df_predict
        WHERE
        gamma_email <> -1
    ) AS u_probability,
    'email' AS output_column_name
    FROM __splink__df_predict
    GROUP BY
    gamma_email

    UNION ALL
    SELECT
    0 AS comparison_vector_value,
    AVG(match_probability) AS m_probability,
    AVG(1 - match_probability) AS u_probability,
    '_probability_two_random_records_match' AS output_column_name
    FROM __splink__df_predict
    """

    return sql


def pandas_way(df_concat_with_sample):
    sql = f"""
    WITH __splink__df_concat_with_tf_sample AS (
    SELECT
        *
    FROM {df_concat_with_sample}
    ), __splink__df_blocked AS (
    SELECT
        l.unique_id AS unique_id_l,
        r.unique_id AS unique_id_r,
        l.first_name AS first_name_l,
        r.first_name AS first_name_r,
        l.surname AS surname_l,
        r.surname AS surname_r,
        l.dob AS dob_l,
        r.dob AS dob_r,
        l.email as email_l,
        r.email as email_r,
        l.city as city_l,
        r.city as city_r
    FROM __splink__df_concat_with_tf_sample AS l
    INNER JOIN __splink__df_concat_with_tf_sample AS r
        ON 1 = 1
    WHERE
        l.unique_id < r.unique_id
    ), __splink__df_comparison_vectors AS (
    SELECT
        unique_id_l,
        unique_id_r,
        CASE
        WHEN first_name_l IS NULL
            OR first_name_r IS NULL
        THEN -1
        WHEN first_name_l = first_name_r
        THEN 2
        WHEN LEVENSHTEIN(first_name_l, first_name_r) <= 2
        THEN 1
        ELSE 0
        END AS gamma_first_name,

        CASE
        WHEN surname_l IS NULL
            OR surname_r IS NULL
        THEN -1
        WHEN surname_l = surname_r
        THEN 1
        ELSE 0
        END AS gamma_surname,
        CASE
        WHEN dob_l IS NULL
            OR dob_r IS NULL
        THEN -1
        WHEN dob_l = dob_r
        THEN 1
        ELSE 0
        END AS gamma_dob,

        CASE
        WHEN email_l IS NULL
            OR email_r IS NULL
        THEN -1
        WHEN email_l = email_r
        THEN 1
        ELSE 0
        END AS gamma_email,

        CASE
        WHEN city_l IS NULL
            OR city_r IS NULL
        THEN -1
        WHEN city_l = city_r
        THEN 1
        ELSE 0
        END AS gamma_city
    FROM __splink__df_blocked
    ), __splink__df_predict AS (
    SELECT
        *,
        CAST(1.0 AS DOUBLE) AS match_probability
    FROM __splink__df_comparison_vectors
    )

    SELECT
    gamma_first_name as comparison_vector_value,
    sum(match_probability) as u_probability,
    'first_name' as output_column_name
    FROM __splink__df_predict
    group by gamma_first_name

    UNION ALL

    SELECT
    gamma_surname as comparison_vector_value,
    sum(match_probability) as u_probability,
    'surname' as output_column_name
    FROM __splink__df_predict
    group by gamma_surname

    UNION ALL

    SELECT
    gamma_dob as comparison_vector_value,
    sum(match_probability) as u_probability,
    'dob' as output_column_name
    FROM __splink__df_predict
    group by gamma_dob

    UNION ALL

    SELECT
    gamma_email as comparison_vector_value,
    sum(match_probability) as u_probability,
    'email' as output_column_name
    FROM __splink__df_predict
    group by gamma_email

    UNION ALL

    SELECT
    gamma_city as comparison_vector_value,
    sum(match_probability) as u_probability,
    'city' as output_column_name
    FROM __splink__df_predict
    group by gamma_city

    """
    return sql
