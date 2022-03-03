from splink.parse_sql import get_columns_used_from_sql


def test_get_columns_used():
    sql_1 = """
    lat_lng_uncommon_l['lat'] - lat_lng_uncommon_r['lat']
    """
    assert set(get_columns_used_from_sql(sql_1)) == set(
        [
            "lat_lng_uncommon_l",
            "lat_lng_uncommon_r",
        ]
    )

    sql_2 = """
    transform(latlongexplode(lat_lng_arr_uncommon_l,lat_lng_arr_uncommon_r ),
    x -> sin(radians(x['place2']['lat'] - x['place1']['lat'])) )
    """

    assert set(get_columns_used_from_sql(sql_2)) == set(
        [
            "lat_lng_arr_uncommon_l",
            "lat_lng_arr_uncommon_r",
        ]
    )

    sql_3 = "AGGREGATE(cities, 0, (x, y) -> x + length(y))"

    assert set(get_columns_used_from_sql(sql_3)) == set(
        [
            "cities",
        ]
    )

    sql_4 = "AGGREGATE(cities, 0, x ->  length(x['a']))"

    assert set(get_columns_used_from_sql(sql_4)) == set(
        [
            "cities",
        ]
    )
