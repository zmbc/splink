# from .dialect_base_classes import _dialect_base_factory

# from ..comparison_level_library import (
#     ArrayIntersectLevelBase,
#     ColumnsReversedLevelBase,
#     DamerauLevenshteinLevelBase,
#     DatediffLevelBase,
#     DistanceFunctionLevelBase,
#     DistanceInKMLevelBase,
#     ElseLevelBase,
#     ExactMatchLevelBase,
#     JaccardLevelBase,
#     JaroLevelBase,
#     JaroWinklerLevelBase,
#     LevenshteinLevelBase,
#     NullLevelBase,
#     PercentageDifferenceLevelBase,
# )

# class null_level(DuckDBBase, NullLevelBase):
#     pass


# class exact_match_level(DuckDBBase, ExactMatchLevelBase):
#     pass


# class else_level(DuckDBBase, ElseLevelBase):
#     pass


# class columns_reversed_level(DuckDBBase, ColumnsReversedLevelBase):
#     pass


# class distance_function_level(DuckDBBase, DistanceFunctionLevelBase):
#     pass


# class levenshtein_level(DuckDBBase, LevenshteinLevelBase):
#     pass


# def _core_comparison_levels(dialect):

#     base_class = _dialect_base_factory(dialect)

#     return {
#         "null_level":
#     }
