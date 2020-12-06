from pyspark.sql.dataframe import DataFrame
from typing import List


class IllegalCharRemover:
    """
    Class provides possibilities to remove illegal chars from string column.
    """
    def __init__(self, chars: List[str], replacement):
        pass

    def remove_illegal_chars(self, dataframe: DataFrame, source_column: str, target_column: str):
        pass
