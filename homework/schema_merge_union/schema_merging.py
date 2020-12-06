from pyspark.sql import DataFrame


class SchemaMerging:
    """
    Class provides possibilities to union tow datasets with different schemas.
    Result dataset should contain all rows from both with columns from both dataset.
    If columns have the same name and type - they are identical.
    If columns have different types and the same name, 2 new column should be provided with next pattern:
    {field_name}_{field_type}
    """

    # ToDo: Implement dataset union with schema merging
    def union(self, dataframe1: DataFrame, dataframe2: DataFrame):
        pass
