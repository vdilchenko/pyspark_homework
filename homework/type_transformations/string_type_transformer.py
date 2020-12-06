from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

class StringTypeTransformer:
    """
    Class provides transformation for columns with string types to other, where it possible.
    """

    def transform_dataframe(self, dataframe: DataFrame, expected_schema: StructType):
        pass
