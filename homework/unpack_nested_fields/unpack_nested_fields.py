from pyspark.sql.dataframe import DataFrame


class UnpackNestedFields:
    """
    Class provides possibilities to unpack nested structures in row recursively and provide flat structure as result.
    To clarify rules, please investigate tests.
    After unpacking of structure additional columns should be provided with next name {struct_name}.{struct_field_name}

    """

    # ToDo implement unpacking of nested fields
    def unpack_nested(self, dataframe: DataFrame):
        pass
