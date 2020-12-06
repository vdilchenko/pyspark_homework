from pyspark.sql import DataFrame


class HistoryProduct:
    """
    Class provides possibilities to compute history of rows.
    You should compare old and new dataset and define next for each row:
     - row was changed
     - row was inserted
     - row was deleted
     - row not changed
     Result should contain all rows from both datasets and new column `meta`
     where status of each row should be provided ('not_changed', 'changed', 'inserted', 'deleted')
    """
    def __init__(self, primary_keys=None):
        pass

    # ToDo: Implement history product
    def get_history_product(self, old_dataframe: DataFrame, new_dataframe: DataFrame):
        pass