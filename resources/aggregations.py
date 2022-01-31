from pyspark.sql import functions as F


class Aggregations:
    @staticmethod
    def timeFrameWindow(df: object, frame: str, columns: list, aggcol: str, orderbycol: str) -> object:
        """
        One single row for each 10 minutes
        :param df: source data
        :param frame: time frame for window slice
        :param columns: dataframe columns
        :param aggcol: aggregation column
        :param orderbycol: orderby column
        :return: Two options of dataframes
        """
        columns.remove(aggcol)
        df_result = df.groupBy(F.window(aggcol, frame), *columns) \
            .agg(F.max(aggcol).alias(aggcol)) \
            .orderBy(orderbycol)

        ##### SECOND OPTION FOR THIS TOPIC #####
        df_result2 = df.groupBy(F.window(aggcol, frame)) \
            .agg(F.max(aggcol).alias(aggcol)) \
            .orderBy(orderbycol)

        return df_result, df_result2

    @staticmethod
    def countWithCondition(df: object) -> object:
        """
        Count how many actions of each type there are per minute
        :param df: source data
        :return: Dataframe
        """
        count_condition = lambda cond: F.sum(F.when(cond, 1).otherwise(0))

        df_result = df.groupBy(F.window("time", "1 minute"), "action") \
            .agg(count_condition(F.col("action") == 'Open').alias("Opened"),
                 count_condition(F.col("action") == 'Close').alias("Closed")
                 )

        return df_result

    @staticmethod
    def avgActions(df: object) -> int:
        """
        Average number of actions each 10 minutes
        :param df: source data
        :return: average number
        """
        df_result = df.groupBy(F.window("time", "10 minutes")) \
            .agg(F.count("action").alias("actionCounts")).orderBy("window")

        df_result = df_result.groupBy().avg("actionCounts")

        return round(df_result.collect()[0][0])

    @staticmethod
    def topTenActionType(df: object) -> object:
        """
        Top 10 minutes with biggest amount of “open” action
        :param df: source data
        :return: Dataframe
        """
        df_result = df.groupBy(F.window("time", "10 minutes"), "action") \
            .agg(F.count("action").alias("actionCounts"))

        df_result = df_result.filter(F.col("action") == "Open").orderBy("actionCounts", ascending=False).limit(10)

        return df_result
