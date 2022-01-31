from utls.configs import configs
from pyspark.sql import SparkSession
from resources.aggregations import *


class Program:
    @staticmethod
    def __getSpark():
        ss = SparkSession.builder \
            .appName("SCRM_LIDL_CHALLENGE") \
            .master("local[*]") \
            .getOrCreate()
        return ss

    @staticmethod
    def _getDataframe():
        spark = Program.__getSpark()
        f = spark.read.option("header", "true").csv(configs["SOURCE_FILE"])
        return f

    @staticmethod
    def getAvgNumber():
        file = Program._getDataframe()
        agg = Aggregations()
        # Average number of actions each 10 minutes
        avg_actions = agg.avgActions(file)
        return avg_actions

    @staticmethod
    def getTopTenOpened():
        file = Program._getDataframe()
        agg = Aggregations()
        top_ten = agg.topTenActionType(file)
        return top_ten

    @staticmethod
    def getOneMinuteWindow():
        file = Program._getDataframe()
        agg = Aggregations()
        one_minute_window = agg.countWithCondition(df=file)
        return one_minute_window


class DriverProgram:
    @staticmethod
    def main():
        p = Program()
        file = p._getDataframe()
        agg = Aggregations()

        # One single row for each 10 minutes
        ten_minute_window, ten_minute_window2 = agg.timeFrameWindow(df=file, frame="10 minutes",
                                                                    columns=["time", "action"],
                                                                    aggcol="time",
                                                                    orderbycol="time")
        ten_minute_window.show(truncate=False)
        ten_minute_window2.show(truncate=False)

        # Count how many actions of each type there are per minute
        one_minute_window = Program.getOneMinuteWindow()
        one_minute_window.show(truncate=False)

        # Average number of actions each 10 minutes
        print(f"The average of actions is: {Program.getAvgNumber()}\n")

        # Top 10 minutes with biggest amount of “open” action
        top_ten = Program.getTopTenOpened()
        top_ten.show(truncate=False)


if __name__ == '__main__':
    main = DriverProgram()
    main.main()
