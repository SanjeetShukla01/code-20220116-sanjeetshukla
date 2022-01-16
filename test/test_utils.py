"""
# test_utils for project bmi_calculator
# Created by @Sanjeet Shukla at 11:09 AM 1/16/2022 using PyCharm
"""
from pyspark.sql import SparkSession


def create_spark_session(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def create_df(spark, data, schema):
    df = spark.createDataFrame(data=data, schema=schema)
    return df


def compare_df(df1, df2):
    data1 = df1.collect()
    data2 = df2.collect()
    return set(data1) == set(data2)
