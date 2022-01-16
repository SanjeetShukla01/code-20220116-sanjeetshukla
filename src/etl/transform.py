"""
# transform.py for project bmi_calculator
# Created by @Sanjeet Shukla at 9:07 AM 1/16/2022 using PyCharm
"""
from pyspark.sql.functions import col, when, round


class Transform:

    def __init__(self, spark):
        self.spark = spark

    def calculate_bmi(self, df):
        """
        This function calculates height in meters and BMI value for the input dataframe.
        :param df: raw input dataframe for which the BMI will be calculated.
        :return: dataframe with height in meters,BMI value added to the input dataframe
        """
        return df.withColumn('HeightM', df.HeightCm / 100).withColumn('BMI', round(
            df.WeightKg / ((df.HeightCm / 100) * (df.HeightCm / 100)), 2))

    def get_bmi_category(self, df):
        """
        This function adds the BMI category and Health risk based on the BMI value
        :param df: input dataframe with BMI values
        :return: Dataframe with  BMI category and Health risk derived from their respective BMI values
        """
        return df.withColumn('BMI Category', when(df.BMI <= 18.4, "Underweight")
                             .when((df.BMI >= 18.5) & (df.BMI <= 24.9), "Normal weight")
                             .when((df.BMI >= 25) & (df.BMI <= 29.9), "Overweight")
                             .when((df.BMI >= 30) & (df.BMI <= 34.9), "Moderately obese")
                             .when((df.BMI >= 35) & (df.BMI <= 39.9), "Severely obese")
                             .when((df.BMI >= 40), "Very severely obese")) \
            .withColumn('Health risk', when(df.BMI <= 18.4, "Malnutrition risk")
                        .when((df.BMI >= 18.5) & (df.BMI <= 24.9), "Low risk")
                        .when((df.BMI >= 25) & (df.BMI <= 29.9), "Enhanced risk")
                        .when((df.BMI >= 30) & (df.BMI <= 34.9), "Medium risk")
                        .when((df.BMI >= 35) & (df.BMI <= 39.9), "High risk")
                        .when((df.BMI >= 40), "Very high risk"))

    def get_overweight_count(self, df):
        """
        This function returns the count of people who are in 'Overweight' category
        :param df: dataframe with BMI value and their respective BMI categories
        :return: Count of records of people with BMI category as 'Overweight'
        """
        return df.filter(col("BMI Category") == 'Overweight').count()
