"""
# test_transform for project bmi_calculator
# Created by @Sanjeet Shukla at 10:55 AM 1/16/2022 using PyCharm
"""

import unittest
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DecimalType, FloatType, DoubleType
from src.etl.transform import Transform
import test.test_utils


class TransformTest(unittest.TestCase):

    def get_test_dataframe(self):
        """
        Get the input dataframe
        :return: data and the schema for the dataframe
        """
        data = [("Female", 166, 62), ("Male", 171, 96)]
        schema = StructType([StructField("Gender", StringType(), True),
                             StructField("HeightCm", IntegerType(), True),
                             StructField("WeightKg", IntegerType(), True)])
        return data, schema

    def get_bmi_dataframe(self):
        """
        Get the dataframe with bmi info
        :return: data and schema for the dataframe
        """
        data = [("Female", 166, 62, 1.66, 22.5), ("Male", 171, 96, 1.71, 32.83)]
        schema = StructType([StructField("Gender", StringType(), True),
                             StructField("HeightCm", IntegerType(), True),
                             StructField("WeightKg", IntegerType(), True),
                             StructField("HeightM", DoubleType(), True),
                             StructField("BMI", DoubleType(), True)])

        return data, schema

    def get_test_bmi_category_df():
        """
        Get the dataframe for the bmi category
        :return: data and schema for the data frame
        """
        data = [("Female", 166, 62, 1.66, 22.5, "Normal weight", "Low risk"),
                ("Male", 171, 96, 1.71, 32.83, "Moderately obese", "Medium risk")]
        schema = StructType([StructField("Gender", StringType(), True),
                             StructField("HeightCm", IntegerType(), True),
                             StructField("WeightKg", IntegerType(), True),
                             StructField("HeightM", DoubleType(), True),
                             StructField("BMI", DoubleType(), True),
                             StructField("BMI Category", StringType(), True),
                             StructField("Health risk", StringType(), True)])

        return data, schema

    def test_calculate_bmi(self):
        """
        Test Calculate BMI
        :return: None
        """
        spark = test.test_utils.create_spark_session("test_calculate_bmi")
        data, schema = self.get_test_dataframe()
        input_df = test.test_utils.create_df(spark, data, schema)

        transform_process = Transform(spark)
        transformed_df = transform_process.calculate_bmi(input_df)
        transformed_df.show()
        transformed_df.printSchema()

        data, schema = self.get_bmi_dataframe()
        expected_df = test.test_utils.create_df(spark, data, schema)
        assert (test.test_utils.compare_df(transformed_df, expected_df))


if __name__ == "__main__":
    unittest.main()
