"""
# ingest.py for project bmi_calculator
# Created by @Sanjeet Shukla at 9:06 AM 1/16/2022 using PyCharm
"""

import logging.config
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
from src.utils import column_constants


class Ingest:
    logging.config.fileConfig("config/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def ingest_json_data(self, file_path):
        """
        This function reads the json file and returns a data frame
        :param spark: Spark session
        :param file_path: input file path where the data is available
        :return: dataframe from input JSON file
        """
        logger = logging.getLogger("Ingest")
        logger.info("Ingesting data")
        try:
            columns = getattr(column_constants, "column_constants")
            schema = StructType([StructField(columns["GENDER"], StringType(), True),
                                 StructField(columns["HEIGHT_CM"], IntegerType(), True),
                                 StructField(columns["WEIGHT_KG"], IntegerType(), True)])
            input_file = self.spark.read.option("multiLine", "true").schema(schema).json(file_path)
            return input_file.persist()
        except Exception as exp:
            logger.error("An error occured while ingesting data > " + str(exp))

