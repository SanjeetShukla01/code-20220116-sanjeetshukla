"""
# load.py for project bmi_calculator
# Created by @Sanjeet Shukla at 9:08 AM 1/16/2022 using PyCharm
"""
import logging
from pyspark.sql.dataframe import DataFrame
import logging.config


class Load:
    logging.config.fileConfig("config/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def write_output_csv(self, df: DataFrame, path: str):
        """
        This function writes the dataframe to the output location in csv format
        :param logger: logger object
        :param path: the output folder path
        :param df: dataframe which is to be written to the desired location in csv
        :return: None
        """
        try:
            logger = logging.getLogger("Load")
            logger.info("persisting data")
            df.write.mode("overwrite").csv(path, header='true')
            logger.info("Data written to the output location: %s", path)
        except Exception as exp:
            logger.error("An error occurred while persisting data > " + str(exp))

