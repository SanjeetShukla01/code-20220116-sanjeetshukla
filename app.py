"""
# app.py for project bmi_calculator
# Created by @Sanjeet Shukla at 9:06 AM 1/16/2022 using PyCharm
"""
import logging.config
import sys
from pyspark.sql import SparkSession
from src.utils import utils
from src.etl import ingest, transform, load


class App:
	logging.config.fileConfig("config/logging.conf")

	def run_pipeline(self):
		try:
			input_path = utils.get_config("IO_CONFIGS", "INPUT_DATA_PATH")
			ingest_process = ingest.Ingest(self.spark)
			df = ingest_process.ingest_json_data(input_path + "/input_data.json")
			df.show()

			transform_process = transform.Transform(self.spark)
			tdf = transform_process.calculate_bmi(df)
			logging.info("BMI Calculated")
			tdf.show()

			bmi_category_df = transform_process.get_bmi_category(tdf)
			logging.info("BMI Category and Health risk calculated")
			bmi_category_df.show()

			people_count = transform_process.get_overweight_count(bmi_category_df)
			logging.info("Total number of people with overweight category: {}".format(people_count))

			output_path = utils.get_config("IO_CONFIGS", "OUTPUT_DATA_PATH")
			persist_process = load.Load(self.spark)
			persist_process.write_output_csv(bmi_category_df, output_path)
		except Exception as exp:
			logging.error("An error occurred while running the pipeline > " + str(exp))
			# send email notification or log to database
			sys.exit(1)

	def create_spark_session(self):
		self.spark = SparkSession.builder \
			.appName("myPySparkApp") \
			.enableHiveSupport() \
			.getOrCreate()

	def close(self):
		self.spark.stop

if __name__ == "__main__":
	logging.info("running pipeline")
	pipeline = App()
	pipeline.create_spark_session()
	pipeline.run_pipeline()
	pipeline.close()
