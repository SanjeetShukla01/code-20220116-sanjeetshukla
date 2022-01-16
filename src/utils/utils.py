"""
# utility_functions for project bmi_calculator
# Created by @Sanjeet Shukla at 9:54 AM 1/16/2022 using PyCharm
"""
import configparser
import logging.config
import datetime


def get_config(config_section: str, config_value: str):
	config = configparser.ConfigParser()
	config.read("config/pipeline.ini")
	return config.get(config_section, config_value)


def get_partitioned_output_path(file_path):
	"""
	This function provides the output path to which the data frame will be created
	:param file_path: file path from config_utils for the value 'output_file_path'
	:return: output file path with date partition
	"""
	file_path = file_path + '/' + str(datetime.datetime.today().year) + '/' + str(
		datetime.datetime.today().month) + '/' + str(datetime.datetime.today().day)
	logging.info("file_path: " + file_path)
	return file_path



