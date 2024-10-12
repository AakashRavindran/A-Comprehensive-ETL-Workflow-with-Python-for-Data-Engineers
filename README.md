# A-Comprehensive-ETL-Workflow-with-Python-for-Data-Engineers
A Comprehensive ETL Workflow with Python for Data Engineers -  extract data from CSV, JSON, and XML formats, transform it, and load the transformed data into a structured format for further processing.

This Project is to perform the below

1.Extract data from CSV, JSON, and XML files.
2.Transform the extracted data into a desired format, including unit conversions.
3.Load the transformed data into a CSV file for future use in databases.
4.Log the progress of ETL operations for monitoring purposes.

The Source data files are placed in 'Source' Folder
The Final output csv is placed in 'out' Folder
The logs are being logged inside a log file in the 'logs' folder

In this code we have made use of the below Python modules

glob to handle file formats.
pandas to read CSV and JSON files (Also XML if required).
xml.etree.ElementTree to parse XML data.
os module to get the path and split file name to get extension of file
logging module to get the logs


inches_to_m function has been created to convert inches to metres
pounds_to_kg function has been created to convert weight in pounds to Kgs

different functions have been created for each file format to convert them to Pandas Datafreames --> csv_get_data, json_get_data, xml_get_data

A Master Function has been created to call the relevant function based on the file type and combine the extracted data into a single DataFrame-->master_file_ETL , This takes 2 arguments Source folder and Destination csv file

This project highlights the practical implementation of ETL processes using Python. The data extraction from multiple file formats, transformation of units, and loading of the final data into a structured CSV format demonstrate essential data engineering skills. Additionally, by logging each step of the process, you can monitor the progress and debug issues if they arise


