# Import the required modules

import glob
import pandas as pd
import xml.etree.ElementTree as ET
import datetime
import os
import logging  # Ued the logging module to create logs

# import lxml --> used when pandas.read_xml is used

# Configure the logging process
logging.basicConfig(
    filename="logs/etl_process_logs.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# Convert inches to cm
def inches_to_m(height_inc):
    return round(height_inc * 0.0254, 2)


# Convert pounds to Kg
def pounds_to_kg(weight_p):
    return round(weight_p * 0.453592, 2)


# Function to Convert csv to pandas dataframe
def csv_get_data(file):
    logging.info(f"Extracting data from CSV: {file}")
    csv_df = pd.read_csv(file)
    csv_df["height"] = csv_df["height"].apply(inches_to_m)
    csv_df["weight"] = csv_df["weight"].apply(pounds_to_kg)
    logging.info(f"Converted height to cm and weight to kg")
    logging.info(f"Transformation complete to dataframe  for CSV: {file}")
    return csv_df


# Function to Convert JSON to pandas dataframe
def json_get_data(file):
    logging.info(f"Extracting data from JSON: {file}")
    json_df = pd.read_json(file, lines=True)
    json_df["height"] = json_df["height"].apply(inches_to_m)
    json_df["weight"] = json_df["weight"].apply(pounds_to_kg)
    logging.info(f"Converted height to cm and weight to kg")
    logging.info(f"Transformation complete to dataframe  for JSON: {file}")
    return json_df


# Function to Convert XML to pandas dataframe
def xml_get_data(file):
    logging.info(f"Extracting data from XML: {file}")
    tree = ET.parse(file)
    root = tree.getroot()
    person_data = []
    for person in root.findall("person"):
        name = person.find("name").text
        height = inches_to_m(float(person.find("height").text))
        weight = pounds_to_kg(float(person.find("weight").text))
        person_data.append({"name": name, "height": height, "weight": weight})
    xml_df = pd.DataFrame(person_data)
    logging.info(f"Converted height to cm and weight to kg")
    logging.info(f"Transformation complete to dataframe  for XML: {file}")
    return xml_df
    # xml_df = pd.read_xml(file)
    # xml_df["height"] = xml_df["height"].apply(inches_to_m)
    # xml_df["weight"] = xml_df["weight"].apply(pounds_to_kg)
    # return xml_df


# Use glob to find files in the source folder and create the master function
def master_file_ETL(source_folder, destination_folder):
    dataframes_list = []
    logging.info("Searching Source Folder....")
    for file_path in glob.glob(source_folder):
        _, file_extension = os.path.splitext(file_path)
        if file_extension == '.csv':
            logging.info(f"CSV file found{file_path}")
            csv_df_in = csv_get_data(file_path)
            dataframes_list.append(csv_df_in)
        elif file_extension == '.xml':
            logging.info(f"XML file found{file_path}")
            xml_df_in = xml_get_data(file_path)
            dataframes_list.append(xml_df_in)
        elif file_extension == '.json':
            logging.info(f"JSON file found{file_path}")
            json_df_in = json_get_data(file_path)
            dataframes_list.append(json_df_in)
        else:
            print(f"Unsupported file type: {file_path}")
            logging.warning(f"Unsupported file found{file_path}")

    master_dataframe = pd.concat(dataframes_list, ignore_index=True)
    logging.info("Begin Load data into destination path..")
    master_dataframe.to_csv(destination_folder)
    logging.info(f"Data successfully loaded to: {destination_folder}")


# Execution of the function

source = "source/*"
destination = "out/final_out.csv"
print(master_file_ETL(source, destination))
