import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from ETL_interaction.etl_interaction import main as etl_file, save_file as save_file, import_to_postgres
findspark.init()
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", required= True)
parser.add_argument("--output_path", required= True)
parser.add_argument("--current_day", required= True)
parser.add_argument("--to_day", required= True)
spark = SparkSession.builder.appName("ETL") \
    .config("spark.driver.memory", "6g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
def read_file(path):
    file = spark.read.json(path)
    return file

def input_file():
    path = str(input("Please put your input link: "))
    return path

def output_file():
    output = str(input("Output link: "))    
    return output
"""args = parser.parse_args()
input_path = args.input_path
output_path = args.output_path
current_date = args.current_day
to_day = args.to_day"""
input_path = "D:/ChuyenNganh/Project/customer-360-platform/ETL/data/log_content/"
output_path = "D:/ChuyenNganh/Project/customer-360-platform/ETL/output/"
current_date = str(input("Nhap ngay: "))
to_day = str(input("Nhap ngay: "))
current = datetime.strptime(current_date, "%Y%m%d").date()
to = datetime.strptime(to_day, "%Y%m%d"). date()
date_list = []
start = current
while(start <= to):
    date_list.append(datetime.strftime(start, "%Y%m%d"))
    start += timedelta(days = 1)

print("EXTRACT")
new_df = spark.read.json(input_path + date_list[0] + ".json")
for date in range(1, len(date_list)):
    file = spark.read.json(input_path + date_list[date] + ".json")
    if new_df is None:
        new_df = file
    else:
        new_df = new_df.union(file)
new_df = etl_file(new_df)
save_file(new_df, output_path)
import_to_postgres(new_df)
