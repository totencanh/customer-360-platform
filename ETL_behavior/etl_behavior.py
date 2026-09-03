import findspark
findspark.init()
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import Window
spark = SparkSession.builder.appName("ETL_behavior").config("sql.driver.memory", "6g")\
                    .config("spark.sql.shuffle.partitions", "50")\
                    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
                    .getOrCreate()

def read_file(path):
    df = spark.read.parquet(path)
    return df

def process_log_search(df):
    df = df.select('user_id','keyword')
    df = df.groupBy('user_id','keyword').count()
    df = df.withColumnRenamed('count','TotalSearch')
    df = df.orderBy('user_id',ascending = False )
    window = Window.partitionBy('user_id').orderBy(col('TotalSearch').desc())
    df = df.withColumn('Rank',row_number().over(window))
    df = df.filter(col('Rank') == 1)
    df = df.withColumnRenamed('keyword','Most_Search')
    df = df.select('user_id','Most_Search')
    return df 

def import_to_postgres(df):
    url = "jdbc:postgresql://" + 'localhost' + ':' + '5432' + '/' + 'customer360'
    driver = "org.postgresql.Driver"
    user = 'totencanh'
    password = 'totencanh'
    df.write.format('jdbc').option('url', url).option('driver', driver).option('dbtable', 'customer_behavior_stats').option('user', user).option('password', password).mode('append').save()
def save_path(df, save_path):
    df.repartition(1).write.mode("overwrite").option("header", "true").csv(save_path)
    print("Save successfully")
input_path = "D:/ChuyenNganh/Project/customer-360-platform/ETL_behavior/data/log_search/"
output_path = "D:/ChuyenNganh/Project/customer-360-platform/ETL_behavior/output/"
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
new_df = read_file(input_path + date_list[0])
for date in range(1, len(date_list)):
    df = read_file(input_path + date_list[date])
    if new_df is None:
        new_df = df
    else:
        new_df = new_df.union(df)
print("TRANSFORM")
df = process_log_search(new_df)
df.show()
save_path(df, output_path)
import_to_postgres(df)
