import findspark
findspark.init()
import shutil, os
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

def transform_category(df):
    df = df.withColumn("Type",
               when((col("AppName") == 'CHANNEL') | (col("AppName") =='DSHD')| (col("AppName") =='KPLUS')| (col("AppName") =='KPlus'), "Truyền Hình")
              .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
                     (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| (col("AppName") =='DANET'), "Phim Truyện")
              .when((col("AppName") == 'RELAX'), "Giải Trí")
              .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
              .when((col("AppName") == 'SPORT'), "Thể Thao")
              .otherwise("Error"))
    return df 
def pivot_data(df):
    df = df.groupBy("Contract").pivot("Type").agg({"TotalDuration": "sum"}).withColumnRenamed("sum(TotalDuration)", "TotalDuration").fillna(0)
    return df
def cal_device(df):
    statis = df.select("Contract", "Mac").groupBy("Contract").agg({"Mac": "count"})
    statis = statis.withColumnRenamed("count(Mac)", "Total_device")
    return statis
def statistic_total(df):
    df_total = df.select("Contract", "TotalDuration", "Type", "date").groupBy("Contract", "Type", "date").agg({"TotalDuration": "sum"})
    df_total = df_total.withColumnRenamed("sum(TotalDuration)", "TotalDuration")
    return df_total
def save_file(df, save_path):
    df.repartition(1).write.mode("overwrite").option("header", "true").csv(save_path)
    print("Save successfully")
def most_watch(df):
    columns = [c for c in df.columns if c not in ("Contract", "Total_device", "activate_level")]
    max_value = greatest(*columns)
    result = None
    for c in columns:
        if result is None:
            result = when(col(c) == max_value, lit(c))
        else:
            result = result.when(col(c) == max_value, lit(c))
    df = df.withColumn("most_watch", result)
    return df
def customer_taste(df):
    columns = [c for c in df.columns if c not in ("Contract", "most_watch", "Total_device", "activate_level")]
    df = df.withColumn("customer_taste", concat_ws(",", array(*[when(col(c) > 0, lit(c)) for c in columns])))
    return df
def sum_activeness(df):
    df = df.groupBy("contract").agg({"date": "count"})
    df = df.withColumnRenamed("count(date)", "sum_date")
    df = df.withColumn("activate_level", when((col("sum_date") <= 7) & (col("sum_date") >= 1), "very low")
                       .when((col("sum_date") >= 8) & (col("sum_date") <= 14), "low")
                       .when((col("sum_date") >= 15) & (col("sum_date") >= 21), "moderate")
                       .when((col("sum_date") >= 22) & (col("sum_date") <= 28), "high")
                       .when((col("sum_date") >= 29) & (col("sum_date") <= 31), "very high"))
    df = df.drop("sum_date")
    return df
def import_to_postgres(result):
    print("LOAD")
    url = 'jdbc:postgresql://' + 'localhost' + ':' + '5432' + '/' + 'customer360'
    driver = "org.postgresql.Driver"
    user = 'totencanh'
    password = 'totencanh'
    result.write.format('jdbc').option('url',url).option('driver',driver).option('dbtable','customer_content_stats').option('user',user).option('password',password).mode('append').save()
    return print("Data Import Successfully")
def main(df):
    print("TRANSFORM")
    df = df.select("_source.*")   
    df = df.withColumn("date", to_date(regexp_extract(input_file_name(), r"(\d{8})\.json", 1), "yyyyMMdd"))
    active = sum_activeness(df)
    df = transform_category(df)
    df_new = cal_device(df)
    df_total = statistic_total(df)
    df = pivot_data(df_total)
    df = df.join(active, on = "Contract", how = "inner")
    df = df.join(df_new, on = "Contract", how = "inner")
    df = most_watch(df)
    df = customer_taste(df)
    print("TRANSFORM SUCCESSFULLY")
    return df