# Databricks notebook source
#widgets for source and target information
  # Source File Path = /FileStore/metric_data.csv
  # Target File Path for daily batch = /Filestore/aggregated_metric
  # source_file_type = csv
#The source data is having 3 columns (metric name, value, timestamp)
#this script agreegates the data based on each metric for each single day batch, which can be on week and month as well
#output file is a parquet file with the columns (metric, batch_date,avg_value,min_value,max_value)

dbutils.widgets.removeAll()
dbutils.widgets.dropdown("source_file_type",'csv',['csv', 'json', 'parquet'])
i_file_type =  dbutils.widgets.get("source_file_type")    

dbutils.widgets.text("input_file_path","/Filestore/","Source File Path")
i_file_location =  dbutils.widgets.get("input_file_path")     

dbutils.widgets.text("output_file_path","/Filestore/","Target File Path for daily batch")
o_file_location =  dbutils.widgets.get("output_file_path")     

# COMMAND ----------

def input_file_reader(i_file_location,i_file_type):
### Read source data file from the given locaion ###

    idf= spark.read.format(i_file_type).option("inferSchema","true").option("header","true").load(i_file_location)
    return idf

# COMMAND ----------

#    """  File processing and output file generation """

from pyspark.sql.functions import *
try:
    df=input_file_reader(i_file_location,i_file_type) 
    odf=df.withColumn("batch_date",to_date(col("timestamp"))).groupBy("metric","batch_date").agg(avg('value').alias("avg_value"),min('value').alias("min_value"),max('value').alias("max_value"))
    odf.write.format("parquet").mode("overwrite").partitionBy("batch_date").save(o_file_location)
    display(odf)
    print("Agreegated batch data is ready at the location " + o_file_location) 
except Exception as err:
    err_desc=str(err).replace("'","").replace('"','').replace("\n", " ")    
    print(err_desc)
