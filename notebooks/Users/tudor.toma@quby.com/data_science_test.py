# Databricks notebook source
import numpy as np
import pandas as pd
import sklearn
import pyspark
from pyspark.sql.types import StringType
from pyspark import SQLContext
#import nilmtk
sqlContext = SQLContext(sc)
import qupy
import keras

# COMMAND ----------

def test_function(message):
  print(message)

# COMMAND ----------

validation_data_rdd = sc.textFile("/FileStore/tables/fq7e4sa31484042903852/validation_outcome.csv").map(lambda line: line.split(","))


header = validation_data_rdd.first() #extract header
#print(header)
data = validation_data_rdd.filter(lambda x: x != header) 

validation_data_df = data.toDF(header)
validation_data_df.show()
validation_data_df_pd = validation_data_df.toPandas()
print(validation_data_df_pd.head())
validation_data_df_pd[validation_data_df_pd['Precision']!=-1].groupby(['user','appliance']).count()

# COMMAND ----------

validation_data_df.count()

# COMMAND ----------

display(spark.table("validation_outcome"))

# COMMAND ----------

my_new_df = spark.table("validation_outcome")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/fq7e4sa31484042903852"))

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/fq7e4sa31484042903852/consolidated_matches.csv")


# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/eit5p6ir1484041960276",True)

# COMMAND ----------

