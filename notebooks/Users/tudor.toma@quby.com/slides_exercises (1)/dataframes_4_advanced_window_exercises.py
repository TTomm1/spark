# Databricks notebook source
import sys
import pyspark.sql.functions as sf

# COMMAND ----------

ddf_air = spark.read.parquet('/FileStore/tables/32u1wgs71485526830279')

# COMMAND ----------

ddf_air.show()

# COMMAND ----------

# MAGIC %md ## Exercise
# MAGIC Create a new column for a numeric column of your choice with the column difference between with the previous month in the 'airport', 'carrier' window. When there is nothing in the previous element, put a 0 (not mathematically correct, but still).

# COMMAND ----------

# MAGIC %md ## Exercise
# MAGIC Remove all the groups of 'airport', 'carrier', 'year' where more than 20% of flights is delayed by 2000 (make the parameters adjustable!)

# COMMAND ----------

# MAGIC %md ## Exercise
# MAGIC 
# MAGIC Take a look at the NA patterns in the airlines DF. It seems like low volume airports do not have flights every month!
# MAGIC 
# MAGIC Let's do something barbaric then: fill the all the NA columns with the minimum over the window 'airport', 'carrier', and the previous value with the same window, ordered by 'year', 'month'