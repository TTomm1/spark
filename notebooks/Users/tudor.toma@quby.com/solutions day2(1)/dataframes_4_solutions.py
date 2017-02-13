# Databricks notebook source
import sys
from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as sf

# COMMAND ----------

ddf_air = spark.read.parquet('/FileStore/tables/32u1wgs71485526830279')

# COMMAND ----------

# MAGIC %md ## Exercise
# MAGIC 
# MAGIC Take a look at the NA patterns in the ddf_air Dsf. It seems like low volume airports do not have flights every month!
# MAGIC 
# MAGIC Let's do something barbaric then: fill the all the NA columns with the minimum over the window 'airport', 'carrier', and the previous value with the same window, ordered by 'year', 'month'

# COMMAND ----------

# MAGIC %md ### *Solution*

# COMMAND ----------

def fill_column_wmin(df, column_name):
    wspec = Window.partitionBy('airport', 'carrier').rangeBetween(-sys.maxsize, 0)
    return (df.withColumn(column_name,
                          sf.when(~sf.isnan(column_name), sf.col(column_name))
                           .otherwise(sf.min(column_name).over(wspec))))

# COMMAND ----------

def fill_column_wprevious(df, column_name):
    wspec = Window.partitionBy('airport', 'carrier').orderBy('year', 'month')
    return (df.withColumn(column_name,
                          sf.when(~sf.isnan(column_name), sf.col(column_name))
                           .otherwise(sf.lag(column_name).over(wspec))))

# COMMAND ----------

# MAGIC %md Check that it indeed works for a column

# COMMAND ----------

fill_column_wprevious(ddf_air, "arr_flights").filter((sf.col('year').isin([2014, 2015])) &
                         (sf.col('month').isin([10, 11]))  &
                         (sf.col('airport') == 'PSC') & 
                         (sf.col('carrier') == 'DL')).toPandas().head().T

# COMMAND ----------

# MAGIC %md Now we can do it for every column

# COMMAND ----------

intermediate = ddf_air

for col in ddf_air.columns[6:]:
    intermediate = fill_column_wprevious(intermediate, col)

# COMMAND ----------

intermediate.filter((sf.col('year').isin([2014, 2015])) &
                         (sf.col('month').isin([10, 11]))  &
                         (sf.col('airport') == 'PSC') & 
                         (sf.col('carrier') == 'DL')).toPandas().head().T

# COMMAND ----------

# MAGIC %md ## Exercise
# MAGIC Create a new column for a numeric column of your choice with the column difference between with the previous month in the 'airport', 'carrier' window. When there is nothing in the previous element, put a 0 (not mathematically correct, but still).

# COMMAND ----------

# MAGIC %md ### *Solution*

# COMMAND ----------

wspec = Window.partitionBy('airport', 'carrier').orderBy('year', 'month')
(ddf_air.withColumn('arr_flights_diff',
                    sf.col('arr_flights') - sf.lag('arr_flights').over(wspec))
        .fillna(0, subset=['arr_flights_diff'])).limit(20).toPandas().head().T

# COMMAND ----------

# MAGIC %md ## Exercise
# MAGIC Remove all the groups of 'airport', 'carrier', 'year' where more than 20% of flights is delayed by 2000 (make the parameters adjustable!)

# COMMAND ----------

# MAGIC %md ### *Solution*

# COMMAND ----------

wspec = Window.partitionBy('airport', 'carrier', 'year')
delay_threshold = 1000
pct_threshold = 0.2
(ddf_air.withColumn('above_delay_threshold',
                     (sf.col('arr_delay') > delay_threshold).cast("int"))
         .withColumn('ones',
                     sf.lit(1))
         .withColumn('count_in_window',
                     sf.sum('ones').over(wspec))
         .withColumn('pct_above_delay_threshold',
                     (sf.sum('above_delay_threshold').over(wspec) 
                      / sf.col('count_in_window')))
         .filter(sf.col('pct_above_delay_threshold') < pct_threshold))

# COMMAND ----------

