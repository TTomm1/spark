# Databricks notebook source
dir_root = '/Users/jelte/gdd/training/quby/ds-with-spark/'
from pyspark.sql import Window, Row
from pyspark.sql import functions as sf
ddf_heroes = spark.read.csv(dir_root + 'heroes.csv', header = True)

# COMMAND ----------

ddf_temp = spark.createDataFrame([Row(mid=1, month=1.0, temperature=3.0),
 Row(mid=2, month=1.0, temperature=6.0),
 Row(mid=3, month=2.0, temperature=4.0),
 Row(mid=4, month=3.0, temperature=8.0),
 Row(mid=5, month=3.0, temperature=9.0),
 Row(mid=6, month=3.0, temperature=8.0),
 Row(mid=7, month=3.0, temperature=12.0)])

# COMMAND ----------

# MAGIC %md ## 4. Windows

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as sf

# COMMAND ----------

ddf_heroes = spark.read.csv(dir_root + 'heroes.csv', header = True)

# COMMAND ----------

ddf_heroes.show()

# COMMAND ----------

# MAGIC %md Compute the demeaned attack for each hero wrt other heroes in its role

# COMMAND ----------

ddf_heroes_agg = (ddf_heroes
                  .groupBy('role')
                  .agg(sf.mean('attack').alias('mean_attack')))

# COMMAND ----------

ddf_heroes_agg.show()

# COMMAND ----------

(ddf_heroes
 .join(ddf_heroes_agg, on = ['role'])
 .withColumn('demeaned_attack', sf.col('attack') - sf.col('mean_attack'))
 .show(5))

# COMMAND ----------

# MAGIC %md ### 4.1 partitionBy()
# MAGIC - add aggregation as column (cleaner)
# MAGIC - partition into windows by variable

# COMMAND ----------

wspec = Window.partitionBy('role')
ddf_role = (ddf_heroes.withColumn('demeaned_attack', sf.col('attack') - sf.mean('attack').over(wspec))) 
ddf_role.show(10) 

# COMMAND ----------

# MAGIC %md Quick question: how is partitionBy different (in result) from the groupby-join method?

# COMMAND ----------

ddf_role.sort('_c0').show(5)

# COMMAND ----------

ddf_role.dtypes

# COMMAND ----------

ddf_role.sort(sf.col('_c0').cast('Int')).show(5)

# COMMAND ----------

# MAGIC %md ### 4.2 orderBy()
# MAGIC - compute aggregates on a sliding window and add as new column
# MAGIC - default window size: all rows before up to including this row
# MAGIC - orderBy columns specify the ordering
# MAGIC - apply order sensitive functions: lag, lead, first, last etc.

# COMMAND ----------

ddf_temp = spark.createDataFrame([Row(mid=1, month=1.0, temperature=3.0),
 Row(mid=2, month=1.0, temperature=6.0),
 Row(mid=3, month=2.0, temperature=4.0),
 Row(mid=4, month=3.0, temperature=8.0),
 Row(mid=5, month=3.0, temperature=9.0),
 Row(mid=6, month=3.0, temperature=8.0),
 Row(mid=7, month=3.0, temperature=12.0)])

# COMMAND ----------

# MAGIC %md #### Rolling mean

# COMMAND ----------

wspec = Window.orderBy('mid')
(ddf_temp
 .withColumn('mean_temp', sf.mean('temperature').over(wspec))
 .withColumn('window_start', sf.first('mid').over(wspec))
 .withColumn('window_end', sf.last('mid').over(wspec))
 .sort('mid')
 .show())

# COMMAND ----------

# MAGIC %md #### Rolling mean accross months

# COMMAND ----------

wspec = Window.orderBy('month')
(ddf_temp
 .withColumn('mean_temp', sf.mean('temperature').over(wspec))
 .withColumn('window_start', sf.first('mid').over(wspec))
 .withColumn('window_end', sf.last('mid').over(wspec))
 .sort('mid')
 .show())

# COMMAND ----------

# MAGIC %md #### partitionBy AND orderBy
# MAGIC Rolling means within each month

# COMMAND ----------

wspec = Window.partitionBy('month').orderBy('mid') # order of orderBy and partitionBy clause doesn't matter
(ddf_temp
 .withColumn('mean_temp', sf.mean('temperature').over(wspec))
 .withColumn('window_start', sf.first('mid').over(wspec))
 .withColumn('window_end', sf.last('mid').over(wspec))
 .sort('mid')
 .show())

# COMMAND ----------

# MAGIC %md ### 4.3 rowsBetween()
# MAGIC - specify size of window in terms of rows before and after a row in ordering

# COMMAND ----------

import sys

# COMMAND ----------

-sys.maxsize, sys.maxsize

# COMMAND ----------

wspec = Window.orderBy('mid').rowsBetween(-1, 0)
(ddf_temp
 .withColumn('mean_temp', sf.mean('temperature').over(wspec))
 .withColumn('window_start', sf.first('mid').over(wspec))
 .withColumn('window_end', sf.last('mid').over(wspec))
 .withColumn('temperature_minus_last', sf.col('temperature') - sf.last('temperature').over(wspec)) # why all zero?
 .sort('mid')
 .show())

# COMMAND ----------

# MAGIC %md `orderBy(col)` without row specification is implicitly `orderBy(col).rowsBetween(-sys.maxsize, 0)`

# COMMAND ----------

# MAGIC %md ### 4.4 rowsBetween() vs rangeBetween()

# COMMAND ----------

# MAGIC %md rowsBetween: this row and the next row

# COMMAND ----------

wspec = Window.orderBy('month').rowsBetween(0, 1)
(ddf_temp
 .withColumn('window_start_mid', sf.first('mid').over(wspec))
 .withColumn('window_end_mid', sf.last('mid').over(wspec))
 .withColumn('max_temp', sf.max('temperature').over(wspec))
 .show())

# COMMAND ----------

# MAGIC %md rangeBetween: this value and the next value (of the column specified in orderBy)

# COMMAND ----------

wspec = Window.orderBy('month').rangeBetween(0, 1)
(ddf_temp
 .withColumn('window_start_mid', sf.first('mid').over(wspec))
 .withColumn('window_end_mid', sf.last('mid').over(wspec))
 .withColumn('max_temp', sf.max('temperature').over(wspec))
 .show())

# COMMAND ----------

# MAGIC %md ### 4.5 lead() and lag()
# MAGIC - access specific rows before or after a row
# MAGIC - can access data outside of window

# COMMAND ----------

wspec = Window.orderBy('mid')
(ddf_temp
 .withColumn('prev_temp', sf.lag('temperature').over(wspec))
 .withColumn('next_in_two_temp', sf.lead('temperature', count=2).over(wspec)) # why last 2 null?
 .show())

# COMMAND ----------

# MAGIC %md ### *Exercise A*
# MAGIC 2. Add a column with the average temperature of the month
# MAGIC 3. Compute the temperature delta with the previous measurement
# MAGIC 1. Exclude rows of months with an average temperature below 5 degrees 

# COMMAND ----------

from pyspark.sql import Row, Window, functions as sf

# COMMAND ----------

ddf_temp = spark.createDataFrame([Row(mid=1, month=1.0, temperature=3.0),
 Row(mid=2, month=1.0, temperature=6.0),
 Row(mid=3, month=2.0, temperature=4.0),
 Row(mid=4, month=3.0, temperature=8.0),
 Row(mid=5, month=3.0, temperature=9.0),
 Row(mid=6, month=3.0, temperature=8.0),
 Row(mid=7, month=3.0, temperature=12.0)])

# COMMAND ----------

wspec = Window.partitionBy('month')
ddf_temp = ddf_temp.withColumn('mean_temp_month', sf.mean('temperature').over(wspec))

# COMMAND ----------

wspec = Window.orderBy('mid')
ddf_temp = ddf_temp.withColumn('temp_delta', sf.col('temperature') - sf.lag('temperature').over(wspec))

# COMMAND ----------

ddf_temp.filter(sf.col('mean_temp_month') > 5).show()

# COMMAND ----------

# MAGIC %md ### *Exercise B*
# MAGIC 1. Demean the flight delays partitioning by year;
# MAGIC 2. Demean the flight delays partitioning by year/carrier;
# MAGIC 3. For each year, find the carriers with the most flights cancelled;
# MAGIC 4. Same as 3., but normalize by number of flights;
# MAGIC 5. Per airline, find the airport with the most delays due to security reasons in a given year/month.

# COMMAND ----------

ddf_air = spark.read.load('/FileStore/tables/32u1wgs71485526830279')

# COMMAND ----------

# 1
window = (Window.partitionBy(ddf_air['year']))

(ddf_air.dropna(subset='arr_delay')
        .select('year', 'carrier', ddf_air['arr_delay'] - sf.avg('arr_delay').over(window))).show(5)

# COMMAND ----------

# 2
window = (Window.partitionBy(ddf_air['year'], ddf_air['month']))
(ddf_air.dropna(subset='arr_delay')
        .select('year', 'month', 'carrier', ddf_air['arr_delay'] - sf.avg('arr_delay').over(window))).show(5)

# COMMAND ----------

# 3
cancelled_ddf_air = (ddf_air.dropna(subset='arr_cancelled')
                              .select('year', 'carrier', 'arr_cancelled')
                              .groupby('year', 'carrier').agg({'arr_cancelled': 'sum'})
                              .withColumnRenamed('sum(arr_cancelled)', 'cancelled'))

window = (Window.partitionBy(cancelled_ddf_air['year'])).orderBy(cancelled_ddf_air['cancelled'].desc())
ranked_c_ddf_air = cancelled_ddf_air.select('year', 'carrier', 'cancelled', sf.rank().over(window).alias('n'))

ranked_c_ddf_air.filter(ranked_c_ddf_air['n'] < 5).show(5)

# COMMAND ----------

# 4
cancelled_ddf_air = (ddf_air.dropna(subset='arr_cancelled')
                              .select('year', 'carrier', 'arr_cancelled', 'arr_flights')
                              .groupby('year', 'carrier').agg({'arr_cancelled': 'sum', 'arr_flights': 'sum'})
                              .withColumnRenamed('sum(arr_cancelled)', 'cancelled')
                              .withColumnRenamed('sum(arr_flights)', 'flights')
                              .selectExpr('year', 'carrier', 'cancelled/flights')
                              .withColumnRenamed('(cancelled / flights)', 'cancelled_pct'))

window = (Window.partitionBy(cancelled_ddf_air['year'])).orderBy(cancelled_ddf_air['cancelled_pct'].desc())
ranked_c_ddf_air = cancelled_ddf_air.select('year', 'carrier', 'cancelled_pct', sf.rank().over(window).alias('n'))

ranked_c_ddf_air.filter(ranked_c_ddf_air['n'] < 5).show(5)

# COMMAND ----------

# 5. Per airline, find the airport with the most delays due to security reasons in a given year/month.
window = (Window.partitionBy(ddf_air['carrier'])).orderBy(ddf_air['security_delay'].desc())
sec = ddf_air.dropna().select('carrier', 'airport', 'security_delay', sf.rank().over(window).alias('n'))
sec.filter(sec['n'] == 1).show(5)

# COMMAND ----------

