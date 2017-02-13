# Databricks notebook source
dir_root = '/FileStore/tables/eknyuewv1485525522232/'
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

ddf_temp = spark.createDataFrame([Row(mid=1, month=1.0, temperature=3.0),
 Row(mid=2, month=1.0, temperature=6.0),
 Row(mid=3, month=2.0, temperature=4.0),
 Row(mid=4, month=3.0, temperature=8.0),
 Row(mid=5, month=3.0, temperature=9.0),
 Row(mid=6, month=3.0, temperature=8.0),
 Row(mid=7, month=3.0, temperature=12.0)])

# COMMAND ----------

ddf_temp.show()

# COMMAND ----------

wspec = Window.partitionBy('month')
(ddf_temp.withColumn('average_temp',sf.mean('temperature').over(wspec))).show()
ddf_temp2 = ddf_temp.withColumn('average_temp',sf.mean('temperature').over(wspec))
wspec = Window.orderBy('mid').partitionBy('month')
ddf_temp3 = ddf_temp2.withColumn('delta_prev_temp',sf.col('temperature')-sf.lag('temperature').over(wspec))
ddf_temp3.show()

# COMMAND ----------

ddf_temp3.filter(sf.col('average_temp')>=5).show()

# COMMAND ----------

# MAGIC %md ### *Exercise B*
# MAGIC 1. Demean the flight delays partitioning by year;
# MAGIC 2. Demean the flight delays partitioning by year/carrier;
# MAGIC 3. For each year, find the carriers with the most flights cancelled;
# MAGIC 4. Same as 3., but normalize by number of flights;
# MAGIC 5. Per airline, find the airport with the most delays due to security reasons in a given year/month.

# COMMAND ----------

ddf_air = spark.read.parquet('/FileStore/tables/32u1wgs71485526830279')

# COMMAND ----------

ddf_air.show()

# COMMAND ----------

wspec = Window.partitionBy('year')
ddf_air = ddf_air.fillna(0,['arr_delay'])
ddf_air2 = ddf_air.withColumn('demeaned_delay',sf.col('arr_delay').cast('Double')-sf.mean(sf.col('arr_delay').cast('Double')).over(wspec))
ddf_air2.show()

# COMMAND ----------

wspec = Window.partitionBy(['year','carrier'])
ddf_air2 = ddf_air.withColumn('demeaned_delay',sf.col('arr_delay').cast('Double')-sf.mean(sf.col('arr_delay').cast('Double')).over(wspec))
ddf_air2.show()

# COMMAND ----------

ddf_air.select('arr_cancelled').show()

# COMMAND ----------

wspec = Window.partitionBy(['year','carrier'])
ddf_air=ddf_air.fillna(0,['arr_cancelled'])
ddf_air2 = ddf_air.withColumn('number_cancelations', sf.sum(sf.col('arr_cancelled').cast('Double')).over(wspec)).select(['year','carrier','number_cancelations']).drop_duplicates().sort(sf.col('year').desc())
#ddf_air2.show()
wspec = Window.partitionBy('year').orderBy('year')
ddf_air2.groupBy('year').agg(sf.max('number_cancelations')).show()
ddf_air3 = ddf_air2.withColumn('max_number_cancelations_per_year',sf.max('number_cancelations').over(wspec)).filter(sf.col('number_cancelations')==sf.col('max_number_cancelations_per_year'))
ddf_air3.show()


# COMMAND ----------

wspec = Window.partitionBy(['year','carrier'])
ddf_air = ddf_air.fillna(0,['arr_flights'])
ddf_air4 =ddf_air.withColumn('number_cancelations', sf.sum(sf.col('arr_cancelled').cast('Double')).over(wspec)).withColumn('number_flights',sf.sum(sf.col('arr_flights').cast('Double')).over(wspec)).select(['year','carrier','number_cancelations','number_flights']).drop_duplicates().sort(sf.col('year').desc())
ddf_air4 = ddf_air4.withColumn('normalized_cancelations_by_flights',sf.col('number_cancelations').cast('Double')/sf.col('number_flights').cast('Double'))
ddf_air4.show()
wspec = Window.partitionBy('year').orderBy('year')
ddf_air4 = ddf_air4.withColumn('max_number_cancelations_per_year',sf.max('normalized_cancelations_by_flights').over(wspec)).filter(sf.col('normalized_cancelations_by_flights')==sf.col('max_number_cancelations_per_year'))

ddf_air4.show()

# COMMAND ----------

ddf_air.show()

# COMMAND ----------

#Per airline, find the airport with the most delays due to security reasons in a given year/month.
wspec = Window.partitionBy(['carrier','year','month','airport'])
ddf_air = ddf_air.fillna(0,['security_delay'])
ddf_air2 = ddf_air.withColumn('total_security_delays',sf.sum(sf.col('security_delay').cast('Double')).over(wspec)).select(['airport','carrier','year','month','total_security_delays']).drop_duplicates()
#ddf_air2.show()
#ddf_air5 = #ddf_air2.groupBy(['carrier','year','month']).agg(sf.max(sf.col('total_security_delays')).alias('max_total_security_delays')).sort(['carrier','year','month'])

#ddf_air5.show()
wspec2=Window.partitionBy(['carrier','year','month'])
ddf_air5=ddf_air2.withColumn('max_total_security_delays',sf.max(sf.col('total_security_delays').cast('Double')).over(wspec2)).sort(['carrier','year','month'])

#ddf_air5.withColumn('max_sec_delays_of_an_airport',sf.max(sf.col()))
ddf_air5 =ddf_air5.filter(sf.col("total_security_delays")==sf.col("max_total_security_delays")).sort(['carrier','year','month'])
ddf_air5.select(['carrier','year','month','total_security_delays','airport']).filter((sf.col('year')==2016)&(sf.col('month')==1)).show()

# COMMAND ----------

print(ddf_air5.count())
display(ddf_air5.select(['carrier','year','month','total_security_delays','airport']).filter((sf.col('year')==2015)))#&(sf.col('month')==1)))

# COMMAND ----------

# MAGIC %md ## 5. Output: storing your results

# COMMAND ----------

# MAGIC %md ### Hive table

# COMMAND ----------

ddf_temp.write.saveAsTable('db.table_name')

# COMMAND ----------

# MAGIC %md ### Parquet

# COMMAND ----------

ddf_temp.write.save(path = 'ddf',
               format = 'parquet', #json 
               mode = 'overwrite')

# COMMAND ----------

# MAGIC %md ### Local or HDFS?

# COMMAND ----------

local_prepend = 'file://'
hdfs_prepend = 'hdfs://'

# COMMAND ----------

ddf_temp.write.save('file://' + '/home/jelte/ddf')

# COMMAND ----------

# MAGIC %md ### coalesce

# COMMAND ----------

ddf_temp.coalesce(1).write.save()