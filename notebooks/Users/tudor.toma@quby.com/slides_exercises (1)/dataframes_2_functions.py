# Databricks notebook source
dir_root = '/FileStore/tables/eknyuewv1485525522232/'

# COMMAND ----------

from pyspark.sql import functions as sf
import numpy as np

# COMMAND ----------

ddf = spark.createDataFrame([[np.nan, 'John'],
                             [None, 'Michael'],
                             [30., 'Andy'],
                             [19., 'Justin'],
                             [30., 'James Dr No From Russia with Love Bond']], 
                             schema = ['age', 'name'])

# COMMAND ----------

ddf_air = spark.read.load('/FileStore/tables/32u1wgs71485526830279')

# COMMAND ----------

# MAGIC %md ## Intermezzo: laziness in Spark
# MAGIC - Transformations (lazy, Catalyst)
# MAGIC     - filter
# MAGIC     - select
# MAGIC     - join
# MAGIC     - etc. (most)
# MAGIC 
# MAGIC 
# MAGIC - Actions (actual computation)
# MAGIC     - count
# MAGIC     - show
# MAGIC     - head

# COMMAND ----------

# MAGIC %md Quick question: what would be a good moment to cache?

# COMMAND ----------

# MAGIC %md ## 3. Functions
# MAGIC - lots of functions (too many)
# MAGIC - know the fundamentals
# MAGIC - API Docs: https://spark.apache.org/docs/latest/api/python/pyspark.sql.html

# COMMAND ----------

from pyspark.sql import functions as sf

# COMMAND ----------

# MAGIC %md ### 3.1 when -> otherwise
# MAGIC 2 ways of being Andy

# COMMAND ----------

(ddf
 .withColumn('is_andy', sf.col('name') == 'Andy')
 .withColumn('is_andy2', sf.when(sf.col('name') == 'Andy', True)
                           .otherwise(False))
 .show())

# COMMAND ----------

(ddf
 .withColumn('whos_this', sf.when(sf.col('name') == 'Andy', 'Yup, Andy')
                            .when(sf.col('name') == 'Justin', 'Justin here'))
 .show())

# COMMAND ----------

# MAGIC %md ### 3.2 isin()
# MAGIC 2 ways of being Andy or Justin

# COMMAND ----------

(ddf
 .withColumn('is_andy_or_justin', (sf.col('name') == 'Andy') |
                                  (sf.col('name') == 'Justin'))
 .withColumn('is_andy_or_justin2', sf.col('name').isin('Andy', 'Justin'))
 .show())

# COMMAND ----------

ddf = ddf.withColumn('is_teen', sf.col('age').isin(list(range(20))))
ddf.show()

# COMMAND ----------

# MAGIC %md ### 3.4 lit()

# COMMAND ----------

(ddf.withColumn('5', sf.lit(5))
    .show())

# COMMAND ----------

# MAGIC %md ### 3.5 ~ (negation)

# COMMAND ----------

(ddf.withColumn('aint_no_teen', ~sf.col('is_teen'))
    .show())

# COMMAND ----------

# MAGIC %md ### Intermezzo: raw SQL
# MAGIC Henk: No

# COMMAND ----------

ddf.registerTempTable('ddf')
(spark
 .sql("SELECT age, count(*) FROM ddf GROUP BY age")
 .show())

# COMMAND ----------

# MAGIC %md ### 3.6 join()

# COMMAND ----------

ddf1 = spark.createDataFrame([[1], [2]], schema=['a'])
ddf1.show()

# COMMAND ----------

ddf2 = spark.createDataFrame([[2], [3]], schema=['a'])
ddf2.show()

# COMMAND ----------

ddf1.join(ddf2, on = ['a'], how = 'inner').show()

# COMMAND ----------

ddf1.join(ddf2, on = ddf1.a == ddf2.a).show()

# COMMAND ----------

# MAGIC %md ### 3.7 isNull() / isNotNull() and isnan()
# MAGIC Other very useful functions are `isNull()` and `isNotNull()`. They're used like this

# COMMAND ----------

(ddf.withColumn('imputed_age', sf.when(sf.col('age').isNull(), 40)
                                 .otherwise(sf.col('age')))
    .show())

# COMMAND ----------

(ddf.withColumn('imputed_age', sf.when(sf.isnan('age'), 40)
                                 .otherwise(sf.col('age')))
    .show())

# COMMAND ----------

# MAGIC %md ### 3.8 fillna()
# MAGIC - fills both null and NaN
# MAGIC - fills only 1 value

# COMMAND ----------

(ddf
 .fillna(40, subset='age')
 .show()) 

# COMMAND ----------

# MAGIC %md ### 3.9 dropna()
# MAGIC - drops both null and NaN

# COMMAND ----------

(ddf
 .groupBy('age')
 .count()
 .dropna(subset = 'age')
 .show())

# COMMAND ----------

# MAGIC %md ### 3.10 sample()
# MAGIC - possible to take subset of data toPandas

# COMMAND ----------

ddf_air = spark.read.load(dir_root + 'airlines.parquet')

# COMMAND ----------

(ddf_air.sample(False, fraction=0.0002)
        .select('year', 'month')
        .show())

# COMMAND ----------

# MAGIC %md ### 3.11 distinct() / countDistinct()

# COMMAND ----------

(ddf.distinct()
    .show())

# COMMAND ----------

(ddf.agg(sf.countDistinct('age').alias('distinct_ages'))
    .show())

# COMMAND ----------

# MAGIC %md ### 3.12 User defined functions (UDF)
# MAGIC - executed in RDD-land
# MAGIC - avoid where possible

# COMMAND ----------

from pyspark.sql.types import IntegerType
slen = sf.udf(lambda s: len(s), IntegerType())

(ddf.withColumn('name_length', slen(ddf.name))
    .show())

# COMMAND ----------

# MAGIC %md ### *Exercise*

# COMMAND ----------

# MAGIC %md 1. Explore the `ddf_air` DF, and count how many NaN's you have in each column;
# MAGIC 2. Fill the NaN with something that makes sense for each column.
# MAGIC 3. With a UDF, capture the state in the `airport_name` column (e.g. 'NY' in 'New York, NY: John F. Kennedy International') and
# MAGIC 4. make a new dataframe `ddf_states` with columns `airport, state`
# MAGIC 3. Remove duplicates from ddf_states (hint: lookup `drop_duplicates()` in the docs)
# MAGIC 3. Join `ddf_states` onto the original `ddf_air` 
# MAGIC 7. add a column weather_condition that is 
# MAGIC ```
# MAGIC 'rainy' if the `weather_delay` is greather than 1200
# MAGIC 'stormy' if in addition to this the arrival is diverted by more than 15 minutes
# MAGIC 'bright' otherwise
# MAGIC ```
# MAGIC 6. Split the DF into a train and test set sorted by time cols (hint: lookup `limit()` in the docs)

# COMMAND ----------

ddf_air = spark.read.load('/FileStore/tables/32u1wgs71485526830279')

# COMMAND ----------

ddf_air.show()

# COMMAND ----------

# MAGIC %md Columns mean:
# MAGIC 
# MAGIC * `arr_flights`: flights arrived
# MAGIC * `arr_del15`: flights delayed more than 15';
# MAGIC * `carrier_ct`: delayed by carrier;
# MAGIC * `weather_ct`: by weather;
# MAGIC * `nas_ct`: by national aviation system;
# MAGIC * `security_ct`: by security;
# MAGIC * `late_aircraft_ct`: by late aircraft arrival;
# MAGIC * `arr_cancelled`: cancelled;
# MAGIC * `arr_diverted`: deverted;
# MAGIC * `arr_delay`: total delay and then breakdown below;
# MAGIC * `carrier_delay`;
# MAGIC * `weather_delay`;
# MAGIC * `nas_delay`;
# MAGIC * `security_delay`;
# MAGIC * `late_aircraft_delay`.

# COMMAND ----------

ddf_air.dtypes

# COMMAND ----------

original_size = ddf_air.count()
print(original_size)
nonmissing = [ddf_air.select(sf.col(c)).dropna().count() for c in ddf_air.columns]

# COMMAND ----------

missing  = [original_size-current_size for current_size in nonmissing ]
missing

# COMMAND ----------

for c in ddf_air.columns:
  current_type = ddf_air.select(sf.col(c))
  inputtedval = 0
  if(current_type=='string'):
    inputtedval = 'missing'
  ddf_air.select(sf.col(c)).fillna(inputtedval,subset=c)
ddf_air.show()

# COMMAND ----------

#With a UDF, capture the state in the airport_name column (e.g. 'NY' in 'New York, NY: John F. Kennedy International') and
#make a new dataframe ddf_states with columns airport, state

capture_state = sf.udf(lambda s: s.strip().split(':')[0].split(',')[1])

ddf_air2 = ddf_air.withColumn('state', capture_state(sf.col('airport_name'))).collect()
    

# COMMAND ----------

type(ddf_air2)
ddf_air2

# COMMAND ----------

ddf_air.count()

# COMMAND ----------

#Remove duplicates from ddf_states (hint: lookup drop_duplicates() in the docs)
ddf_states = spark.createDataFrame(ddf_air2).select(sf.col('airport_name'),sf.col('state'))
ddf_states = ddf_states.drop_duplicates()
print(ddf_states.count())
ddf_states.show()

# COMMAND ----------

ddf_state_air.dtypes

# COMMAND ----------

#Join ddf_states onto the original ddf_air
#add a column weather_condition that is 
#'rainy' if the `weather_delay` is greather than 1200
#'stormy' if in addition to this the arrival is diverted by more than 15 minutes
#'bright' otherwise
ddf_state_air = ddf_states.join(ddf_air,on=['airport_name'])
print(ddf_state_air.count())
ddf_state_air = ddf_state_air.withColumn('weather_condition',sf.when(sf.col('weather_delay').cast('Double')>1200, (sf.when(sf.col('arr_diverted').cast('Double')>15,'stormy').otherwise('rainy'))).otherwise('bright'))
#ddf_state_air.show()
ddf_state_air.filter(sf.col('weather_condition')!='bright').show()

# COMMAND ----------

help(ddf_air.join )