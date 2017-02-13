# Databricks notebook source
dir_root = '/FileStore/tables/eknyuewv1485525522232/'

# COMMAND ----------

from pyspark.sql import functions as sf

# COMMAND ----------

# MAGIC %md # DataFrames

# COMMAND ----------

# MAGIC %md ## 0. Why Spark dataframes?

# COMMAND ----------

# MAGIC %md - #### Schema (strongly typed)

# COMMAND ----------

# MAGIC %md - #### Optimizations via Catalyst
# MAGIC     - like SQL query planner + physical plan
# MAGIC     - more declarative
# MAGIC     - e.g. filter push down

# COMMAND ----------

# MAGIC %md 
# MAGIC * #### SPEED
# MAGIC 
# MAGIC <img src="images/dataframe-api-speed.png" width="80%" />

# COMMAND ----------

# MAGIC %md - #### Familiar to SQL users
# MAGIC     - similar expressions
# MAGIC 
# MAGIC - #### Simpler code

# COMMAND ----------

# MAGIC %md *RDDs*
# MAGIC ```python
# MAGIC (rdd.map(lambda x: (x[0], (x[1], 1)))
# MAGIC     .reduceByKey(sum_pair)
# MAGIC     .mapValues(lambda s: s[0] / s[1]))
# MAGIC ```
# MAGIC 
# MAGIC Where
# MAGIC 
# MAGIC ```python    
# MAGIC def sum_pair(pair):
# MAGIC     x, y = pair
# MAGIC     return (x[0] + y[0], x[1] + y[1])
# MAGIC ```

# COMMAND ----------

# MAGIC %md *DataFrame API*
# MAGIC ```python
# MAGIC (ddf.groupBy('name')
# MAGIC     .agg({'age': 'avg'})
# MAGIC     .collect())
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## 1. Getting data

# COMMAND ----------

# MAGIC %md ### 1.1 Create

# COMMAND ----------

# MAGIC %md #### From rdd to dataframe

# COMMAND ----------

rdd = sc.parallelize([[None, 'Michael'],
                      [30, 'Andy'],
                      [19, 'Justin'],
                      [30, 'James Dr No From Russia with Love Bond']])
rdd.collect()

# COMMAND ----------

rdd.toDF()

# COMMAND ----------

ddf = rdd.toDF() # Henk: sdf
ddf.show()

# COMMAND ----------

# MAGIC %md #### Directly from python

# COMMAND ----------

ddf = spark.createDataFrame([[None, 'Michael'],
                             [30, 'Andy'],
                             [19, 'Justin'],
                             [30, 'James Dr No From Russia with Love Bond']], 
                            schema = ['age', 'name'])

# COMMAND ----------

ddf.show()

# COMMAND ----------

ddf.columns

# COMMAND ----------

ddf.dtypes

# COMMAND ----------

ddf.schema

# COMMAND ----------

ddf.printSchema()

# COMMAND ----------

# MAGIC %md #### Still an rdd

# COMMAND ----------

ddf.first()

# COMMAND ----------

ddf.rdd.map(lambda r: r['age'] + 1 if r['age'] != None else r['age']).collect()

# COMMAND ----------

# MAGIC %md #### If your name is too long

# COMMAND ----------

# MAGIC %md Mr. David Feirn
# MAGIC 
# MAGIC ```James Dr No From Russia with Love Goldfinger Thunderball You Only Live Twice On Her Majestys Secret Service Diamonds Are Forever Live and Let Die The Man with the Golden Gun The Spy Who Loved Me Moonraker For Your Eyes Only Octopussy A View to a Kill The Living Daylights Licence to Kill Golden Eye Tomorrow Never Dies The World Is Not Enough Die Another Day Casino Royale Bond```

# COMMAND ----------

ddf.show()

# COMMAND ----------

ddf.show(n=10, truncate=False)

# COMMAND ----------

# MAGIC %md ### 1.2 Importing data

# COMMAND ----------

# MAGIC %md #### Hive

# COMMAND ----------

#spark.read.table('db.table_name')

# COMMAND ----------

# MAGIC %md #### CSV

# COMMAND ----------

spark.read.csv(dir_root + 'heroes.csv').show(3)

# COMMAND ----------

ddf = spark.read.csv(dir_root + 'heroes.csv', header=True)
ddf.show(3)

# COMMAND ----------

ddf.columns

# COMMAND ----------

ddf.select([c for c in ddf.columns if c != '_c0'])

# COMMAND ----------

# MAGIC %md #### Pandas

# COMMAND ----------

import pandas as pd

# COMMAND ----------

df = pd.read_csv(dir_root + 'heroes.csv', index_col = 0)

# COMMAND ----------

ddf = spark.createDataFrame(df)
ddf.show(3)

# COMMAND ----------

ddf.toPandas().head(3)

# COMMAND ----------

# MAGIC %md #### Parquet
# MAGIC - preferred format
# MAGIC     - small file size (efficient compression)
# MAGIC     - schema
# MAGIC     - works accross machines (unlike pickle)
# MAGIC    

# COMMAND ----------

#ddf = spark.read.parquet('/FileStore/tables/32u1wgs71485526830279')

# COMMAND ----------

# MAGIC %md #### Other formats
# MAGIC 
# MAGIC * Jdbc;
# MAGIC * HDFS;
# MAGIC * Avro*;
# MAGIC * HBase*;
# MAGIC * Cassandra*;
# MAGIC * etc.
# MAGIC 
# MAGIC ${}^*$ external

# COMMAND ----------

# MAGIC %md ## 2. Basics (do stuff)
# MAGIC - `filter`
# MAGIC - `select`
# MAGIC - `sort`
# MAGIC - `groupBy`

# COMMAND ----------

# MAGIC %md ### 2.1 select()
# MAGIC - select columns
# MAGIC - make new columns

# COMMAND ----------

ddf = spark.createDataFrame([[None, 'Michael'],
                             [30, 'Andy'],
                             [19, 'Justin'],
                             [30, 'James Dr No From Russia with Love Bond']], 
                            schema = ['age', 'name'])

# COMMAND ----------

ddf.show()

# COMMAND ----------

# MAGIC %md #### Selecting existing cols

# COMMAND ----------

from pyspark.sql import functions as sf # Spark functions

# COMMAND ----------

(ddf.select('name', # if possible
            ddf.name, # no
            ddf['name'], # no
            sf.col('name')) # all other cases
    .show())

# COMMAND ----------

# MAGIC %md #### Create new columns

# COMMAND ----------

(ddf.select('*',
            sf.col('age') + 1,
            sf.sqrt('age')) # apply Spark functionf
    .show())

# COMMAND ----------

ddf2 = ddf.select(ddf.age + 1)
ddf2.columns

# COMMAND ----------

(ddf2.select('(age + 1)')
     .show())

# COMMAND ----------

# MAGIC %md #### Naming new columns

# COMMAND ----------

(ddf.select((ddf.age + 1).alias('age_inc'))
   .show())

# COMMAND ----------

(ddf.withColumn('age_inc', ddf.age + 1)
    .show())

# COMMAND ----------

# MAGIC %md #### *Intermezzo: query formatting*

# COMMAND ----------

# MAGIC %md Henk
# MAGIC ```python
# MAGIC (
# MAGIC     ddf.groupBy('name')
# MAGIC     .agg({'age': 'avg'})
# MAGIC     .collect()
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC Some others
# MAGIC ```python
# MAGIC (ddf.groupBy('name')
# MAGIC     .agg({'age': 'avg'})
# MAGIC     .collect())
# MAGIC ```
# MAGIC 
# MAGIC Jelte
# MAGIC ```python
# MAGIC (ddf
# MAGIC  .groupBy('name')
# MAGIC  .agg({'age': 'avg'})
# MAGIC  .collect())
# MAGIC ```

# COMMAND ----------

# MAGIC %md ### 2.2 sort()

# COMMAND ----------

(ddf
 .sort('age')
 .show())

# COMMAND ----------

(ddf
 .sort(sf.col('age').desc())
 .show())

# COMMAND ----------

# MAGIC %md ### 2.3 filter()

# COMMAND ----------

(ddf
 .filter(sf.col('age') > 21)
 .show())

# COMMAND ----------

(ddf
 .filter((sf.col('age') > 21) &
         (sf.col('name') != 'Andy'))
 .show())

# COMMAND ----------

# MAGIC %md ### 2.4 groupBy() -> agg() (aggregate)

# COMMAND ----------

(ddf
 .groupBy("age")
 .count()
 .show())

# COMMAND ----------

(ddf
 .groupBy("age")
 .agg({'age': 'max', 'age': 'first', 'age': 'stddev'}) # why does this happen?
 .show())

# COMMAND ----------

(ddf
 .groupBy("age")
 .agg(sf.max('age').alias('max_age'),
      sf.first('age').alias('first_age'),
      sf.stddev('age').alias('stddev_age'))
 .show())

# COMMAND ----------

# MAGIC %md ### *Exercises*

# COMMAND ----------

# MAGIC %md 1. Fix the Heroes of the Storm dataset such that it loads column names
# MAGIC 2. check the dtypes: what do you notice?
# MAGIC 3. load the dataset again using pandas 
# MAGIC 4. transform the pandas df to a spark ddf
# MAGIC 2. Explore the data and remove corrupted rows
# MAGIC 3. Which hero has the most hp?
# MAGIC 4. Add a column with the 'attack_momentum', computed as attack * attack_spd
# MAGIC 5. Which role on average has the highest attack?
# MAGIC 6. Figure out which roles and attack_type frequently co-occur
# MAGIC 7. Deliver a dataframe with names of the heroes with the highest attack in their role 
# MAGIC 8. export to Pandas
# MAGIC 
# MAGIC Bonus
# MAGIC 9. make a function that accepts a dataframe and a list colnames. Let it return the mean and stddev of the columns 
# MAGIC 10. apply the function to the hp and attack column such that the result has columns:
# MAGIC 
# MAGIC `hp_mean, hp_stddev, attack_mean, attack_stddev`

# COMMAND ----------

from pyspark.sql import functions as sf
ddf_heroes = spark.read.csv('/FileStore/tables/eknyuewv1485525522232/heroes.csv')

# COMMAND ----------

ddf_heroes.show()

# COMMAND ----------

ddf_heroes.dtypes

# COMMAND ----------

import pandas as pd
ddf_heroes_pd = ddf_heroes.toPandas()
ddf_heroes_pd.head()

# COMMAND ----------

print(list(ddf_heroes_pd.ix[0,1:]))
mynames = ['index']+list(ddf_heroes_pd.ix[0,1:])
ddf2=spark.createDataFrame(ddf_heroes_pd,schema =mynames)#'name', 'hp', 'attack', 'attack_spd', 'attack_type', 'role'])
print("before"+str(ddf2.count()))
#ddf3=ddf2.filter((sf.col('index').isNotNull())) 
ddf2=ddf2.dropna()
print("after"+str(ddf2.count()))
ddf2.show()

# COMMAND ----------

#Which hero has the most hp?
#max_hp = ddf2.agg(sf.max('hp').alias('max_hp')).select(sf.col('max_hp')).rdd.map(lambda x: float(x['max_hp'])).collect()
from  pyspark.sql.types import IntegerType

max_hp = ddf2.agg(sf.max(sf.col('hp').cast('Int')).alias('max_hp')).select(sf.col('max_hp')).collect()[0]['max_hp']
#or
max_hp = ddf2.agg(sf.max(sf.col('hp').cast(IntegerType())).alias('max_hp')).select(sf.col('max_hp')).collect()[0]['max_hp']
print(max_hp)
#print(max_hp[0])
ddf2.filter(sf.col('hp')==max_hp).show()

# COMMAND ----------

ddf3 = ddf2.withColumn("attack_momentum",sf.col('attack').cast('Int') * sf.col('attack_spd').cast('Float'))
ddf3.show()

# COMMAND ----------

help(ddf2.filter)

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

