# Databricks notebook source
# MAGIC %md # Numeric RDDs

# COMMAND ----------

import os
data = os.path.abspath('/FileStore/tables/eknyuewv1485525522232')
scoresdata = os.path.abspath('/FileStore/tables/m8log4dh1485528441773')

# COMMAND ----------

# MAGIC %md #### Exercise 1
# MAGIC 
# MAGIC Use the following code to create a numeric RDD with the temparatures
# MAGIC 
# MAGIC ```python
# MAGIC filepath = "{data}/weather.csv"
# MAGIC weather_rdd = sc.textFile(filepath)
# MAGIC temperature_rdd = weather_rdd.map(lambda r: float(r.split(',')[1]))
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC Use this last RDD to calculate the mean temperature and the variance

# COMMAND ----------

## Solution 1
# Type b to add a cell below
# Enter to start coding
filepath = "{data}/weather.csv".format(data=data)
weather_rdd = sc.textFile(filepath)
temperature_rdd = weather_rdd.map(lambda r: float(r.split(',')[1]))

print temperature_rdd.mean()
print temperature_rdd.variance()
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md #### Exercise 2
# MAGIC 
# MAGIC Create a histogram with 12 buckets from the `temparature_rdd` RDD

# COMMAND ----------

## Solution 2
# Type b to add a cell below
# Enter to start coding
temperature_rdd.histogram(12)
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md ----
# MAGIC # Pair RDDs

# COMMAND ----------

# MAGIC %md ## Creating PairRDDs

# COMMAND ----------

# MAGIC %md #### Exercise 3
# MAGIC 
# MAGIC Most of the time your data will have multiple columns and it's pretty easy to pick a key and a value. But our `temperature_rdd` for example doesn't.
# MAGIC 
# MAGIC It is however possible to create a key.
# MAGIC 
# MAGIC Look at the `zip*` functions in the Spark documentation and use (one of) them to create a PairRDD called `temperature_pairs`
# MAGIC 
# MAGIC You can find the Spark documentation here:
# MAGIC - [Python](https://spark.apache.org/docs/latest/api/python/index.html)
# MAGIC - [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#package)

# COMMAND ----------

## Solution 3
# Type b to add a cell below
# Enter to start coding
temperature_pairs = temperature_rdd.zipWithIndex()
print temperature_pairs.take(10)
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md #### Exercise 4
# MAGIC 
# MAGIC Use the map function of the `weather_rdd` to create a PairRDD called `weather_types` with the first column as the key and the 22nd column as the value

# COMMAND ----------

## Solution 4
# Type b to add a cell below
# Enter to start coding
weather_types = weather_rdd.map(lambda r: (r.split(',')[0], r.split(',')[21]))
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md #### Exercise 5
# MAGIC 
# MAGIC Use the keyBy function to create a new RDD called `keyed_weather_types` with the exact same structure and contents as the `weather_types`
# MAGIC 
# MAGIC You can verify this visually, but you can also use the following code to check. It should return a empty array
# MAGIC 
# MAGIC ```python
# MAGIC keyed_weather_types.join(weather_types).filter(lambda (k,(v1, v2)): v1 != v2).take(10)
# MAGIC ```

# COMMAND ----------

## Solution 5
# Type b to add a cell below
# Enter to start coding
keyed_weather_types = weather_rdd.keyBy(lambda r: r.split(',')[0]).mapValues(lambda v: v.split(',')[21])

print keyed_weather_types.join(weather_types).filter(lambda (k,(v1, v2)): v1 != v2).take(10)
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md # Working with PairRDDs

# COMMAND ----------

# MAGIC %md #### Exercise 6
# MAGIC 
# MAGIC Create a pairRDD with the weather type (column 22) as key and temperature (column 2) as value.  
# MAGIC Filter out the empty keys!!  
# MAGIC Use this RDD to determine the maximum or minimum temperature during a specific weather type.
# MAGIC 
# MAGIC Hint: reduceByKey

# COMMAND ----------

## Solution 6
# Type b to add a cell below
# Enter to start coding
weather_types_and_temp = weather_rdd.filter(lambda r: r.split(',')[21] != '') \
.map(lambda r: (r.split(',')[21], int(r.split(',')[1])))

print weather_types_and_temp.reduceByKey(min).take(10)
print weather_types_and_temp.reduceByKey(max).take(10)

# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md #### Exercise 7
# MAGIC 
# MAGIC How about the average temperature.
# MAGIC 
# MAGIC Hint: Use the combineByKey function to create a RDD with the stucture (key, (sum_of_temps, total_nr_of_temp_measures)) and map over that RDD

# COMMAND ----------

## Solution 7
# Type b to add a cell below
# Enter to start coding
print weather_types_and_temp.combineByKey(lambda temp: (int(temp),1), \
                                    lambda (t1,count), temp: (t1 + int(temp), count + 1), \
                                    lambda (t1, c1), (t2, c2): (t1 + t2, c1 + c2)) \
.map(lambda (key, (sum_of_temps, total_nr_of_temp_measures)): (key, sum_of_temps/total_nr_of_temp_measures)).take(10)

#OR use mapValues instead of MAP
print weather_types_and_temp.combineByKey(lambda temp: (int(temp),1), \
                                    lambda (t1,count), temp: (t1 + int(temp), count + 1), \
                                    lambda (t1, c1), (t2, c2): (t1 + t2, c1 + c2)) \
.mapValues(lambda (sum_of_temps, total_nr_of_temp_measures): sum_of_temps/total_nr_of_temp_measures).take(10)
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md #### Exercise 8
# MAGIC 
# MAGIC ##### Step 1
# MAGIC Create a file called scores.txt in the `{data}` directory with the following contents:
# MAGIC 
# MAGIC ```
# MAGIC A,1
# MAGIC B,3
# MAGIC C,3
# MAGIC D,2
# MAGIC E,1
# MAGIC F,4
# MAGIC G,2
# MAGIC H,4
# MAGIC I,1
# MAGIC J,8
# MAGIC K,5
# MAGIC L,1
# MAGIC M,3
# MAGIC N,1
# MAGIC O,1
# MAGIC P,3
# MAGIC Q,10
# MAGIC R,1
# MAGIC S,1
# MAGIC T,1
# MAGIC U,1
# MAGIC V,4
# MAGIC W,4
# MAGIC X,8
# MAGIC Y,4
# MAGIC Z,10
# MAGIC ```
# MAGIC 
# MAGIC These are the scores per letter in the Scrabble game.

# COMMAND ----------

## Solution 8 Step 1
# Type b to add a cell below
# Enter to start coding

# !!In the Terminal!! #
#cd <path to git checkout dir>/ds-with-spark/data
#vi scores.txt
# i <enter> ctrl+shift+V to paste the scores
#:q to exit vi

# COMMAND ----------

# MAGIC %md ##### Step 2
# MAGIC 
# MAGIC Create a RDD called scores with the letter as key and the integer value from the second column as value

# COMMAND ----------

## Solution 8 Step 2
# Type b to add a cell below
# Enter to start coding
scores = sc.textFile('{data}/scores.txt'.format(data=scoresdata))\
.map(lambda r: (r.split(',')[0], int(r.split(',')[1])))
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md ##### Step 3
# MAGIC 
# MAGIC Create a RDD called words from the file `{data}/words`

# COMMAND ----------

## Solution 8 Step 3
# Type b to add a cell below
# Enter to start coding
words = sc.textFile('{data}/words'.format(data=data))
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md ##### Step 4
# MAGIC 
# MAGIC Create a RDD called `letters` that can be joined with the scores RDD.
# MAGIC 
# MAGIC Hint: The key needs to be a uppercase letter and you need to keep track of the word the letters came from

# COMMAND ----------

## Solution 8 Step 4
# Type b to add a cell below
# Enter to start coding
letters = words.flatMap(lambda word: [(letter.upper(), word) for letter in word])
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md ##### Step 5
# MAGIC 
# MAGIC Join the letters and scores RDDs into a RDD called `joined`

# COMMAND ----------

## Solution 8 Step 5
# Type b to add a cell below
# Enter to start coding
joined = letters.join(scores)
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md ##### Step 6
# MAGIC Create a RDD called word_scores by transforming the joined RDD into a RDD with the sum of scores from all letters a word consists of

# COMMAND ----------

## Solution 8 Step 6
# Type b to add a cell below
# Enter to start coding
word_scores = joined.map(lambda (key, (word, score)): (word, score)).reduceByKey(lambda v1, v2: v1 + v2)
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md ##### Step 7
# MAGIC Order the RDD and take the first element to find out the highest scoring word

# COMMAND ----------

## Solution 8 Step 7
# Type b to add a cell below
# Enter to start coding
word_scores.sortBy(lambda (key, value): value, False).first()
# Shift + Enter to execute the code

# COMMAND ----------

