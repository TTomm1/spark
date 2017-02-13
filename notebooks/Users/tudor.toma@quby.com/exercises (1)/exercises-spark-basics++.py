# Databricks notebook source
# MAGIC %md # Numeric RDDs

# COMMAND ----------

## Initialize local data dir
import os
data = os.path.abspath('/FileStore/tables/eknyuewv1485525522232')
scoresdata = os.path.abspath('/FileStore/tables/m8log4dh1485528441773')


# COMMAND ----------

# MAGIC %md #### Exercise 1
# MAGIC 
# MAGIC Use the following code to create a numeric RDD with the temparatures
# MAGIC 
# MAGIC ```python
# MAGIC filepath = "{data}/weather.csv".format(data=data)
# MAGIC weather_rdd = sc.textFile(filepath)
# MAGIC temperature_rdd = weather_rdd.map(lambda r: float(r.split(',')[1]))
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC Use this last RDD to calculate the mean temperature and the variance

# COMMAND ----------

filepath = "{data}/weather.csv".format(data=data)
weather_rdd = sc.textFile(filepath)
temperature_rdd = weather_rdd.map(lambda r: float(r.split(',')[1]))

# COMMAND ----------

temperature_rdd.take(10)
temperature_rdd.count()

# COMMAND ----------

# MAGIC %md #### Exercise 2
# MAGIC 
# MAGIC Create a histogram with 12 buckets from the `temparature_rdd` RDD

# COMMAND ----------

myhist= temperature_rdd.histogram(buckets=12)
print(myhist)
type(myhist)
myhist[0]

# COMMAND ----------

import matplotlib.pyplot as plt
fig = plt.figure()
myplot = plt.plot(myhist[0][1:],myhist[1])

# COMMAND ----------

display(fig)

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

len(temperature_rdd.collect())

# COMMAND ----------

import numpy as np
keys = sc.parallelize(np.arange(0,len(temperature_rdd.collect()),1),temperature_rdd.getNumPartitions())
temperature_pairs = keys.zip(temperature_rdd)

temperature_pairs.take(10)

# COMMAND ----------

# MAGIC %md #### Exercise 4
# MAGIC 
# MAGIC Use the map function of the `weather_rdd` to create a PairRDD called `weather_types` with the first column as the key and the 22nd column as the value

# COMMAND ----------

"this ,is , a , test".split(',')

# COMMAND ----------

weather_rdd.take(10)
weather_rdd.map(lambda x: x.split(",")).take(10)

# COMMAND ----------

np.arange(0,21,1)
[index for index in np.arange(0,22,1) if index == 0 ]

# COMMAND ----------

weather_types = weather_rdd.map(lambda x: x.split(",")).map(lambda x:[x[index]  for index in np.arange(0,22,1) if index == 0 or index==21]).keyBy(lambda x: x[0]).mapValues(lambda x: x[1])

print(weather_types.take(10))


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

keyed_weather_types = weather_types.keyBy(lambda x:x[1])
print(keyed_weather_types.take(10))
keyed_weather_types.join(weather_types).filter(lambda (k,(v1, v2)): v1 != v2).take(10)

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

weather_type_temp = weather_rdd.map(lambda x: x.split(",")).map(lambda x:[x[index]  for index in np.arange(0,22,1) if index == 1 or index == 21]).filter(lambda x: x[1]!='').keyBy(lambda x: x[1]).mapValues(lambda x: x[0])
print(weather_type_temp.take(10))
print(set(weather_type_temp.keys().collect()))


# COMMAND ----------

min_temp = weather_type_temp.reduceByKey(lambda a,b: min(float(a),float(b)))
#max_tem[]
print(min_temp.take(10))
max_temp = weather_type_temp.reduceByKey(lambda a,b: max(float(a),float(b)))
print(max_temp.take(10))
#import gc
#del min
#gc.collect()

# COMMAND ----------

# MAGIC %md #### Exercise 7
# MAGIC 
# MAGIC How about the average temperature.
# MAGIC 
# MAGIC Hint: Use the combineByKey function to create a RDD with the stucture (key, (sum_of_temps, total_nr_of_temp_measures)) and map over that RDD

# COMMAND ----------

min_max_temp=max_temp.join(min_temp)

# COMMAND ----------

min_max_temp.take(10)

# COMMAND ----------

wtt_mean=weather_type_temp.combineByKey(lambda x:(float(x),1),
                               lambda x,y: (x[0]+float(y),x[1]+1),
                               lambda x,y: (x[0]+y[0],x[1]+y[1])
                         ).mapValues(lambda (y,z):float(y)/z)
wtt_mean.collect()

# COMMAND ----------

# MAGIC %md #### Exercise 8
# MAGIC 
# MAGIC ##### Step 1
# MAGIC Create a file called scores.txt in the `data` directory with the following contents:
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

import io
#print(data)
filecontent = " A,1\nB,3\nC,3\nD,2\nE,1\nF,4\nG,2\nH,4\nI,1\nJ,8\nK,5\nL,1\nM,3\nN,1\nO,1\nP,3\nQ,10\nR,1\nS,1\nT,1\nU,1\nV,4\nW,4\nX,8\nY,4\nZ,10"
with io.FileIO('tudor_scores.txt', 'w') as file:
    file.write(filecontent)



# COMMAND ----------

# MAGIC %md ##### Step 2
# MAGIC 
# MAGIC Create a RDD called scores with the letter as key and the integer value from the second column as value

# COMMAND ----------

scores = sc.textFile('{data}/scores.txt'.format(data=scoresdata)).map(lambda r: (r.split(',')[0], int(r.split(',')[1])))
scores.collect()

# COMMAND ----------

# MAGIC %md ##### Step 3
# MAGIC 
# MAGIC Create a RDD called words from the file `'{data}/words'.format(data=data)`

# COMMAND ----------

words = sc.textFile('{data}/words'.format(data=data))
words.take(10)

# COMMAND ----------

# MAGIC %md ##### Step 4
# MAGIC 
# MAGIC Create a RDD called `letters` that can be joined with the scores RDD.
# MAGIC 
# MAGIC Hint: The key needs to be a uppercase letter and you need to keep track of the word the letters came from

# COMMAND ----------

testing = sc.parallelize(words.take(10))
print(testing.collect())
testing2=testing.map(lambda x: (x[0].upper(),x)).reduceByKey(lambda x,y: x+' '+y)
print(testing2.collect())
testing3 = testing.map(lambda a: (a[0].upper(),a)).aggregateByKey([],lambda x,y: x+[y],lambda x,y:x+y)
print(testing3.collect())

# COMMAND ----------

letters = words.map(lambda x: (x[0].upper(),x)).aggregateByKey([],lambda x,y: x+[y],lambda x,y:x+y)
letters.take(1)

# COMMAND ----------

letters = words.map(lambda x: (x,list(x)))
letters.take(10)

# COMMAND ----------

# MAGIC %md ##### Step 5
# MAGIC 
# MAGIC Join the letters and scores RDDs into a RDD called `joined`

# COMMAND ----------

letter_score = letters.join(scores)
letter_score.map(lambda x: x[1]).collect()


# COMMAND ----------

# MAGIC %md ##### Step 6
# MAGIC Create a RDD called word_scores by transforming the joined RDD into a RDD with the sum of scores from all letters a word consists of

# COMMAND ----------

letter_score.map(lambda x: x[1]).collect()
word_scores = letter_score

# COMMAND ----------

# MAGIC %md ##### Step 7
# MAGIC Order the RDD and take the first element to find out the highest scoring word