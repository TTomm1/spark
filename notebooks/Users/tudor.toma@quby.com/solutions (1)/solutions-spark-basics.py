# Databricks notebook source
# MAGIC %md # The spark context
# MAGIC 
# MAGIC Feel free to make a choice to run either the python or the scala shell or experiment with both.
# MAGIC 
# MAGIC ## The spark context in your shell
# MAGIC 
# MAGIC Start the shell
# MAGIC - Python:
# MAGIC     `IPYTHON=1 pyspark`
# MAGIC 
# MAGIC - Scala:
# MAGIC     `spark-shell`
# MAGIC     
# MAGIC #### Exercise 1
# MAGIC 
# MAGIC After the REPL is started type `sc` followed by `<Enter>` to see if the SparkContext is available
# MAGIC 
# MAGIC 
# MAGIC #### Exercise 2
# MAGIC Type `sc.` followed by `<Tab>` to see a list of options you have
# MAGIC 
# MAGIC ----

# COMMAND ----------

# MAGIC %md ## The spark context in your notebook
# MAGIC 
# MAGIC The following exercises can be executed from this notebook directly.

# COMMAND ----------

# MAGIC %md #### Exercise 3
# MAGIC Unlike the spark shell which initializes the SparkContext for you upon starting, this notebook is not.
# MAGIC Use the following code to create a new SparkContext in a new cell below this one.
# MAGIC ```
# MAGIC import findspark
# MAGIC findspark.init()
# MAGIC import pyspark
# MAGIC 
# MAGIC spark = pyspark.sql.SparkSession.builder \
# MAGIC           .master("local") \
# MAGIC           .getOrCreate()
# MAGIC sc = spark.sparkContext
# MAGIC ```

# COMMAND ----------

## Solution 3
# Type b to add a cell below
# Enter to start coding
import findspark
findspark.init()
import pyspark

spark = pyspark.sql.SparkSession.builder \
          .master("local") \
          .getOrCreate()
sc = spark.sparkContext
print(sc)
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md ----
# MAGIC # RDD creation
# MAGIC 
# MAGIC The following exercises can be executed from this notebook directly.
# MAGIC 
# MAGIC However feel free to experiment with this code from inside the spark-shell (in Scala or Python).

# COMMAND ----------

# MAGIC %md The most common way to create an RDD is to read data from files. The default location of files depends on the configuration of your cluster environment.
# MAGIC 
# MAGIC 
# MAGIC #### Exercise 4
# MAGIC 
# MAGIC Create a new RDD called `mywords` from the **local** file `"{data}/words".format(data=data)`
# MAGIC Use the `mywords.count()` method to verify the data is correctly read (Remember you need an action to make sure spark starts working)
# MAGIC  

# COMMAND ----------

## Solution 4
# Type b to add a cell below
# Enter to start coding
## Initialize local data dir
import os
data = os.path.abspath('/FileStore/tables/eknyuewv1485525522232')

mywords = sc.textFile("{data}/words".format(data=data))
mywords.count()
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md #### Exercise 5
# MAGIC 
# MAGIC Create a RDD called mynumbers from a list/array of numbers ranging from 1 to 10000 with 3 partitions

# COMMAND ----------

## Solution 5
# Type b to add a cell below
# Enter to start coding
mynumbers = sc.parallelize(range(1, 10000), 3)
print mynumbers.take(10)
print mynumbers.count()
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md ----
# MAGIC # RDD inspection
# MAGIC 
# MAGIC Sometimes it is usefull to inspect a part of the RDD to make sure data is read correctly and to understand the format in which the data lives inside the RDD
# MAGIC 
# MAGIC #### Exercise 6
# MAGIC 
# MAGIC Use the `mywords` RDD created in exercise 4 and visually inspect the first 10 records.

# COMMAND ----------

## Solution 6
# Type b to add a cell below
# Enter to start coding
mywords.take(10)
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md #### Exercise 7
# MAGIC 
# MAGIC Collect all data from the `mynumbers` RDD into a local variabel called local_lengths and print the contents of this variable.
# MAGIC 
# MAGIC 
# MAGIC **Question: Should you do that a lot during exploration of Terabytes of data?**

# COMMAND ----------

## Solution 7
# Type b to add a cell below
# Enter to start coding
local_lengths = mynumbers.collect()
print local_lengths
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md ----
# MAGIC # RDD transformations
# MAGIC 
# MAGIC Most of the time RDD are created by transforming other RDDs in the next exercises we will walk through a few of the API methods that can be used for that.
# MAGIC 
# MAGIC Feel free to use the `take(10)`, `collect()` or `foreach` functions to inspect the endresults
# MAGIC 
# MAGIC #### Exercise 8
# MAGIC 
# MAGIC Create a RDD trough transforming (map) the `mywords` RDD into a RDD with the length of the words and call it mylengths

# COMMAND ----------

## Solution 8
# Type b to add a cell below
# Enter to start coding
mylengths = mywords.map(lambda word: len(word))
mylengths.take(10)
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md #### Exercise 9
# MAGIC 
# MAGIC Create a RDD called `filteredwords` containing all words starting with the character `"e"` in the `mywords` RDD 

# COMMAND ----------

## Solution 9
# Type b to add a cell below
# Enter to start coding
filteredwords = mywords.filter(lambda word: word[0] == 'e')
print filteredwords.take(10)
print filteredwords.count()
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md #### Exercise 10
# MAGIC 
# MAGIC Use the following code to create the RDD called `lettercombinations`
# MAGIC 
# MAGIC ```
# MAGIC lettercombinations = filteredwords.map(lambda word: [letter for letter in word])
# MAGIC ```
# MAGIC 
# MAGIC Then create a RDD called `letters` by flattening the current structure and make a rdd with the distinct elements
# MAGIC ```python
# MAGIC [[u'e'], [u'e', u'a'], [u'e', u'a', u'c', u'h'], [u'e', u'a', u'c', u'h', u'w', u'h', u'e', u'r', u'e'], [u'e', u'a', u'g', u'e', u'r'], [u'e', u'a', u'g', u'e', u'r', u'l', u'y'], [u'e', u'a', u'g', u'e', u'r', u'n', u'e', u's', u's'], [u'e', u'a', u'g', u'l', u'e'], [u'e', u'a', u'g', u'l', u'e', u'l', u'i', u'k', u'e'], [u'e', u'a', u'g', u'l', u'e', u's', u's']]
# MAGIC ```
# MAGIC 
# MAGIC into 
# MAGIC 
# MAGIC ```python
# MAGIC [u'e', u'a', u'c', u'h', u'w', u'r', u'g', u'l', u'y', u'n', u's', u'i', u'k', ......]
# MAGIC 
# MAGIC ```

# COMMAND ----------

## Solution 10
# Type b to add a cell below
# Enter to start coding
lettercombinations = filteredwords.map(lambda word: [letter for letter in word])
letters = lettercombinations.flatMap(lambda comb: comb).distinct()
print letters.take(10)
print letters.count()
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md #### Exercise 11
# MAGIC 
# MAGIC Have a look at the lineage of the last created RDD `letters`

# COMMAND ----------

## Solution 11
# Type b to add a cell below
# Enter to start coding
print letters.toDebugString()
# Shift + Enter to execute the code

# COMMAND ----------

