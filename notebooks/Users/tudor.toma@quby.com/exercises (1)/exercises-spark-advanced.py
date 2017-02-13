# Databricks notebook source
# MAGIC %md # Broadcast variables

# COMMAND ----------

## Initialize scores data dir
import os
data = os.path.abspath('/FileStore/tables/eknyuewv1485525522232')
scoresdata = os.path.abspath('/FileStore/tables/m8log4dh1485528441773')


# COMMAND ----------

# MAGIC %md #### Exercise 1
# MAGIC 
# MAGIC ##### Step 1
# MAGIC 
# MAGIC Create a dict called scores with the letter as key and the value from the second column as value

# COMMAND ----------

# MAGIC %md ##### Step 2
# MAGIC 
# MAGIC Create a broadcast variable called scoresbc

# COMMAND ----------

# MAGIC %md ##### Step 3
# MAGIC 
# MAGIC Create a RDD called words from the file `"{data}/words".format(data=data)`

# COMMAND ----------

# MAGIC %md ##### Step 4
# MAGIC 
# MAGIC Create a RDD called `letters` that can be joined with the scores RDD.
# MAGIC 
# MAGIC Hint: The key needs to be a uppercase letter and you need to keep track of the word the letters came from

# COMMAND ----------

# MAGIC %md ##### Step 5
# MAGIC Create a RDD called word_scores by transforming the joined RDD into a RDD with the sum of scores from all letters a word consists of

# COMMAND ----------

# MAGIC %md ##### Step 6
# MAGIC Order the RDD and take the first element to find out the highest scoring word

# COMMAND ----------

# MAGIC %md # Persisting a RDD

# COMMAND ----------

# MAGIC %md #### Exercise 2
# MAGIC 
# MAGIC Create a new RDD by making a reference to the sorted `word_scores` RDD called `persisted_scores`,  
# MAGIC Persist this last RDD with the `persist()` method.  
# MAGIC Count the number of records to make sure the caching will take place  
# MAGIC 
# MAGIC Use the toDebugString method to verify the storage level.