# Databricks notebook source
# MAGIC %md # Broadcast variables

# COMMAND ----------

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

## Solution 1 Step 1
# Type b to add a cell below
# Enter to start coding
scores = {"A":1,
"B":3,
"C":3,
"D":2,
"E":1,
"F":4,
"G":2,
"H":4,
"I":1,
"J":8,
"K":5,
"L":1,
"M":3,
"N":1,
"O":1,
"P":3,
"Q":10,
"R":1,
"S":1,
"T":1,
"U":1,
"V":4,
"W":4,
"X":8,
"Y":4,
"Z":10}
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md ##### Step 2
# MAGIC 
# MAGIC Create a broadcast variable called scoresbc

# COMMAND ----------

## Solution 1 Step 2
# Type b to add a cell below
# Enter to start coding
scoresbc = sc.broadcast(scores)
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md ##### Step 3
# MAGIC 
# MAGIC Create a RDD called words from the file `/home/cloudera/ds-with-spark/data/words`

# COMMAND ----------

## Solution 1 Step 3
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

## Solution 1 Step 4
# Type b to add a cell below
# Enter to start coding
letters = words.flatMap(lambda word: [(letter.upper(), word) for letter in word])
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md ##### Step 5
# MAGIC Create a RDD called word_scores by transforming the joined RDD into a RDD with the sum of scores from all letters a word consists of

# COMMAND ----------

## Solution 1 Step 5
# Type b to add a cell below
# Enter to start coding
word_scores = letters.map(lambda (letter, word): (word, int(scoresbc.value.get(letter, 0))))\
.reduceByKey(lambda v1, v2: v1 + v2)
# Shift + Enter to execute the code

# COMMAND ----------

# MAGIC %md ##### Step 6
# MAGIC Order the RDD and take the first element to find out the highest scoring word

# COMMAND ----------

## Solution 1 Step 6
# Type b to add a cell below
# Enter to start coding
word_scores.sortBy(lambda (key, value): value, False).first()
# Shift + Enter to execute the code

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

# COMMAND ----------

## Solution 2
# Type b to add a cell below
# Enter to start coding
persisted_scores = word_scores.sortBy(lambda (key, value): value, False)
persisted_scores.persist()
persisted_scores.count()

print persisted_scores.toDebugString()
# Shift + Enter to execute the code

# COMMAND ----------

