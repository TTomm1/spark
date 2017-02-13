# Databricks notebook source
# MAGIC %md # Spark applications

# COMMAND ----------

## Initialize local data dir
import os
data = os.path.abspath('/FileStore/tables/eknyuewv1485525522232')
scoresdata = os.path.abspath('/FileStore/tables/m8log4dh1485528441773')

# COMMAND ----------

# MAGIC %md #### Exercise 1
# MAGIC 
# MAGIC Pick a dataset form the data dir (or download one yourself) and write a Spark application with it.
# MAGIC 
# MAGIC run this application with `spark-submit` with different configurations of the `--master` commandline option.

# COMMAND ----------

# MAGIC %md #### Exercise 2
# MAGIC 
# MAGIC Open the Spark UI (most of the times located at http://localhost:4040) and browse through the different sections.
# MAGIC 
# MAGIC Experiment for example with:
# MAGIC 
# MAGIC - Persisting RDDs (look at the sotrage tab)
# MAGIC - Vary the number of cores in local mode (see Executors tab)
# MAGIC - Compare the toDebugString output with the Jobs and Stages tabs

# COMMAND ----------

# MAGIC %md #### Exercise 3
# MAGIC 
# MAGIC Look at the spark documentation and find out other configuration options you can set for spark-submit
# MAGIC for example:
# MAGIC 
# MAGIC - num-executors (yarn mode only)
# MAGIC - executor-memory
# MAGIC - driver-memory
# MAGIC - py-files (add external files the job needs)