# Databricks notebook source
# MAGIC %md # The spark shell
# MAGIC 
# MAGIC Feel free to make a choice to run either the python or the scala shell or experiment with both.
# MAGIC 
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
# MAGIC How to get help in the REPL
# MAGIC 
# MAGIC Python
# MAGIC ```
# MAGIC help()
# MAGIC ```
# MAGIC 
# MAGIC Scala
# MAGIC ```
# MAGIC :help
# MAGIC ```
# MAGIC 
# MAGIC Q: Whats the difference between them.
# MAGIC 
# MAGIC #### Exercise 2
# MAGIC 
# MAGIC Try out some of the functional constructs mentioned in the lecture
# MAGIC 
# MAGIC #### Exercise 2A
# MAGIC 
# MAGIC Python
# MAGIC ```
# MAGIC animals = ["Dog", "Cat", "Monkey"]
# MAGIC 
# MAGIC #convert all names to uppercase
# MAGIC 
# MAGIC all_upper = ...
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC Scala
# MAGIC ```
# MAGIC val animals = List("Dog", "Cat", "Monkey")
# MAGIC 
# MAGIC // convert all names to uppercase
# MAGIC 
# MAGIC val all_upper = ...
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC #### Exercise 2B
# MAGIC 
# MAGIC Python
# MAGIC ```
# MAGIC animals = ["Dog", "Cat", "Monkey"]
# MAGIC 
# MAGIC #Filter names without 'o' in them
# MAGIC 
# MAGIC only_o = ...
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC Scala
# MAGIC ```
# MAGIC val animals = List("Dog", "Cat", "Monkey")
# MAGIC 
# MAGIC // Filter names without 'o' in them
# MAGIC 
# MAGIC val only_o = ...
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC #### Exercise 2C
# MAGIC 
# MAGIC Python
# MAGIC ```
# MAGIC iot = [["Dog","Cat","Monkey"],["Apple","Orange","Lemon"],["Car","Bike","Motor"]]
# MAGIC 
# MAGIC #Flatten this to a single list of things
# MAGIC 
# MAGIC things = ...
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC Scala
# MAGIC ```
# MAGIC val iot = List(List("Dog","Cat","Monkey"),List("Apple","Orange","Lemon"),List("Car","Bike","Motor"))
# MAGIC 
# MAGIC // Flatten this to a single list of things
# MAGIC 
# MAGIC val things = ...
# MAGIC ```
# MAGIC ----

# COMMAND ----------

animals = ["Dog", "Cat", "Monkey"]

#convert all names to uppercase

all_upper = [x.upper() for x in animals]
print(all_upper)

iot = [["Dog","Cat","Monkey"],["Apple","Orange","Lemon"],["Car","Bike","Motor"]]

#Flatten this to a single list of things
`
things = [val for sublist in iot for val in sublist]
print(things)

# COMMAND ----------

# MAGIC %md # Jupyter intro
# MAGIC 
# MAGIC The Jupyter Notebook is a web application that allows you to create and share documents that contain live code, equations, visualizations and explanatory text
# MAGIC 
# MAGIC The notebook contains cells which can contain code or documentation.
# MAGIC In this course the main used cell types are code and markdown.
# MAGIC 
# MAGIC In this exercise we will play a bit with this concept while also introducing the elements needed to work with Spark inside a notebook

# COMMAND ----------

# MAGIC %md ## Navigation inside the notebook
# MAGIC In the help menu above there is a keyboard shortcuts menu available.
# MAGIC Check this out to help you complete this simple exercise.
# MAGIC 
# MAGIC #### Exercise 1
# MAGIC Create a new cell below this one of type 'markdown' and add some markdown text in it.

# COMMAND ----------

# MAGIC %md #### Exercise 2
# MAGIC Create a new code cell above this cell with the following python code:
# MAGIC 
# MAGIC ```
# MAGIC sum_of_twos = 2 + 2
# MAGIC sum_of_twos
# MAGIC ```

# COMMAND ----------

# MAGIC %md #### Exercise 3
# MAGIC Select the cell created in exercise 2 and execute the code.

# COMMAND ----------

# MAGIC %md #### Exercise 4
# MAGIC Make use of a variable created in a previous cell.
# MAGIC 
# MAGIC Create a new cell below the current one and multiply the value of the variable sum_of_twos by 8.
# MAGIC Make sure to execute this code to see the result.
# MAGIC 
# MAGIC ----

# COMMAND ----------

# MAGIC %md ## Integrate Spark in your notebook
# MAGIC The python package findspark is created just for this purpose. It is already installed in this environment with the command `pip install findspark` and depends on the environmentvariable SPARK_HOME. If this variable is not set it will look in a few default locations to find a Spark installation.
# MAGIC 
# MAGIC #### Exercise 1
# MAGIC Create a new cell below this one and use the following code to initialize the findspark package
# MAGIC ```
# MAGIC import findspark
# MAGIC findspark.init()
# MAGIC ```

# COMMAND ----------

# MAGIC %md #### Exercise 2
# MAGIC In the cell below this execute the following code to verify that pyspark module is indeed made available
# MAGIC 
# MAGIC ```
# MAGIC import pyspark
# MAGIC ```

# COMMAND ----------

1+1

# COMMAND ----------

# MAGIC %md ----