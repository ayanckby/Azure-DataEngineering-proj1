# Databricks notebook source
# MAGIC %md
# MAGIC # Connect Databricks with Datalake
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### link of the website where we can get the details for connecging databricks with datalake using access key--
# MAGIC
# MAGIC [https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage](url)

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.datalakeforproj1.dfs.core.windows.net","uoq7yiIubIm7gb4gut7O6j9zRDdlJeTNoqDv8wXmZY7Nqm3L1HYviejWtpVChLqRbw0UekHAtdcX+AStX/hc0w==")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@datalakeforproj1.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### Read All Data and Store them into DataFrames

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calender

# COMMAND ----------


df_cal = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Calendar/")



# COMMAND ----------

# MAGIC %md
# MAGIC #### Customers

# COMMAND ----------

df_cus = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Customers/")

###AdventureWorks_Product_Categories

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product Category

# COMMAND ----------

df_procat = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Product_Categories/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product Subcategories

# COMMAND ----------

df_subcat = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Product_Subcategories/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Products

# COMMAND ----------

df_pro = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Products/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Returns

# COMMAND ----------

df_ret = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Returns/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales_2015

# COMMAND ----------

df_sales15 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Sales_2015/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales_2016

# COMMAND ----------

df_sales16 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Sales_2016/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales_2017

# COMMAND ----------

df_sales17 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Sales_2017/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Territories

# COMMAND ----------

df_ter = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Territories/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### We are taking all sales data in a single DataFrame (2015,2016,2017) using *
# MAGIC

# COMMAND ----------

df_sales = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Sales*/")

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformation on Data

# COMMAND ----------

# MAGIC %md
# MAGIC Import Required Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Month and Year in Calender DataFrame

# COMMAND ----------

df_cal = df_cal.withColumn("Month",month(col('Date')))\
    .withColumn('Year',year(col('Date')))

# COMMAND ----------

df_cal.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the transformed data into our "silver" container in the data lake

# COMMAND ----------

df_cal.write.format('parquet')\
    .mode('append')\
        .option('path','abfss://silver@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Calendar')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Data Transformation

# COMMAND ----------

df_cus.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Concat all three names(prefix,firstname,lastname) columns into a new column called fullname

# COMMAND ----------

df_cus = df_cus.withColumn('FullName', concat_ws(' ',col('Prefix'),col('FirstName'),col('LastName')))
# concat_ws() is a function in pyspark.sql.functions which takes a list of columns and concatenates them with a separator. In this case, we are concatenating the Prefix, FirstName and LastName columns with space which is the first item of the function.

# COMMAND ----------

# We have another function to concat in pyspark but it is very lengthy and each time we have to use lit() function to add a space.
df_cus.withColumn("name", concat(col('Prefix'), lit(' '), col('FirstName'), lit(' '), col('LastName'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the file in the silver container

# COMMAND ----------

df_cus.write.format('parquet').mode('append').option('Path', 'abfss://silver@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Customers').save()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Products Subcategory Data Transformation

# COMMAND ----------

df_subcat.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nothing to transform here so directly store it into the silver container

# COMMAND ----------

df_subcat.write.format('parquet').mode('append').option('path', 'abfss://silver@datalakeforproj1.dfs.core.windows.net/AdventureWorks_ProductSubcategories').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products Data Transformation

# COMMAND ----------

df_pro.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### we want to transform the ProductSKU column with only those letters which comes before the "-"
# MAGIC
# MAGIC ##### also we want only the first word of ProductName which is letters before the first space

# COMMAND ----------

# In this kind of scenarios we will use split() function to split the sting based on our condition in this case first is "-" and second is " " and then we will use indexing to get the first element of the resulting array

df_pro = df_pro.withColumn('ProductSKU', split(col('ProductSKU'), '-')[0])\
    .withColumn('ProductName', split(col('ProductName'), ' ')[0])

#df_pro.display()

# COMMAND ----------

df_pro.write.format('parquet').mode('append').option('path','abfss://silver@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Products')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Returns Data Transformation

# COMMAND ----------

df_ret.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Nothing to Transform here as well so just save it 

# COMMAND ----------

df_ret.write.format('parquet').mode('append').option('path','abfss://silver@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Returns').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Territories Data Transformation

# COMMAND ----------

df_ter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Again nothing to change here so just Save it

# COMMAND ----------

df_ter.write.format('parquet').mode('append').option('path','abfss://silver@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Territories').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Data Transformation

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Convert Date to Date with timeStamp

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate', to_timestamp(col('StockDate')))

# COMMAND ----------

# MAGIC %md
# MAGIC  In this situation all order numbers are mistakenly placed with s but it should be t so 
# MAGIC ###   2. Change all 'S' in OrderNumber and make them 'T'

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber', regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 3. Multiply OrderLine Item with OrderQuantity in a new column

# COMMAND ----------

df_sales = df_sales.withColumn('Total_in_Line', (col('OrderLineItem') * col('OrderQuantity')))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Analysis
# MAGIC ### Find how many orders we get for each days

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('OrderCount')).orderBy('OrderCount', ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Based on this Result Make a Chart

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('TotalOrders')).orderBy('TotalOrders', ascending = False).display()
# Now click on the Plus button to the right of the table to see the Visualization of the Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lets see for each country How many regions we have .... with Visualization

# COMMAND ----------

df_ter.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Save the data into silver container

# COMMAND ----------

df_sales.write.format('parquet').mode('append').option('path','abfss://silver@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Sales').save()

# COMMAND ----------

# MAGIC %md 
# MAGIC Save the remaining files

# COMMAND ----------

df_pro.write.format('parquet').mode('append').option('path','abfss://silver@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Products').save()

# COMMAND ----------

df_procat.write.format('parquet').mode('append').option('path','abfss://silver@datalakeforproj1.dfs.core.windows.net/AdventureWorks_Product_Categories').save()

# COMMAND ----------

