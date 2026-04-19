# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.assignmentadf.dfs.core.windows.net",
  "SP4WI5XoPV0ls2+CKRybhCUyU6xn9LtV5L9wM42VRypj37RcP7Z29KXCtQILMp7s9rsPujYstJ35+ASt8bZj0Q=="
)




# COMMAND ----------

from pyspark.sql.functions import *
import re

def to_snake(name):
    name = name.strip()
    name = name.replace(" ", "_")             
    name = re.sub(r'[^\w]', '_', name)        
    name = re.sub(r'_+', '_', name)            
    return name.lower()

def convert_columns(df):
    return df.toDF(*[to_snake(c) for c in df.columns])

# COMMAND ----------

# DBTITLE 1,CUSTOMER
from pyspark.sql.functions import *

spark.conf.set(
  "fs.azure.account.key.assignmentadf.dfs.core.windows.net",
  "SP4WI5XoPV0ls2+CKRybhCUyU6xn9LtV5L9wM42VRypj37RcP7Z29KXCtQILMp7s9rsPujYstJ35+ASt8bZj0Q=="
)

# READ
df = spark.read.format("csv") \
    .option("header", True) \
    .load("abfss://bronze@assignmentadf.dfs.core.windows.net/sales_view/customer/")

df = convert_columns(df)

df.printSchema()  # check columns

# TRANSFORM
df = df.withColumn("first_name", split(col("name"), " ").getItem(0)) \
       .withColumn("last_name", split(col("name"), " ").getItem(1)) \
       .withColumn("domain", split(split(col("email_id"), "@").getItem(1), "\\.").getItem(0)) \
       .withColumn("date",
    coalesce(
        expr("try_to_date(split(joining_date, ' ')[0], 'dd-MM-yyyy')"),
        expr("try_to_date(split(joining_date, ' ')[0], 'MM-dd-yyyy')")
    )
) \
       .withColumn("time", split(col("joining_date"), " ").getItem(1)) \
       .withColumn("expenditure_status",
           when(expr("try_cast(spent as double)") < 200, "MINIMUM")
           .otherwise("MAXIMUM")
       )

# WRITE
df.write.format("delta") \
.mode("overwrite") \
.save("abfss://silver@assignmentadf.dfs.core.windows.net/sales_view/customer")

# COMMAND ----------

# DBTITLE 1,PRODUCT
df = spark.read.format("csv") \
    .option("header", True) \
    .load("abfss://bronze@assignmentadf.dfs.core.windows.net/sales_view/products/")


df = convert_columns(df)
df.printSchema()  # check columns


df = df.withColumn("sub_category",
    when(col("category_id")==1,"phone")
    .when(col("category_id")==2,"laptop")
    .when(col("category_id")==3,"playstation")
    .otherwise("e-device")
)

# Date formatting
df = df.withColumn("created_at",
    coalesce(
        expr("try_to_date(created_at, 'dd-MM-yyyy')"),
        expr("try_to_date(created_at, 'MM-dd-yyyy')")
    )
) \
.withColumn("updated_at",
    coalesce(
        expr("try_to_date(updated_at, 'dd-MM-yyyy')"),
        expr("try_to_date(updated_at, 'MM-dd-yyyy')")
    )
) \
.withColumn("expiry_date",
    coalesce(
        expr("try_to_date(expiry_date, 'dd-MM-yyyy')"),
        expr("try_to_date(expiry_date, 'MM-dd-yyyy')")
    )
)

df.write.format("delta") \
.mode("overwrite") \
.save("abfss://silver@assignmentadf.dfs.core.windows.net/sales_view/product")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,STORE
df = spark.read.format("csv") \
    .option("header", True) \
    .load("abfss://bronze@assignmentadf.dfs.core.windows.net/sales_view/store/")

df = convert_columns(df)
df.printSchema()  # check columns


df = df.withColumn("store_category",
    split(split(col("email_address"), "@").getItem(1), "\\.").getItem(0)
)

df = df.withColumn("created_at",
    coalesce(
        expr("try_to_date(created_at, 'dd-MM-yyyy')"),
        expr("try_to_date(created_at, 'MM-dd-yyyy')")
    )
).withColumn("updated_at",
    coalesce(
        expr("try_to_date(updated_at, 'dd-MM-yyyy')"),
        expr("try_to_date(updated_at, 'MM-dd-yyyy')")
    )
).withColumn("opening_date",
    coalesce(
        expr("try_to_date(opening_date, 'dd-MM-yyyy')"),
        expr("try_to_date(opening_date, 'MM-dd-yyyy')")
    )
)

df.write.format("delta") \
.mode("overwrite") \
.save("abfss://silver@assignmentadf.dfs.core.windows.net/sales_view/store")

# COMMAND ----------

# DBTITLE 1,SALES
df = spark.read.format("csv") \
    .option("header", True) \
    .load("abfss://bronze@assignmentadf.dfs.core.windows.net/sales_view/sales/")

df = convert_columns(df)
df = df.toDF(*[c.replace(" ", "_") for c in df.columns])

df = df.withColumn("order_date",
    coalesce(
        expr("try_to_date(orderdate, 'dd-MM-yyyy')"),
        expr("try_to_date(orderdate, 'MM-dd-yyyy')")
    )
).withColumn("ship_date",
    coalesce(
        expr("try_to_date(shipdate, 'dd-MM-yyyy')"),
        expr("try_to_date(shipdate, 'MM-dd-yyyy')")
    )
)

df.write.format("delta") \
.mode("overwrite") \
.save("abfss://silver@assignmentadf.dfs.core.windows.net/sales_view/sales")

display(df)

# COMMAND ----------

df.printSchema()