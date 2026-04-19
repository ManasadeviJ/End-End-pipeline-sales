# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.assignmentadf.dfs.core.windows.net",
  "SP4WI5XoPV0ls2+CKRybhCUyU6xn9LtV5L9wM42VRypj37RcP7Z29KXCtQILMp7s9rsPujYstJ35+ASt8bZj0Q=="
)




# COMMAND ----------

# DBTITLE 1,JOIN PRODUCT + STORE
from pyspark.sql.functions import col# READ SILVER TABLES

product_df = spark.read.format("delta") \
.load("abfss://silver@assignmentadf.dfs.core.windows.net/sales_view/product")

store_df = spark.read.format("delta") \
.load("abfss://silver@assignmentadf.dfs.core.windows.net/sales_view/store")

# JOIN
product_df = product_df.alias("p")
store_df = store_df.alias("s")

prod_store_df = product_df.join(
    store_df,
    col("p.store_id") == col("s.store_id"),
    "inner"
)

# SELECT REQUIRED COLUMNS
prod_store_df = prod_store_df.select(
    col("p.store_id"),
    col("s.store_name"),
    col("s.location"),
    col("s.manager_name"),
    col("p.product_name"),
    col("p.product_code"),
    col("p.description"),
    col("p.category_id"),
    col("p.price"),
    col("p.stock_quantity"),
    col("p.supplier_id"),
    col("p.created_at").alias("product_created_at"),
    col("p.updated_at").alias("product_updated_at"),
    col("p.image_url"),
    col("p.weight"),
    col("p.expiry_date"),
    col("p.is_active"),
    col("p.tax_rate")
)
prod_store_df.display()

# COMMAND ----------


# READ SALES
sales_df = spark.read.format("delta") \
.load("abfss://silver@assignmentadf.dfs.core.windows.net/sales_view/sales")

# JOIN
sales_df = sales_df.alias("s")
prod_store_df = prod_store_df.alias("ps")

sales_df = sales_df.alias("s")
prod_store_df = prod_store_df.alias("ps")

final_df = sales_df.join(
    prod_store_df,
    col("s.product_id") == col("ps.product_id"),
    "inner"
)

# SELECT FINAL OUTPUT
final_df = final_df.select(
    col("s.order_date").alias("OrderDate"),
    col("s.category").alias("Category"),
    col("s.city").alias("City"),
    col("s.customerid").alias("CustomerID"),
    col("s.orderid").alias("OrderID"),
    col("s.product_id").alias("ProductID"),
    col("s.profit").alias("Profit"),
    col("s.region").alias("Region"),
    col("s.sales").alias("Sales"),
    col("s.segment").alias("Segment"),
    col("s.ship_date").alias("ShipDate"),
    col("s.shipmode").alias("ShipMode"),
    "s.latitude",
    "s.longitude",
    "ps.store_name",
    "ps.location",
    "ps.manager_name",
    "ps.product_name",
    "ps.price",
    "ps.stock_quantity",
    "ps.image_url"
)

final_df.display()

# COMMAND ----------

final_df.write.format("delta") \
.mode("overwrite") \
.save("abfss://gold@assignmentadf.dfs.core.windows.net/sales_view/store_product_sales_analysis")
display("store_product_sales_analysis")