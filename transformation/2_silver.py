# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## SILVER NOTEBOOK LOOKUP TABLES 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("sourcefolder","netflix_directors")
dbutils.widgets.text("targetfolder","netflix_directors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variables

# COMMAND ----------

var_src_folder = dbutils.widgets.get("sourcefolder")
var_tgt_folder = dbutils.widgets.get("targetfolder")

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", True)\
    .load(f"abfss://bronze@netflixdatalakeochz7.dfs.core.windows.net/{var_src_folder}")

# COMMAND ----------

df.write.format("delta")\
    .mode("append")\
    .option("path", f"abfss://silver@netflixdatalakeochz7.dfs.core.windows.net/{var_tgt_folder}")\
    .save()