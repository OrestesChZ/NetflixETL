# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## DLT GOLD LAYER

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

looktables_rules = {
    "rule1" : "show_id is NOT NULL"
}

# COMMAND ----------

@dlt.table(
    name = "gold_netflixdirectors"
)

@dlt_expect_all_or_drop(looktables_rules)
def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@netflixdatalakeochz7.dfs.core.windows.net/netflix_directors")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcast"
)

@dlt_expect_all_or_drop(looktables_rules)
def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@netflixdatalakeochz7.dfs.core.windows.net/netflix_cast")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcountries"
)

@dlt_expect_all_or_drop(looktables_rules)
def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@netflixdatalakeochz7.dfs.core.windows.net/netflix_countries")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcategory"
)

@dlt_expect_or_drop("rule1", "show_id is NOT NULL")
def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@netflixdatalakeochz7.dfs.core.windows.net/netflix_category")
    return df

# COMMAND ----------

@dlt.table

def gold_stg_netflixtitles:
    df = spark.readStream.format("delta").load("abfss://silver@netflixdatalakeochz7.dfs.core.windows.net/netflix_titles")
    return df


# COMMAND ----------

@dlt.view

def gold_trns_netflixtitles:
    df = spark.readStream.table("LIVE.gold_stg_netflixtitles")
    df = df.withColumn("newflag", lit("1"))
    return df

# COMMAND ----------

masterdata_rules = {
    "rule1" : "newflag is NOT NULL",
    "rule2" : "show_id is NOT NULL"
}

# COMMAND ----------

@dlt.table

@dlt_expect_all_or_drop(masterdata_rules)
def gold_netflixtitles:
    df = spark.readStream.table("LIVE.gold_trns_netflixtitles")
    return df