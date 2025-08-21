# Databricks notebook source
# MAGIC %sql
# MAGIC -- Schema creation
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze1;
# MAGIC

# COMMAND ----------

# Read Parties CSV
parties_df = spark.read.option("header", True).csv("dbfs:/Volumes/workspace/default/rawdata/Parties.csv")

# Read PartyAddress CSV
party_address_df = spark.read.option("header", True).csv("dbfs:/Volumes/workspace/default/rawdata/PartyAddress.csv")

# Write Parties to Bronze
parties_df.write.format("delta").mode("overwrite").saveAsTable("bronze1.parties_bronze")

# Write PartyAddress to Bronze
party_address_df.write.format("delta").mode("overwrite").saveAsTable("bronze1.partyaddress_bronze")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Bronze tables
# MAGIC SELECT * FROM bronze1.parties_bronze LIMIT 5;
# MAGIC
# MAGIC SELECT * FROM bronze1.partyaddress_bronze LIMIT 5;
