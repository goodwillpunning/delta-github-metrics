# Databricks notebook source
# MAGIC %sh pip install --upgrade pypistats

# COMMAND ----------

# DBTITLE 1,Helper Function
import pyspark.sql.functions as F
def makeRepoDataFrame(repo):
  df = pypistats.overall(repo, mirrors=True, total=True, format="pandas")
  overall_pdf = df[df["category"] != "Total"].sort_values("date")
  recent_pdf = pypistats.recent(repo, format="pandas")
  overall_df = spark.createDataFrame(overall_pdf.drop("category", axis=1)).withColumn("name", F.lit(repo))
  recent_df = spark.createDataFrame(recent_pdf).withColumn("name", F.lit(repo))
  return overall_df, recent_df

# COMMAND ----------

# DBTITLE 1,Create DataFrame for each repo
import pypistats
overall_deltalake_df, recent_deltalake_df = makeRepoDataFrame("deltalake")
overall_deltaspark_df, recent_deltaspark_df = makeRepoDataFrame("delta-spark")

# COMMAND ----------

# DBTITLE 1,Combine DataFrames
overall_complete_df = overall_deltalake_df.union(overall_deltaspark_df)
recent_complete_df = recent_deltalake_df.union(recent_deltaspark_df)

# COMMAND ----------

# DBTITLE 1,Write out DataFrames to Delta Table
overall_complete_df.write.format("delta").mode('overwrite').saveAsTable("delta_metrics.overall_complete_pypi_stats")
recent_complete_df.write.format("delta").mode('overwrite').saveAsTable("delta_metrics.recent_complete_pypi_stats")
