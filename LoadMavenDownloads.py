# Databricks notebook source
# DBTITLE 1,Delta Core
# Load Delta Core Maven downloads
data = [
  ["Mar 2019", 0, 0, 0],
  ["Apr 2019", 525, 525, 0],
  ["May 2019", 2079, 2079, 0],
  ["Jun 2019", 4470, 4470, 0],
  ["Jul 2019", 6321, 6321, 0],
  ["Aug 2019", 9713, 9713, 0],
  ["Sep 2019", 17087, 17087, 0],
  ["Oct 2019", 25157, 25157, 0],
  ["Nov 2019", 25741, 25741, 0],
  ["Dec 2019", 19686, 19686, 0],
  ["Jan 2020", 32065, 32065, 0],
  ["Feb 2020", 74731, 74731, 0],
  ["Mar 2020", 93055, 93055, 0],
  ["Apr 2020", 121325, 121325, 0],
  ["May 2020", 172224, 172224, 0],
  ["Jun 2020", 233826, 233826, 0],
  ["Jul 2020", 251264, 251264, 0],
  ["Aug 2020", 312053, 312053, 0],
  ["Sep 2020", 347568, 347568, 0],
  ["Oct 2020", 351893, 351893, 0],
  ["Nov 2020", 357217, 219888, 137329],
  ["Dec 2020", 373878, 224730, 149148],
  ["Jan 2021", 382301, 238646, 143655],
  ["Feb 2021", 346342, 185791, 160551],
  ["Mar 2021", 415110, 209470, 205640],
  ["Apr 2021", 549168, 209669, 339499],
  ["May 2021", 657218, 222572, 434646],
  ["Jun 2021", 731223, 219103, 512120],
  ["Jul 2021", 785480, 235355, 550125],
  ["Aug 2021", 835287, 261538, 573749],
  ["Sep 2021", 692423, 228494, 463929]
]
core_downloads_df = spark.createDataFrame(data, ["Date_Str", "Total", "Scala_2.11", "Scala_2.12"])

# Overwrite existing table
(core_downloads_df.write
   .format("delta")
   .option("mergeSchema", "true")
   .mode("overwrite")
   .saveAsTable("delta_metrics.delta_core_maven"))

# COMMAND ----------

# DBTITLE 1,Standalone Reader
# Load Delta Standalone Reader Maven downloads
data = [
  ["Dec 2020", 69, 21, 48],
  ["Jan 2021", 124, 31, 93],
  ["Feb 2021", 209, 22, 187],
  ["Mar 2021", 166, 39, 127],
  ["Apr 2021", 231, 33, 198],
  ["May 2021", 557, 22, 535],
  ["Jun 2021", 379, 42, 337],
  ["Jul 2021", 487, 46, 441],
  ["Aug 2021", 621, 76, 545],
  ["Sep 2021", 617, 126, 491]
]
standalone_downloads_df = spark.createDataFrame(data, ["Date_Str", "Total", "Scala_2.11", "Scala_2.12"])

# Overwrite existing table
(standalone_downloads_df.write
   .format("delta")
   .option("mergeSchema", "true")
   .mode("overwrite")
   .saveAsTable("delta_metrics.delta_standalone_maven"))

# COMMAND ----------

# DBTITLE 1,Delta Sharing
# Load Delta Sharing Maven downloads
data = [
  ["May 2021", 26],
  ["Jun 2021", 127],
  ["Jul 2021", 78],
  ["Aug 2021", 108],
  ["Sep 2021", 106]
]
sharing_downloads_df = spark.createDataFrame(data, ["Date_Str", "Total"])

# Overwrite existing table
(sharing_downloads_df.write
   .format("delta")
   .option("mergeSchema", "true")
   .mode("overwrite")
   .saveAsTable("delta_metrics.delta_sharing_maven"))