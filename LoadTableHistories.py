# Databricks notebook source
def get_earliest_history_version(schema, table):
  version = spark.sql(f"""
  SELECT min(version) as vnum
    FROM (
      DESCRIBE HISTORY {schema}.{table}
    )
  """).collect()
  return version[0]['vnum']

# COMMAND ----------

def record_first_history(history_schema, target_schema, table):
  """Helper function that records the first version history of a Delta table"""
  history_table_name = f"{table}_history"
  print(f"Creating table: '{history_schema}.{history_table_name}'")
  
  # Fetch the snapshot timestamp
  earliest_version = get_earliest_history_version(target_schema, table)
  snapshot_date = spark.sql(f"""
  SELECT CAST(date(timestamp) AS STRING) as snapshot_date
    FROM (DESCRIBE HISTORY {target_schema}.{table})
  WHERE version={earliest_version}
  """).collect()[0]['snapshot_date']
    
  # Create the table
  spark.sql(f"""
  CREATE TABLE {history_schema}.{history_table_name}
  USING DELTA AS
  SELECT {earliest_version} AS vnum,
         CAST("{snapshot_date}" AS date) AS snapshot_date,
         *
    FROM {target_schema}.{table}
  VERSION AS OF {earliest_version}
  """)

# COMMAND ----------

record_first_history("delta_metrics", "delta_metrics", "delta_sharing_complete_issues")

# COMMAND ----------

def record_table_history(history_schema, target_schema, table):
  """Helper function that appends Delta table version histories to a history table.
  Appends Delta version histories to a target table of the format `<schema>.<table>_history`
  @param history_schema: the schema for the history Delta table
  @param schema: the schema for the targert Delta table
  @param table: the name of the target Delta table
  """
  history_table_name = f"{table}_history"

  # First, find all missing versions _not_ in the history table
  missing_versions = spark.sql(f"""
  SELECT version
    FROM (DESCRIBE HISTORY {target_schema}.{table})
   WHERE version NOT IN (
     SELECT vnum
       FROM {history_schema}.{history_table_name}
  )""")
  
  # Collect all missing table versions as a list
  missing_table_version_list = [int(row['version']) for row in missing_versions.collect()]
  missing_table_version_list.sort()

  # For each missing version of the table, use Delta time travel to append rows to history table
  if len(missing_table_version_list) > 0:
    for missing_version in missing_table_version_list:

      print("Adding missing version %s to history table..." % missing_version)

      # Fetch the snapshot timestamp
      snapshot_date = spark.sql(f"""
      SELECT CAST(date(timestamp) AS STRING) as snapshot_date
        FROM (DESCRIBE HISTORY {target_schema}.{table})
      WHERE version={missing_version}
      """).collect()[0]['snapshot_date']
      

      # Now insert full version history into history table
      spark.sql(f"""
      INSERT INTO {history_schema}.{history_table_name}
      SELECT {missing_version} AS vnum,
             CAST("{snapshot_date}" AS date) AS snapshot_date,
             *
       FROM {target_schema}.{table}
      VERSION AS OF {missing_version}
      """)

# COMMAND ----------

# DBTITLE 1,Main
# Grab the Delta table histories for all tables
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

metrics_schema = "delta_metrics"
metrics_tables = ["delta_complete_issues", "connectors_complete_issues", "delta_rs_complete_issues", "delta_sharing_complete_issues"]
for table in metrics_tables:
  print("Getting Delta table history for %s.%s" % (metrics_schema, table))
  record_table_history(metrics_schema, metrics_schema, table)

# COMMAND ----------


