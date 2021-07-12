# Databricks notebook source
# MAGIC %pip install ghapi

# COMMAND ----------

# DBTITLE 1,Libraries
from ghapi.core import GhApi

# COMMAND ----------

# DBTITLE 1,Global Vars
GITHUB_ORG_NAME = 'delta-io'
GITHUB_REPO_NAME = 'delta'
DATABRICKS_SECRETS_SCOPE = 'demo'
DATABRICKS_SECRETS_NAME = 'delta-github-metrics-pat'
TARGET_SCHEMA_NAME = 'delta_github_metrics'
PULLS_TABLE_NAME = 'pulls'

# COMMAND ----------

# DBTITLE 1,Ensure Target Schema Exists
# Ensure that the target schema is created
spark.sql("CREATE DATABASE IF NOT EXISTS %s" % TARGET_SCHEMA_NAME)

# COMMAND ----------

# DBTITLE 1,Create GitHub API Client
# Create a GitHub API client
github_pat = dbutils.secrets.get(DATABRICKS_SECRETS_SCOPE, DATABRICKS_SECRETS_NAME)
ghapi = GhApi(token=github_pat)

# COMMAND ----------

# DBTITLE 1,Parse Pull Request Information
pull_requests = ghapi.pulls.list(GITHUB_ORG_NAME, GITHUB_REPO_NAME)

parsed_prs_list = [{
 'number': pr['number'], 
 'state': pr['state'], 
 'title': pr['title'], 
 'author': pr['user']['login'], 
 'merges': pr['auto_merge'],
 'created_at': pr['created_at'],
 'updated_at': pr['updated_at'],
 'merged_at': pr['merged_at'],
 'closed_at': pr['closed_at'],
 'assignee': pr['assignee'],
 'requested_reviewers': pr['requested_reviewers'].items,
} for pr in pull_requests]

prs_df = spark.createDataFrame(parsed_prs_list, 'number integer, state string, title string, author string, merges string, created_at string, updated_at string, merged_at string, closed_at string, assignee string, requested_reviewers string')

# COMMAND ----------

# DBTITLE 1,Save as Managed Delta Table
# Write delta.io pulls information to a managed Delta table
prs_df.write.format('delta').mode('overwrite').saveAsTable('%s.%s' % (TARGET_SCHEMA_NAME, PULLS_TABLE_NAME))

# COMMAND ----------

# DBTITLE 1,Visualize the Pull Requests
display(
  spark.sql(f"""
  select *
    from {TARGET_SCHEMA_NAME}.{PULLS_TABLE_NAME}
  where state = 'open'
  order by created_at asc, updated_at asc
  """)
)

# COMMAND ----------

display(
  spark.sql(f"""
  select author, count(*) as `num_prs`
    from {TARGET_SCHEMA_NAME}.{PULLS_TABLE_NAME}
  group by author
  """)
)

# COMMAND ----------


