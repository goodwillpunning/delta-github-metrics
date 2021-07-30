# Databricks notebook source
# MAGIC %pip install ghapi

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ./Config

# COMMAND ----------

# DBTITLE 1,Libraries
from ghapi.core import GhApi
import datetime

# COMMAND ----------

# Ensure that the target schema is created
spark.sql("CREATE DATABASE IF NOT EXISTS %s" % TARGET_SCHEMA_NAME)

# COMMAND ----------

# Create a GitHub API client
github_pat = dbutils.secrets.get(DATABRICKS_SECRETS_SCOPE, DATABRICKS_SECRETS_NAME)
ghapi = GhApi(token=github_pat)

# COMMAND ----------

# DBTITLE 1,Pull a list of collaborators for each repo
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import ArrayType, StringType

@udf(ArrayType(StringType()))
def list_collaborators(owner, repo, token):
  """Returns a list of collaborators for a given repo."""
  try:
    ghapi = GhApi(token)
    collaborators = ghapi.repos.list_collaborators(owner, repo)
  except:
    collaborators = []
  return collaborators

# COMMAND ----------

# DBTITLE 1,List Repos
# Grab delta.io repos information from GitHub API
repos = ghapi.repos.list_for_org(GITHUB_ORG_NAME)

# Parse repos information into a meaningful table structure
parsed_repos_list = [{
  'id': repo['id'],
  'owner': repo['owner']['login'],
  'owner_id': repo['owner']['id'],
  'name': repo['name'],
  'full_name': repo['full_name'],
  'description': repo['description'],
  'stargazers_count': repo['stargazers_count'],
  'watchers_count': repo['watchers_count'],
  'open_issues_count': repo['open_issues'],
  'forks_count': repo['forks_count'],
  'size': repo['size'],
  'language': repo['language'],
  'license': repo['license']['name'],
  'updated': repo['updated_at']} for repo in repos]
repos_df = spark.createDataFrame(parsed_repos_list, 'id long, owner string, owner_id long, name string, full_name string, description string, stargazers_count long, watchers_count long, open_issues_count long, forks_count long, size long, language string, license string, updated string')\
  .withColumn('collaborators', list_collaborators(col('owner'), col('name'), lit(github_pat)))

# Write delta.io repos information to a managed Delta table
repos_df.write.format('delta').mode('overwrite').saveAsTable('%s.%s' % (TARGET_SCHEMA_NAME, REPOS_TABLE_NAME))

# COMMAND ----------

display(
  spark.sql(f"""
  select *
    from {TARGET_SCHEMA_NAME}.{REPOS_TABLE_NAME}
  """)
)
