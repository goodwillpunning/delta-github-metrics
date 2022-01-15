# Databricks notebook source
# MAGIC %pip install ghapi

# COMMAND ----------

# MAGIC %run ./Config

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view delta_contributors as
# MAGIC (select distinct user, user_url
# MAGIC   from delta_metrics.kafka_delta_ingest_complete_issues)
# MAGIC union
# MAGIC (select distinct user, user_url
# MAGIC   from delta_metrics.connectors_complete_issues)
# MAGIC union 
# MAGIC (select distinct user, user_url
# MAGIC   from delta_metrics.delta_complete_issues)
# MAGIC union
# MAGIC (select distinct user, user_url
# MAGIC   from delta_metrics.delta_sharing_complete_issues)
# MAGIC union
# MAGIC (select distinct user, user_url
# MAGIC   from delta_metrics.delta_rs_complete_issues)
# MAGIC union
# MAGIC (select distinct user, user_url
# MAGIC   from delta_metrics.kafka_delta_ingest_complete_issues)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct user, user_url
# MAGIC   from delta_contributors

# COMMAND ----------

# MAGIC %py
# MAGIC from ghapi.core import GhApi
# MAGIC 
# MAGIC # Create a GitHub API client
# MAGIC github_pat = dbutils.secrets.get(DATABRICKS_SECRETS_SCOPE, DATABRICKS_SECRETS_NAME)
# MAGIC ghapi = GhApi(token=github_pat)
# MAGIC 
# MAGIC contributors = [r.user for r in spark.sql("select distinct user from delta_contributors").collect()]
# MAGIC parsed_contributors_list = [{
# MAGIC   'username': contributor, 
# MAGIC   'email': ghapi.users.get_by_username(contributor)['email'], 
# MAGIC   'orgs': [org['login'] for org in ghapi.orgs.list_for_user(contributor)]
# MAGIC   } for contributor in contributors]
# MAGIC spark.createDataFrame(parsed_contributors_list, 'username string, email string, orgs array<string>').createOrReplaceTempView("contributor_with_orgs")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 
# MAGIC   a.user, 
# MAGIC   a.user_url,
# MAGIC   b.email,
# MAGIC   b.orgs
# MAGIC from 
# MAGIC   delta_contributors a
# MAGIC inner join contributor_with_orgs b on a.user = b.username

# COMMAND ----------


