# Databricks notebook source
# MAGIC %pip install ghapi

# COMMAND ----------

# DBTITLE 1,Load Configs
# MAGIC %run ./Config

# COMMAND ----------

# DBTITLE 1,Libraries
from ghapi.core import GhApi

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

# DBTITLE 1,Get All Pull Requests for All Repos
for REPO in REPOS:
  loop_var = True
  final_list = []
  page = 1
  while loop_var:
    pull_requests = ghapi.pulls.list(GITHUB_ORG_NAME, REPO, per_page=100, page=page)
    if len(pull_requests) > 0:
      parsed_prs_list = [{
       'number': pr['number'], 
       'state': pr['state'], 
       'title': pr['title'], 
       'url': pr['url'],
       'author': pr['user']['login'], 
       'merges': pr['auto_merge'],
       'created_at': pr['created_at'],
       'updated_at': pr['updated_at'],
       'merged_at': pr['merged_at'],
       'closed_at': pr['closed_at'],
       'assignee': pr['assignee'],
       'requested_reviewers': pr['requested_reviewers'].items,
      } for pr in pull_requests]
      final_list += parsed_prs_list
      page += 1
    else:
      loop_var = False
  df = spark.createDataFrame(final_list, 'number integer, state string, title string, url string, author string, merges string, created_at string, updated_at string, merged_at string, closed_at string, assignee string, requested_reviewers string')
  df.write.format('delta').mode('overwrite').saveAsTable('%s.%s' % (TARGET_SCHEMA_NAME, REPO.replace("-", "_") + "_complete_pulls"))

# COMMAND ----------

# DBTITLE 1,Get All Issues for all Repos
for REPO in REPOS:
  loop_var = True
  final_issues_list = []
  page = 1
  while loop_var:
    issues = ghapi.issues.list_for_repo(GITHUB_ORG_NAME, REPO, per_page=100, page=page)
    if len(issues) > 0:
      parsed_issues_list = [{
        'number': issue['number'], 
        'state': issue['state'],
        'url': issue['html_url'],
        'id': issue['id'],
        'title': issue['title'],
        #'milestone': issue['milestone'],
        'created_at': issue['created_at'],
        'updated_at': issue['updated_at'],
        'user': issue['user']['login'],
        'user_url': issue['user']['html_url'],
        #'label': issue['label'],
        'total_reactions': issue['reactions']['total_count']
      
      } for issue in issues if not issue.__contains__("pull_request")]
      final_issues_list += parsed_issues_list
      page += 1
    else:
      loop_var = False
  issues_df = spark.createDataFrame(final_issues_list, 'number integer, state string, url string, id string, title string, created_at string, updated_at string, user string, user_url string, total_reactions string')
  issues_df.write.format('delta').mode('overwrite').saveAsTable(f"{TARGET_SCHEMA_NAME}.{REPO.replace('-', '_')}_complete_issues")

# COMMAND ----------

# DBTITLE 1,Visualize Issues for one Repo
display(
  spark.sql(f"""
  select *
    from {TARGET_SCHEMA_NAME}.delta_complete_issues
  """)
)

# COMMAND ----------

# DBTITLE 1,Visualize Pull Requests for one Repo
display(
  spark.sql(f"""
  select *
    from {TARGET_SCHEMA_NAME}.delta_complete_pulls
  """)
)
