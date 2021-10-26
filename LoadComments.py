# Databricks notebook source
# MAGIC %pip install ghapi

# COMMAND ----------

# MAGIC %run ./Config

# COMMAND ----------

from ghapi.core import GhApi

# COMMAND ----------

# Ensure that the target schema is created
spark.sql("CREATE DATABASE IF NOT EXISTS %s" % TARGET_SCHEMA_NAME)

# COMMAND ----------

# Create a GitHub API client
github_pat = dbutils.secrets.get(DATABRICKS_SECRETS_SCOPE, DATABRICKS_SECRETS_NAME)
ghapi = GhApi(token=github_pat)

# COMMAND ----------

# DBTITLE 1,Load Pull Request Comments 
# Master list of comments which will reduce processing to a single Delta MERGE operation
final_comments_list = []

# Load pull request comments for all repos under the Delta-io org
for REPO in REPOS:
  print(f'Loading pull request comments in repo: {REPO}')
  pull_request_loop_var = True
  pull_request_page = 1
  while pull_request_loop_var:
    # Load all the pull requests for the given repo
    pull_requests = ghapi.pulls.list(GITHUB_ORG_NAME, REPO, state='all', per_page=PULL_REQUESTS_PER_PAGE, page=pull_request_page)
    if len(pull_requests) > 0:
      for pull_request in pull_requests:
        pull_number = pull_request['number']
        pull_request_state = pull_request['state']
        # Grab all the comments for this particular pull request number
        comments_loop_var = True
        comment_page = 1
        while comments_loop_var:
          pull_comments = ghapi.pulls.list_review_comments(GITHUB_ORG_NAME, REPO, pull_number, per_page=COMMENTS_PER_PAGE, page=comment_page)
          if len(pull_comments) > 0:
            # Parse the comment object to fields we are only interested in
            parsed_comments_list = [{
             'repo': REPO,
             'pull_request_number': pull_number,
             'state': pull_request_state,
             'comment_id': comment['id'], 
             'review_id': comment['pull_request_review_id'], 
             'commenter': comment['user']['login'], 
             'commenter_association': comment['author_association'],
             'comment': comment['body'][:1000], 
             'created_at': comment['created_at'], 
             'updated_at': comment['updated_at']
            } for comment in pull_comments]
            # Append the comment to the master list of comments
            final_comments_list += parsed_comments_list
            # Move to the next page of comments
            comment_page += 1
          else:
            comments_loop_var = False
      # Move to the next page of pull requests  
      pull_request_page += 1
    else:
      pull_request_loop_var = False
    
# Create a single DataFrame with all the comments     
pr_comments_df = spark.createDataFrame(final_comments_list, 'repo string, pull_request_number string, state string, comment_id integer, review_id integer, commenter string, commenter_association string, comment string, created_at string, updated_at string')

# COMMAND ----------

display(pr_comments_df)

# COMMAND ----------

pr_comments_df.createOrReplaceTempView("open_pull_request_comments")
spark.sql(f"""
MERGE INTO {TARGET_SCHEMA_NAME}.pull_request_comments as target
USING open_pull_request_comments as source
ON target.repo = source.repo
   AND target.pull_request_number = source.pull_request_number 
   AND target.comment_id = source.comment_id
   AND target.review_id = source.review_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

# COMMAND ----------


