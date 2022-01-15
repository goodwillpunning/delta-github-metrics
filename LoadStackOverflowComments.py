# Databricks notebook source
# MAGIC %pip install stackapi

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ./Config

# COMMAND ----------

# DBTITLE 1,Libraries
from stackapi import StackAPI

# COMMAND ----------

# DBTITLE 1,Ensure Target Schema Exists
# Ensure that the target schema is created
spark.sql("CREATE DATABASE IF NOT EXISTS %s" % TARGET_SCHEMA_NAME)

# COMMAND ----------

# DBTITLE 1,Parse StackOverflow Questions
SITE = StackAPI('stackoverflow')
questions = SITE.fetch('questions', tagged=SEARCH_TAGS, sort='votes')

# Parse Stack Overflow questions into a meaningful table structure
parsed_questions_list = [{
  'id': item['question_id'],
  'tags': item['tags'],
  'is_answered': item['is_answered'],
  'view_count': item['view_count'],
  'answer_count': item['answer_count'],
  'score': item['score'],
  'last_activity_at': item['last_activity_date'],
  'created_at': item['creation_date'],
  'link': item['link'],
  'title': item['title']} for item in questions['items']]

questions_df = spark.createDataFrame(parsed_questions_list, 'id long, tags string, is_answered boolean, view_count long, answer_count long, score long, last_activity_at string, created_at string, link string, title string')

# COMMAND ----------

# DBTITLE 1,Save as a Delta Table
# Write Stack Overflow questions to a managed Delta table
questions_df.write.format('delta').mode('overwrite').saveAsTable('%s.%s' % (TARGET_SCHEMA_NAME, STACK_OVERFLOW_TABLE_NAME))
