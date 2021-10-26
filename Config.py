# Databricks notebook source
GITHUB_ORG_NAME = 'delta-io'
GITHUB_REPO_NAME = 'delta'
DATABRICKS_SECRETS_SCOPE = 'delta-metrics'
DATABRICKS_SECRETS_NAME = 'github-pat'
TARGET_SCHEMA_NAME = 'delta_metrics'
PULLS_TABLE_NAME = 'pulls'
REPOS_TABLE_NAME = 'repos'
STACK_OVERFLOW_TABLE_NAME = 'stackoverflow_questions'
SEARCH_TAGS = 'delta lake'
REPOS = ["delta", "connectors", "delta-rs", "delta-sharing", "kafka-delta-ingest"]
COMMENTS_PER_PAGE = 100
PULL_REQUESTS_PER_PAGE = 100
ISSUES_PER_PAGE = 100
