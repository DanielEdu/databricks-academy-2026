# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import sys
import os

print("Python Runtime:", sys.version)
print("Spark Version:", spark.version)
print("Databricks Runtime Version:", os.environ.get("DATABRICKS_RUNTIME_VERSION"))
