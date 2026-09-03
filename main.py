# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import os
print("Databricks Runtime:", os.environ.get("DATABRICKS_RUNTIME_VERSION"))