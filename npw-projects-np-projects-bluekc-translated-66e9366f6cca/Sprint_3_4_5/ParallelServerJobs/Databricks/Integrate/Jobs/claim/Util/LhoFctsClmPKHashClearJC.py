# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2020 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  FctsClmPrereqSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Clears Facets claim primary key hash files.  This job is run once at the beginning of processing.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Brent Leland\(9)2008-07-29\(9)3567 Primary Key     Original Programming                                               devlIDS
# MAGIC                         
# MAGIC Harikanth Reddy    10/12/2020                                              brought up to standards                                                 IntegrateDev2
# MAGIC Kotha Venkat

# MAGIC Facets Claim - primary key hash file clear
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------
("");