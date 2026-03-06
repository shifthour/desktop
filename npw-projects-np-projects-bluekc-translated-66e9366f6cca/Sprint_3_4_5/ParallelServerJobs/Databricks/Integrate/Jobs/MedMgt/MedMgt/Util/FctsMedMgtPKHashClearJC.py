# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 10/21/08 11:12:18 Batch  14905_40348 PROMOTE bckcetl ids20 dsadm rc for brent 
# MAGIC ^1_2 10/21/08 11:10:57 Batch  14905_40263 INIT bckcett testIDS dsadm rc for brent 
# MAGIC ^1_1 10/20/08 12:51:44 Batch  14904_46312 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_1 10/20/08 10:48:46 Batch  14904_38931 INIT bckcett devlIDS u08717 Brent
# MAGIC ^1_1 07/31/08 07:55:40 Batch  14823_28547 INIT bckcett devlIDS u08717 Brent
# MAGIC 
# MAGIC Â© Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  FctsClmPrereqSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Clears Facets medical management primary key hash files.  This job is run once at the beginning of processing.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Brent Leland\(9)2008-07-29\(9)3567 Primary Key     Original Programming                                               devlIDS                                   Steph Goddard        07/29/2008

# MAGIC Facets Medical Management - primary key hash file clear
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------
