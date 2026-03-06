# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_1 07/29/05 15:21:56 Batch  13725_55324 INIT bckcetl ids20 dsadm Brent
# MAGIC ^1_3 03/30/05 11:26:12 Batch  13604_41180 INIT bckcetl ids20 dsadm Brent
# MAGIC ^1_5 12/28/04 14:00:02 Batch  13512_50409 INIT bckcetl ids20 dsadm Brent
# MAGIC ^1_2 10/19/04 07:18:53 Batch  13442_26349 INIT bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 10/11/04 16:38:56 Batch  13434_59943 PROMOTE bckcetl VERSIONIDS dsadm Gina Parr
# MAGIC ^1_1 10/11/04 16:37:52 Batch  13434_59877 INIT bckccdt testIDS20 dsadm Gina Parr
# MAGIC ^1_1 10/11/04 12:57:35 Batch  13434_46659 PROMOTE bckccdt VERSION u08717 Brent
# MAGIC ^1_1 10/11/04 12:54:07 Batch  13434_46450 INIT bckccdt devIDS20 u08717 Brent
# MAGIC ^1_3 10/06/04 16:28:59 Batch  13429_59343 INIT bckccdt devIDS20 u08717 Brent
# MAGIC ^1_2 09/29/04 09:22:27 Batch  13422_33750 INIT bckccdt devIDS20 u08717 Brent
# MAGIC ^1_1 09/17/04 08:03:28 Batch  13410_29018 INIT bckccdt devIDS20 u10913 O. Nielsen
# MAGIC 
# MAGIC Â© Copyright 2004 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  IdsClmLoadJc
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC 
# MAGIC INPUTS: See "Parameters" tab
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC Hash File Logic Default:
# MAGIC 
# MAGIC IF MOD(iconv(@ID,"MCN")[1,10],':NumFiles:') <= 0 OR MOD(iconv(@ID,"MCN")[1,10],':NumFiles:') > ':NumFiles:' THEN ':NumFiles:' ELSE MOD(iconv(@ID,"MCN")[1,10],':NumFiles:')"'
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC CREATE.FILE
# MAGIC DELETE.FILE
# MAGIC CLEAR.FILE  
# MAGIC  
# MAGIC 
# MAGIC UNIX SCRIPTS USED:  None
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  
# MAGIC               
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2004-09-01     Oliver Nielsen            Original Programming.

# MAGIC Distributed hash file maintenence
# MAGIC 
# MAGIC Parameters:
# MAGIC Action       -  CREATE, DELETE, CLEAR
# MAGIC Name       -  Base name of hash file
# MAGIC Number    - Number of part files to handle
# MAGIC 
# MAGIC Distributed file is created using the base name.  
# MAGIC Part file names are the base name with a 3 digit number suffix (i.e. hf_clm_012)
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


Action = get_widget_value("Action", "CREATE")
HashFileName = get_widget_value("HashFileName", "hf_d_cap")
NumFiles = get_widget_value("NumFiles", "17")