# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 03/23/09 15:20:40 Batch  15058_55245 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 03/12/09 19:44:50 Batch  15047_71107 INIT bckcetl edw10 dcg01 sa - bringing everthing down for test
# MAGIC ^1_2 02/25/09 17:10:06 Batch  15032_61828 INIT bckcetl edw10 dcg01 Bringing production code down to edw_test  claim only
# MAGIC ^1_1 02/01/08 12:08:08 Batch  14642_43698 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 12/27/07 14:18:21 Batch  14606_51505 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 08/31/07 14:39:46 Batch  14488_52789 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_2 04/17/07 08:21:24 Batch  14352_30089 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 04/04/07 08:38:24 Batch  14339_31110 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 12/18/06 12:46:24 Batch  14232_46016 INIT bckcetl edw10 dsadm Backup of Claim For12/18/2006
# MAGIC ^1_1 08/15/06 14:50:29 Batch  14107_53439 PROMOTE bckcetl edw10 dsadm bls
# MAGIC ^1_1 08/15/06 14:45:08 Batch  14107_53117 INIT bckcett devlEDW10 i08185 bls
# MAGIC ^1_2 07/11/06 07:22:48 Batch  14072_26575 INIT bckcett devlEDW10 u08717 Brent
# MAGIC ^1_1 07/07/06 11:49:12 Batch  14068_42557 INIT bckcett devlEDW10 u05779 bj
# MAGIC ^1_1 04/24/06 08:59:47 Batch  13994_32389 INIT bckcett devlEDW10 u03651 steffy
# MAGIC 
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     UwsExpRvnuHierExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from RVNU_EXP_SUB_CAT to a landing file for the IDS working table
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	RVNU_EXP_SUB_CAT
# MAGIC   
# MAGIC 
# MAGIC HASH FILES: 
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                             
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  To create a IDS working table W_RVNU_HIER to use in IdsClmFactExpHash. Need a working table to join to the CLM_LN
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:    Sequential file 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2006-05-01      BJ Luce         Original Programming.

# MAGIC Extract UWS Data
# MAGIC Format  IDS working table to use in IdsClmFactExpHash to join to CLM_LN
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


CurrentDate = get_widget_value('CurrentDate','2005-01-11')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)
extract_query = f"SELECT RVNU_CD, RVNU_HIER_NO, RVNU.EXP_SUB_CAT_CD FROM {UWSOwner}.RVNU_EXP_SUB_CAT RVNU, {UWSOwner}.EXP_SUB_CAT_RVNU_HIER HIER WHERE RVNU.EXP_SUB_CAT_CD = HIER.EXP_SUB_CAT_CD"
df_RVNU_EXP_SUB_CAT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_BusinessRules = df_RVNU_EXP_SUB_CAT.select(
    F.col("RVNU_CD").alias("RVNU_CD"),
    F.col("RVNU_HIER_NO").alias("RVNU_HIER_NO"),
    F.col("EXP_SUB_CAT_CD").alias("EXP_SUB_CAT_CD")
)

df_W_RVNU_HIER = df_BusinessRules.select(
    "RVNU_CD",
    "RVNU_HIER_NO",
    "EXP_SUB_CAT_CD"
)

write_files(
    df_W_RVNU_HIER,
    f"{adls_path}/load/W_RVNU_HIER.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)