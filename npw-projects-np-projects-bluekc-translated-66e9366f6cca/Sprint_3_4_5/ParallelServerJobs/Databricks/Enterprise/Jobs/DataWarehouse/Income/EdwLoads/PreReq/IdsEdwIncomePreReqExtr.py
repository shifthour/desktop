# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC                          
# MAGIC PROCESSING:
# MAGIC     Reads the IDS membership data and loads into hash files that will later be used by Income extraction programs.   This is only for effienciety puporse and to avoide each job creating their own version of the same data
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer           Date                Project/Altiris #                 Change Description                                                                                        Develop Project             Code Reviewer         Date Reviewed       
# MAGIC ------------------        --------------------     ------------------------                  -----------------------------------------------------------------------                                              --------------------------             -------------------------------   ----------------------------       
# MAGIC Sandrew             2007-09-20      eproject #5137 Project Release 8.1 
# MAGIC                                                    IAD Quarterly Release       initial creation                                                                                                devlEDW10                   Steph Goddard           09/28/2007
# MAGIC 
# MAGIC Kimberly Doty      2010-09-27      TTR 551                           Modified CLS_PLN_DESC from VarChar 20 to VarChar 70                           EnterpriseNewDevl        Steph Goddard           10/01/2010
# MAGIC                                                                                              to prevent DataStage from possibly truncating data
# MAGIC Aditya Raju         2013-09-14      5114                                   Newly Programmed                                                                                      EnterpriseWrhsDevl       Peter Marshall             12/3/2013

# MAGIC Pull all supplemental IDS data
# MAGIC Data sets later used in jobs
# MAGIC   Discretionary Income Extract, 
# MAGIC   Fee Discount Income Extract, 
# MAGIC  Subscriber Premium Extract. 
# MAGIC 
# MAGIC Cleared out at the end of the job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSIncmBeginCycle = get_widget_value("IDSIncmBeginCycle","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"SELECT PROD_SK,PROD_ID FROM {IDSOwner}.PROD"
df_db2_Incm_Prod_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_INCM_PROD_DS = df_db2_Incm_Prod_Extr.select("PROD_SK","PROD_ID")
df_INCM_PROD_DS = (
    df_INCM_PROD_DS
    .withColumn("PROD_SK", rpad("PROD_SK", <...>, " "))
    .withColumn("PROD_ID", rpad("PROD_ID", <...>, " "))
)
write_files(
    df_INCM_PROD_DS,
    f"{adls_path}/ds/INCM_PROD.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

extract_query = f"""
SELECT 
INVC.INVC_SK,
INVC.SRC_SYS_CD_SK,
INVC.BILL_INVC_ID,
INVC.CRT_RUN_CYC_EXCTN_SK,
INVC.LAST_UPDT_RUN_CYC_EXCTN_SK,
INVC.BILL_ENTY_SK,
INVC.INVC_TYP_CD_SK,
INVC.BILL_DUE_DT_SK,
INVC.BILL_END_DT_SK,
INVC.CRT_DT_SK,
INVC.CUR_RCRD_IN,
MAP.TRGT_CD AS BILL_ENTY_LVL_CD
FROM {IDSOwner}.INVC INVC,
     {IDSOwner}.BILL_ENTY BILL,
     {IDSOwner}.CD_MPPNG MAP
WHERE INVC.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSIncmBeginCycle}
  AND INVC.BILL_ENTY_SK = BILL.BILL_ENTY_SK
  AND BILL_ENTY_LVL_CD_SK = MAP.CD_MPPNG_SK
"""
df_db2_Incm_Invc_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_INCM_INVC_DS = df_db2_Incm_Invc_Extr.select(
    "INVC_SK",
    "SRC_SYS_CD_SK",
    "BILL_INVC_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "BILL_ENTY_SK",
    "INVC_TYP_CD_SK",
    "BILL_DUE_DT_SK",
    "BILL_END_DT_SK",
    "CRT_DT_SK",
    "CUR_RCRD_IN",
    "BILL_ENTY_LVL_CD"
)
df_INCM_INVC_DS = (
    df_INCM_INVC_DS
    .withColumn("INVC_SK", rpad("INVC_SK", <...>, " "))
    .withColumn("SRC_SYS_CD_SK", rpad("SRC_SYS_CD_SK", <...>, " "))
    .withColumn("BILL_INVC_ID", rpad("BILL_INVC_ID", <...>, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", rpad("CRT_RUN_CYC_EXCTN_SK", <...>, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", rpad("LAST_UPDT_RUN_CYC_EXCTN_SK", <...>, " "))
    .withColumn("BILL_ENTY_SK", rpad("BILL_ENTY_SK", <...>, " "))
    .withColumn("INVC_TYP_CD_SK", rpad("INVC_TYP_CD_SK", <...>, " "))
    .withColumn("BILL_DUE_DT_SK", rpad("BILL_DUE_DT_SK", <...>, " "))
    .withColumn("BILL_END_DT_SK", rpad("BILL_END_DT_SK", <...>, " "))
    .withColumn("CRT_DT_SK", rpad("CRT_DT_SK", <...>, " "))
    .withColumn("CUR_RCRD_IN", rpad("CUR_RCRD_IN", <...>, " "))
    .withColumn("BILL_ENTY_LVL_CD", rpad("BILL_ENTY_LVL_CD", <...>, " "))
)
write_files(
    df_INCM_INVC_DS,
    f"{adls_path}/ds/INCM_INVC.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

extract_query = f"SELECT CLS_SK,GRP_ID,CLS_ID FROM {IDSOwner}.CLS"
df_db2_Incm_Cls_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_INCM_CLS_DS = df_db2_Incm_Cls_Extr.select("CLS_SK","GRP_ID","CLS_ID")
df_INCM_CLS_DS = (
    df_INCM_CLS_DS
    .withColumn("CLS_SK", rpad("CLS_SK", <...>, " "))
    .withColumn("GRP_ID", rpad("GRP_ID", <...>, " "))
    .withColumn("CLS_ID", rpad("CLS_ID", <...>, " "))
)
write_files(
    df_INCM_CLS_DS,
    f"{adls_path}/ds/INCM_CLS.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

extract_query = f"SELECT GRP_SK,GRP_ID FROM {IDSOwner}.GRP"
df_db2_Incm_Group_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_INCM_GROUP_DS = df_db2_Incm_Group_Extr.select("GRP_SK","GRP_ID")
df_INCM_GROUP_DS = (
    df_INCM_GROUP_DS
    .withColumn("GRP_SK", rpad("GRP_SK", <...>, " "))
    .withColumn("GRP_ID", rpad("GRP_ID", <...>, " "))
)
write_files(
    df_INCM_GROUP_DS,
    f"{adls_path}/ds/INCM_GROUP.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

extract_query = f"SELECT SUBGRP_SK,GRP_ID,SUBGRP_ID FROM {IDSOwner}.SUBGRP"
df_db2_Incm_Subgrp_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_INCM_SUBGRP_DS = df_db2_Incm_Subgrp_Extr.select("SUBGRP_SK","GRP_ID","SUBGRP_ID")
df_INCM_SUBGRP_DS = (
    df_INCM_SUBGRP_DS
    .withColumn("SUBGRP_SK", rpad("SUBGRP_SK", <...>, " "))
    .withColumn("GRP_ID", rpad("GRP_ID", <...>, " "))
    .withColumn("SUBGRP_ID", rpad("SUBGRP_ID", <...>, " "))
)
write_files(
    df_INCM_SUBGRP_DS,
    f"{adls_path}/ds/INCM_SUBGRP.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

extract_query = f"SELECT CLS_PLN_SK,CLS_PLN_ID,CLS_PLN_DESC FROM {IDSOwner}.CLS_PLN"
df_db2_Incm_ClspLN_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_INCM_CLSP_LN_DS = df_db2_Incm_ClspLN_Extr.select("CLS_PLN_SK","CLS_PLN_ID","CLS_PLN_DESC")
df_INCM_CLSP_LN_DS = (
    df_INCM_CLSP_LN_DS
    .withColumn("CLS_PLN_SK", rpad("CLS_PLN_SK", <...>, " "))
    .withColumn("CLS_PLN_ID", rpad("CLS_PLN_ID", <...>, " "))
    .withColumn("CLS_PLN_DESC", rpad("CLS_PLN_DESC", <...>, " "))
)
write_files(
    df_INCM_CLSP_LN_DS,
    f"{adls_path}/ds/INCM_CLSP_LN.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

extract_query = f"SELECT SUB_SK,SUB_UNIQ_KEY,SUB_ID FROM {IDSOwner}.SUB"
df_db2_Incm_Sub_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_INCM_SUB_DS = df_db2_Incm_Sub_Extr.select("SUB_SK","SUB_UNIQ_KEY","SUB_ID")
df_INCM_SUB_DS = (
    df_INCM_SUB_DS
    .withColumn("SUB_SK", rpad("SUB_SK", <...>, " "))
    .withColumn("SUB_UNIQ_KEY", rpad("SUB_UNIQ_KEY", <...>, " "))
    .withColumn("SUB_ID", rpad("SUB_ID", <...>, " "))
)
write_files(
    df_INCM_SUB_DS,
    f"{adls_path}/ds/INCM_SUB.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)