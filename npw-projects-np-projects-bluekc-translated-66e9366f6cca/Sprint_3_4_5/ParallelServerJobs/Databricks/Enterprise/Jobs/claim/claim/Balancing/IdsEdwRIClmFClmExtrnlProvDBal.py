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
# MAGIC ^1_1 11/05/07 14:33:17 Batch  14554_52458 PROMOTE bckcetl edw10 dsadm rc for brent
# MAGIC ^1_1 11/05/07 13:30:09 Batch  14554_48642 INIT bckcett testEDW10 dsadm rc for brent
# MAGIC ^1_3 11/02/07 15:11:26 Batch  14551_54690 PROMOTE bckcett testEDW10 u08717 Brent
# MAGIC ^1_3 11/02/07 15:00:18 Batch  14551_54021 INIT bckcett devlEDW10 u08717 Brent
# MAGIC ^1_2 11/02/07 07:43:09 Batch  14551_27794 INIT bckcett devlEDW10 u08717 Brent
# MAGIC ^1_1 11/01/07 11:00:01 Batch  14550_39611 INIT bckcett devlEDW10 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwRIClmFClmExtrnlProvDBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               09/21/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard             10/22/2007

# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Detail files for on-call to research errors
# MAGIC File checked later for rows and email to on-call
# MAGIC Detail files for on-call to research errors
# MAGIC File checked later for rows and email to on-call
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------

# No other imports allowed by the specification

EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query_prmKey = f"""
SELECT
CLM_F.CLM_SK AS CLM_F_CLM_SK,
CLM_EXTRNL_PROV_D.CLM_EXTRNL_PROV_SK AS CLM_EXTRNL_PROV_D_CLM_EXTRNL_PROV_SK
FROM
{EDWOwner}.CLM_F2 CLM_F2 FULL OUTER JOIN {EDWOwner}.CLM_EXTRNL_PROV_D CLM_EXTRNL_PROV_D
ON CLM_F2.CLM_EXTRNL_PROV_SK = CLM_EXTRNL_PROV_D.CLM_EXTRNL_PROV_SK,
{EDWOwner}.CLM_F CLM_F
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_HOST_IN = 'Y'
AND CLM_F.CLM_STTUS_CD IN ('A02','A08','A09')
AND CLM_F.CLM_SK = CLM_F2.CLM_SK
"""

df_SrcTrgtRowComp_prmKey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_prmKey)
    .load()
)

extract_query_Match = f"""
SELECT
CLM_F.SRC_SYS_CD AS CLM_F_SRC_SYS_CD,
CLM_F.CLM_ID AS CLM_F_CLM_ID,
CLM_EXTRNL_PROV_D.SRC_SYS_CD AS CLM_EXTRNL_PROV_D_SRC_SYS_CD,
CLM_EXTRNL_PROV_D.CLM_ID AS CLM_EXTRNL_PROV_D_CLM_ID
FROM
{EDWOwner}.CLM_F CLM_F INNER JOIN {EDWOwner}.CLM_EXTRNL_PROV_D CLM_EXTRNL_PROV_D
ON CLM_F.SRC_SYS_CD = CLM_EXTRNL_PROV_D.SRC_SYS_CD
AND CLM_F.CLM_ID = CLM_EXTRNL_PROV_D.CLM_ID
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_HOST_IN = 'Y'
AND CLM_F.CLM_STTUS_CD IN ('A02','A08','A09')
"""

df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Match)
    .load()
)

extract_query_nKey = f"""
SELECT
CLM_F.SRC_SYS_CD AS CLM_F_SRC_SYS_CD,
CLM_F.CLM_ID AS CLM_F_CLM_ID,
CLM_EXTRNL_PROV_D.SRC_SYS_CD AS CLM_EXTRNL_PROV_D_SRC_SYS_CD,
CLM_EXTRNL_PROV_D.CLM_ID AS CLM_EXTRNL_PROV_D_CLM_ID
FROM
{EDWOwner}.CLM_F CLM_F FULL OUTER JOIN {EDWOwner}.CLM_EXTRNL_PROV_D CLM_EXTRNL_PROV_D
ON CLM_F.SRC_SYS_CD = CLM_EXTRNL_PROV_D.SRC_SYS_CD AND CLM_F.CLM_ID = CLM_EXTRNL_PROV_D.CLM_ID
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_HOST_IN = 'Y'
AND CLM_F.CLM_STTUS_CD IN ('A02','A08','A09')
"""

df_SrcTrgtRowComp_nKey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_nKey)
    .load()
)

df_ParChldMatch = df_SrcTrgtRowComp_Match.select(
    "CLM_F_SRC_SYS_CD",
    "CLM_F_CLM_ID",
    "CLM_EXTRNL_PROV_D_SRC_SYS_CD",
    "CLM_EXTRNL_PROV_D_CLM_ID"
)

write_files(
    df_ParChldMatch,
    f"{adls_path}/balancing/sync/ClmFClmExtrnlProvDBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_Transformer_19_Pkey = df_SrcTrgtRowComp_prmKey.filter(
    F.isnull(F.col("CLM_F_CLM_SK")) | F.isnull(F.col("CLM_EXTRNL_PROV_D_CLM_EXTRNL_PROV_SK"))
).select(
    "CLM_F_CLM_SK",
    "CLM_EXTRNL_PROV_D_CLM_EXTRNL_PROV_SK"
)

df_Transform1_Research1 = df_Transformer_19_Pkey.filter(
    F.isnull(F.col("CLM_F_CLM_SK"))
).select(
    "CLM_F_CLM_SK",
    "CLM_EXTRNL_PROV_D_CLM_EXTRNL_PROV_SK"
)

df_Transform1_Research2 = df_Transformer_19_Pkey.filter(
    F.isnull(F.col("CLM_EXTRNL_PROV_D_CLM_EXTRNL_PROV_SK"))
).select(
    "CLM_F_CLM_SK",
    "CLM_EXTRNL_PROV_D_CLM_EXTRNL_PROV_SK"
)

df_Transform1_Notify = df_Transformer_19_Pkey.limit(1).select(
    F.rpad(
        F.lit("REFERENTIAL INTEGRITY BALANCING PRIMARY KEY IDS - EDW CLM F AND CLM EXTRNL PROV D CHECK FOR OUT OF TOLERANCE"),
        70,
        " "
    ).alias("NOTIFICATION")
)

write_files(
    df_Transform1_Notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

write_files(
    df_Transform1_Research1.select("CLM_F_CLM_SK", "CLM_EXTRNL_PROV_D_CLM_EXTRNL_PROV_SK"),
    f"{adls_path}/balancing/research/IdsEdwPkeyParChldClmFClmExtrnlProvDRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

write_files(
    df_Transform1_Research2.select("CLM_F_CLM_SK", "CLM_EXTRNL_PROV_D_CLM_EXTRNL_PROV_SK"),
    f"{adls_path}/balancing/research/IdsEdwPkeyChldParClmFClmExtrnlProvDRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_Transformer_20_NatKey = df_SrcTrgtRowComp_nKey.filter(
    F.isnull(F.col("CLM_F_CLM_ID")) | F.isnull(F.col("CLM_F_SRC_SYS_CD")) |
    F.isnull(F.col("CLM_EXTRNL_PROV_D_CLM_ID")) | F.isnull(F.col("CLM_EXTRNL_PROV_D_SRC_SYS_CD"))
).select(
    "CLM_F_SRC_SYS_CD",
    "CLM_F_CLM_ID",
    "CLM_EXTRNL_PROV_D_SRC_SYS_CD",
    "CLM_EXTRNL_PROV_D_CLM_ID"
)

df_Transform2_Research3 = df_Transformer_20_NatKey.filter(
    F.isnull(F.col("CLM_F_CLM_ID")) | F.isnull(F.col("CLM_F_SRC_SYS_CD"))
).select(
    "CLM_F_SRC_SYS_CD",
    "CLM_F_CLM_ID",
    "CLM_EXTRNL_PROV_D_SRC_SYS_CD",
    "CLM_EXTRNL_PROV_D_CLM_ID"
)

df_Transform2_Research4 = df_Transformer_20_NatKey.filter(
    F.isnull(F.col("CLM_EXTRNL_PROV_D_CLM_ID")) | F.isnull(F.col("CLM_EXTRNL_PROV_D_SRC_SYS_CD"))
).select(
    "CLM_F_SRC_SYS_CD",
    "CLM_F_CLM_ID",
    "CLM_EXTRNL_PROV_D_SRC_SYS_CD",
    "CLM_EXTRNL_PROV_D_CLM_ID"
)

df_Transform2_Notify = df_Transformer_20_NatKey.limit(1).select(
    F.rpad(
        F.lit("REFERENTIAL INTEGRITY BALANCING NATURAL KEYS IDS - EDW CLM F AND CLM EXTRNL PROV D CHECK FOR OUT OF TOLERANCE"),
        70,
        " "
    ).alias("NOTIFICATION")
)

write_files(
    df_Transform2_Research3.select("CLM_F_SRC_SYS_CD", "CLM_F_CLM_ID", "CLM_EXTRNL_PROV_D_SRC_SYS_CD", "CLM_EXTRNL_PROV_D_CLM_ID"),
    f"{adls_path}/balancing/research/IdsEdwNatkeyParChldClmFClmExtrnlProvDRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

write_files(
    df_Transform2_Research4.select("CLM_F_SRC_SYS_CD", "CLM_F_CLM_ID", "CLM_EXTRNL_PROV_D_SRC_SYS_CD", "CLM_EXTRNL_PROV_D_CLM_ID"),
    f"{adls_path}/balancing/research/IdsEdwNatkeyChldParClmFClmExtrnlProvDRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

write_files(
    df_Transform2_Notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)