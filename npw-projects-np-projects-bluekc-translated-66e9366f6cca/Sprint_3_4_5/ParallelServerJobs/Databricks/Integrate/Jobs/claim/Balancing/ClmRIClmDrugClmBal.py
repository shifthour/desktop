# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_2 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/02/07 15:17:19 Batch  14551_55057 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 11/02/07 14:59:36 Batch  14551_53980 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 11/01/07 14:32:31 Batch  14550_52366 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 11/01/07 14:26:29 Batch  14550_51994 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          ClmRIClmDrugClmBalSeq (Multiple Instance)
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                                        Change Description                                               Development Project        Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------                       ---------------------------------------------------------                              ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               10/22/2007          3264                                                    Originally Programmed                                                    devlIDS30                                
# MAGIC 
# MAGIC Manasa Andru                   01/23/2012       TTR- 1260                           Changed the null check conditions in the  transformers              IntegrateCurDevl          SAndrew                  2012-01-30
# MAGIC                                                                                                               in the Notify output links to give notification when needed.

# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Detail files for on-call to research errors
# MAGIC File checked later for rows and email to on-call
# MAGIC Detail files for on-call to research errors
# MAGIC File checked later for rows and email to on-call
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")
RunID = get_widget_value("RunID","")
SrcSysCd = get_widget_value("SrcSysCd","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_pkey = f"""
SELECT
  CLM.CLM_SK AS CLM_CLM_SK,
  DRUG_CLM.CLM_SK AS DRUG_CLM_CLM_SK
FROM {IDSOwner}.CLM CLM
FULL OUTER JOIN {IDSOwner}.DRUG_CLM DRUG_CLM
  ON CLM.CLM_SK = DRUG_CLM.CLM_SK,
  {IDSOwner}.CD_MPPNG MPPNG1,
  {IDSOwner}.CD_MPPNG MPPNG2
WHERE CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
  AND CLM.SRC_SYS_CD_SK = MPPNG1.CD_MPPNG_SK
  AND MPPNG1.TRGT_CD = '{SrcSysCd}'
  AND CLM.CLM_STTUS_CD_SK = MPPNG2.CD_MPPNG_SK
  AND MPPNG2.TRGT_CD IN ('A02','A08','A09')
"""

extract_query_match = f"""
SELECT
  CLM.SRC_SYS_CD_SK AS CLM_SRC_SYS_CD_SK,
  CLM.CLM_ID AS CLM_CLM_ID,
  DRUG_CLM.SRC_SYS_CD_SK AS DRUG_CLM_SRC_SYS_CD_SK,
  DRUG_CLM.CLM_ID AS DRUG_CLM_CLM_ID
FROM {IDSOwner}.CLM CLM
INNER JOIN {IDSOwner}.DRUG_CLM DRUG_CLM
  ON CLM.SRC_SYS_CD_SK = DRUG_CLM.SRC_SYS_CD_SK
  AND CLM.CLM_ID = DRUG_CLM.CLM_ID,
  {IDSOwner}.CD_MPPNG MPPNG1,
  {IDSOwner}.CD_MPPNG MPPNG2
WHERE CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
  AND CLM.SRC_SYS_CD_SK = MPPNG1.CD_MPPNG_SK
  AND MPPNG1.TRGT_CD = '{SrcSysCd}'
  AND CLM.CLM_STTUS_CD_SK = MPPNG2.CD_MPPNG_SK
  AND MPPNG2.TRGT_CD IN ('A02','A08','A09')
"""

extract_query_natkey = f"""
SELECT
  CLM.SRC_SYS_CD_SK AS CLM_SRC_SYS_CD_SK,
  CLM.CLM_ID AS CLM_CLM_ID,
  DRUG_CLM.SRC_SYS_CD_SK AS DRUG_CLM_SRC_SYS_CD_SK,
  DRUG_CLM.CLM_ID AS DRUG_CLM_CLM_ID
FROM {IDSOwner}.CLM CLM
FULL OUTER JOIN {IDSOwner}.DRUG_CLM DRUG_CLM
  ON CLM.SRC_SYS_CD_SK = DRUG_CLM.SRC_SYS_CD_SK
  AND CLM.CLM_ID = DRUG_CLM.CLM_ID,
  {IDSOwner}.CD_MPPNG MPPNG1,
  {IDSOwner}.CD_MPPNG MPPNG2
WHERE CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
  AND CLM.SRC_SYS_CD_SK = MPPNG1.CD_MPPNG_SK
  AND MPPNG1.TRGT_CD = '{SrcSysCd}'
  AND CLM.CLM_STTUS_CD_SK = MPPNG2.CD_MPPNG_SK
  AND MPPNG2.TRGT_CD IN ('A02','A08','A09')
"""

df_SrcTrgtRowComp_Pkey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_pkey)
    .load()
)

df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_match)
    .load()
)

df_SrcTrgtRowComp_NatKey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_natkey)
    .load()
)

df_ParChldMatch = df_SrcTrgtRowComp_Match.select(
    "CLM_SRC_SYS_CD_SK",
    "CLM_CLM_ID",
    "DRUG_CLM_SRC_SYS_CD_SK",
    "DRUG_CLM_CLM_ID"
)

write_files(
    df_ParChldMatch,
    f"{adls_path}/balancing/sync/ClmDrugClmBalancingTotalMatch.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_Transform1_Research1 = df_SrcTrgtRowComp_Pkey.filter(
    col("CLM_CLM_SK").isNull()
).select(
    "CLM_CLM_SK",
    "DRUG_CLM_CLM_SK"
)

df_Transform1_Research2 = df_SrcTrgtRowComp_Pkey.filter(
    col("DRUG_CLM_CLM_SK").isNull()
).select(
    "CLM_CLM_SK",
    "DRUG_CLM_CLM_SK"
)

df_Transform1_Notify_temp = df_SrcTrgtRowComp_Pkey.filter(
    (col("CLM_CLM_SK").isNull()) | (col("DRUG_CLM_CLM_SK").isNull())
)

df_Transform1_Notify_temp_limited = df_Transform1_Notify_temp.limit(1)

df_Transform1_Notify = df_Transform1_Notify_temp_limited.withColumn(
    "NOTIFICATION",
    lit(f"REFERENTIAL INTEGRITY BALANCING PRIMARY KEY {SrcSysCd} - IDS CLM AND DRUG CLM CHECK FOR OUT OF TOLERANCE")
)

df_Transform1_Notify = df_Transform1_Notify.withColumn(
    "NOTIFICATION",
    rpad(col("NOTIFICATION"), 70, " ")
).select("NOTIFICATION")

write_files(
    df_Transform1_Notify,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

write_files(
    df_Transform1_Research1,
    f"{adls_path}/balancing/research/PkeyParChldClmDrugClmRIResearch.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

write_files(
    df_Transform1_Research2,
    f"{adls_path}/balancing/research/PkeyChldParClmDrugClmRIResearch.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_Transform2_Research3 = df_SrcTrgtRowComp_NatKey.filter(
    (col("CLM_CLM_ID").isNull()) | (col("CLM_SRC_SYS_CD_SK").isNull())
).select(
    "CLM_SRC_SYS_CD_SK",
    "CLM_CLM_ID",
    "DRUG_CLM_SRC_SYS_CD_SK",
    "DRUG_CLM_CLM_ID"
)

df_Transform2_Research4 = df_SrcTrgtRowComp_NatKey.filter(
    (col("DRUG_CLM_CLM_ID").isNull()) | (col("DRUG_CLM_SRC_SYS_CD_SK").isNull())
).select(
    "CLM_SRC_SYS_CD_SK",
    "CLM_CLM_ID",
    "DRUG_CLM_SRC_SYS_CD_SK",
    "DRUG_CLM_CLM_ID"
)

df_Transform2_Notify_temp = df_SrcTrgtRowComp_NatKey.filter(
    ((col("CLM_CLM_ID").isNull()) | (col("CLM_SRC_SYS_CD_SK").isNull()))
    | ((col("DRUG_CLM_CLM_ID").isNull()) | (col("DRUG_CLM_SRC_SYS_CD_SK").isNull()))
)

df_Transform2_Notify_temp_limited = df_Transform2_Notify_temp.limit(1)

df_Transform2_Notify = df_Transform2_Notify_temp_limited.withColumn(
    "NOTIFICATION",
    lit(f"REFERENTIAL INTEGRITY BALANCING NATURAL KEYS {SrcSysCd} - IDS CLM AND DRUG CLM CHECK FOR OUT OF TOLERANCE")
)

df_Transform2_Notify = df_Transform2_Notify.withColumn(
    "NOTIFICATION",
    rpad(col("NOTIFICATION"), 70, " ")
).select("NOTIFICATION")

write_files(
    df_Transform2_Research3,
    f"{adls_path}/balancing/research/NatkeyParChldClmDrugClmRIResearch.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

write_files(
    df_Transform2_Research4,
    f"{adls_path}/balancing/research/NatkeyChldParClmDrugClmRIResearch.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

write_files(
    df_Transform2_Notify,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)