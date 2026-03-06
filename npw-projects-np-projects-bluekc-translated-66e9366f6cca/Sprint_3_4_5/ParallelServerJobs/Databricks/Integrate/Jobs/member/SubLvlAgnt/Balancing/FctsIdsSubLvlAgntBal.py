# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 13:39:53 Batch  14638_49197 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/10/08 10:01:22 Batch  14620_36086 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 15:33:58 Batch  14605_56043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/23/07 15:42:37 Batch  14572_56566 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 09/25/07 16:07:19 Batch  14513_58043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 09/20/07 14:41:37 Batch  14508_52916 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 09/20/07 13:58:24 Batch  14508_50316 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 09/19/07 08:11:02 Batch  14507_29467 PROMOTE bckcett testIDS30 u08717 brent
# MAGIC ^1_1 09/19/07 07:44:31 Batch  14507_27876 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     FctsIdsSubLvlAgntBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              04/06/2007          3264                              Originally Programmed                                    devlIDS30                    Brent Leland                 08-30-2007
# MAGIC                                                                                                           Modified the balancing process, 
# MAGIC                                                                                                           by changing snapshot file to snapshot table

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT 
  SUB_LVL_AGNT.SRC_SYS_CD_SK,
  SUB_LVL_AGNT.AGNT_ID,
  SUB_LVL_AGNT.CLS_PLN_ID,
  SUB_LVL_AGNT.SUB_UNIQ_KEY,
  SUB_LVL_AGNT.SUB_LVL_AGNT_ROLE_TYP_CD_SK,
  SUB_LVL_AGNT.EFF_DT_SK,
  SUB_LVL_AGNT.TERM_DT_SK,
  B_SUB_LVL_AGNT.SRC_SYS_CD_SK as SRC_SYS_CD_SK_1,
  B_SUB_LVL_AGNT.AGNT_ID as AGNT_ID_1,
  B_SUB_LVL_AGNT.CLS_PLN_ID as CLS_PLN_ID_1,
  B_SUB_LVL_AGNT.SUB_UNIQ_KEY as SUB_UNIQ_KEY_1,
  B_SUB_LVL_AGNT.SUB_LVL_AGNT_ROLE_TYP_CD_SK as SUB_LVL_AGNT_ROLE_TYP_CD_SK_1,
  B_SUB_LVL_AGNT.EFF_DT_SK as EFF_DT_SK_1,
  B_SUB_LVL_AGNT.TERM_DT_SK as TERM_DT_SK_1
FROM {IDSOwner}.SUB_LVL_AGNT SUB_LVL_AGNT
FULL OUTER JOIN {IDSOwner}.B_SUB_LVL_AGNT B_SUB_LVL_AGNT
  ON SUB_LVL_AGNT.SRC_SYS_CD_SK = B_SUB_LVL_AGNT.SRC_SYS_CD_SK
  AND SUB_LVL_AGNT.AGNT_ID = B_SUB_LVL_AGNT.AGNT_ID
  AND SUB_LVL_AGNT.CLS_PLN_ID = B_SUB_LVL_AGNT.CLS_PLN_ID
  AND SUB_LVL_AGNT.SUB_UNIQ_KEY = B_SUB_LVL_AGNT.SUB_UNIQ_KEY
  AND SUB_LVL_AGNT.SUB_LVL_AGNT_ROLE_TYP_CD_SK = B_SUB_LVL_AGNT.SUB_LVL_AGNT_ROLE_TYP_CD_SK
  AND SUB_LVL_AGNT.EFF_DT_SK = B_SUB_LVL_AGNT.EFF_DT_SK
  AND SUB_LVL_AGNT.TERM_DT_SK = B_SUB_LVL_AGNT.TERM_DT_SK
WHERE SUB_LVL_AGNT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_SrcTrgtComp = df_SrcTrgtComp.select(
    col("SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    col("AGNT_ID").alias("SRC_AGNT_ID"),
    col("CLS_PLN_ID").alias("SRC_CLS_PLN_ID"),
    col("SUB_UNIQ_KEY").alias("SRC_SUB_UNIQ_KEY"),
    col("SUB_LVL_AGNT_ROLE_TYP_CD_SK").alias("SRC_SUB_LVL_AGNT_ROLE_TYP_CD_SK"),
    col("EFF_DT_SK").alias("SRC_EFF_DT_SK"),
    col("TERM_DT_SK").alias("SRC_TERM_DT_SK"),
    col("SRC_SYS_CD_SK_1").alias("TRGT_SRC_SYS_CD_SK"),
    col("AGNT_ID_1").alias("TRGT_AGNT_ID"),
    col("CLS_PLN_ID_1").alias("TRGT_CLS_PLN_ID"),
    col("SUB_UNIQ_KEY_1").alias("TRGT_SUB_UNIQ_KEY"),
    col("SUB_LVL_AGNT_ROLE_TYP_CD_SK_1").alias("TRGT_SUB_LVL_AGNT_ROLE_TYP_CD_SK"),
    col("EFF_DT_SK_1").alias("TRGT_EFF_DT_SK"),
    col("TERM_DT_SK_1").alias("TRGT_TERM_DT_SK")
)

df_Research = df_SrcTrgtComp.filter(
    (col("SRC_SRC_SYS_CD_SK").isNull())
    | (col("SRC_AGNT_ID").isNull())
    | (col("SRC_CLS_PLN_ID").isNull())
    | (col("SRC_SUB_UNIQ_KEY").isNull())
    | (col("SRC_SUB_LVL_AGNT_ROLE_TYP_CD_SK").isNull())
    | (col("SRC_EFF_DT_SK").isNull())
    | (col("SRC_TERM_DT_SK").isNull())
    | (col("TRGT_SRC_SYS_CD_SK").isNull())
    | (col("TRGT_AGNT_ID").isNull())
    | (col("TRGT_CLS_PLN_ID").isNull())
    | (col("TRGT_SUB_UNIQ_KEY").isNull())
    | (col("TRGT_SUB_LVL_AGNT_ROLE_TYP_CD_SK").isNull())
    | (col("TRGT_EFF_DT_SK").isNull())
    | (col("TRGT_TERM_DT_SK").isNull())
)

df_Research = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_AGNT_ID",
    "TRGT_CLS_PLN_ID",
    "TRGT_SUB_UNIQ_KEY",
    "TRGT_SUB_LVL_AGNT_ROLE_TYP_CD_SK",
    "TRGT_EFF_DT_SK",
    "TRGT_TERM_DT_SK",
    "SRC_SRC_SYS_CD_SK",
    "SRC_AGNT_ID",
    "SRC_CLS_PLN_ID",
    "SRC_SUB_UNIQ_KEY",
    "SRC_SUB_LVL_AGNT_ROLE_TYP_CD_SK",
    "SRC_EFF_DT_SK",
    "SRC_TERM_DT_SK"
)

df_Research = df_Research.withColumn("TRGT_EFF_DT_SK", rpad("TRGT_EFF_DT_SK", 10, " ")) \
    .withColumn("TRGT_TERM_DT_SK", rpad("TRGT_TERM_DT_SK", 10, " ")) \
    .withColumn("SRC_EFF_DT_SK", rpad("SRC_EFF_DT_SK", 10, " ")) \
    .withColumn("SRC_TERM_DT_SK", rpad("SRC_TERM_DT_SK", 10, " "))

if ToleranceCd == 'OUT':
    df_Notify = df_SrcTrgtComp.limit(1)
    df_Notify = df_Notify.withColumn("NOTIFICATION", lit("ROW COUNT BALANCING FACETS - IDS SUB LVL AGNT OUT OF TOLERANCE"))
else:
    df_Notify = spark.createDataFrame([], df_SrcTrgtComp.schema)
    df_Notify = df_Notify.withColumn("NOTIFICATION", lit(None).cast("string"))

df_Notify = df_Notify.select("NOTIFICATION")
df_Notify = df_Notify.withColumn("NOTIFICATION", rpad("NOTIFICATION", 70, " "))

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FacetsIdsSubLvlAgntResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MbrshpBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)