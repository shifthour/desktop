# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 07/24/08 10:56:42 Batch  14816_39411 PROMOTE bckcetl ids20 dsadm rc for brent 
# MAGIC ^1_3 07/24/08 10:41:03 Batch  14816_38477 INIT bckcett testIDS dsadm rc fro brent 
# MAGIC ^1_3 07/22/08 08:32:54 Batch  14814_30778 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_3 07/22/08 08:21:54 Batch  14814_30118 INIT bckcett devlIDS u08717 Brent
# MAGIC ^1_2 07/07/08 11:47:11 Batch  14799_42473 INIT bckcett devlIDS u11141 has
# MAGIC ^1_1 06/23/08 16:01:25 Batch  14785_57704 PROMOTE bckcett devlIDS u10913 O. Nielsen move from devlIDScur to devlIDS for B. Leland
# MAGIC ^1_1 06/23/08 15:24:04 Batch  14785_55472 INIT bckcett devlIDScur u10913 O. Nielsen move from devlIDSCUR to devlIDS for B. Leland
# MAGIC ^1_1 01/28/08 10:06:06 Batch  14638_36371 PROMOTE bckcett devlIDScur dsadm dsadm
# MAGIC ^1_1 01/28/08 09:39:22 Batch  14638_34767 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/27/07 09:46:56 Batch  14606_35219 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/10/07 14:54:28 Batch  14528_53717 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 10/10/07 14:22:50 Batch  14528_51775 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 10/08/07 17:12:25 Batch  14526_61950 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 10/08/07 17:06:33 Batch  14526_61597 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   FacetsIdsPaymtSumBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada             05/10/2007           3264                              Originally Programmed                                    devlIDS30         
# MAGIC 
# MAGIC Parikshith Chada               8/22/2007         3264                              Modified the balancing process,                       devlIDS30                   Steph Goddard            09/15/2007
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
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

query_SrcTrgtComp = f"""
SELECT
PAYMT_SUM.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,
PAYMT_SUM.PAYMT_REF_ID as SRC_PAYMT_REF_ID,
PAYMT_SUM.PAYMT_SUM_LOB_CD_SK as SRC_PAYMT_SUM_LOB_CD_SK,
B_PAYMT_SUM.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK,
B_PAYMT_SUM.PAYMT_REF_ID as TRGT_PAYMT_REF_ID,
B_PAYMT_SUM.PAYMT_SUM_LOB_CD_SK as TRGT_PAYMT_SUM_LOB_CD_SK
FROM {IDSOwner}.PAYMT_SUM PAYMT_SUM
FULL OUTER JOIN {IDSOwner}.B_PAYMT_SUM B_PAYMT_SUM
ON PAYMT_SUM.SRC_SYS_CD_SK = B_PAYMT_SUM.SRC_SYS_CD_SK
AND PAYMT_SUM.PAYMT_REF_ID = B_PAYMT_SUM.PAYMT_REF_ID
AND PAYMT_SUM.PAYMT_SUM_LOB_CD_SK = B_PAYMT_SUM.PAYMT_SUM_LOB_CD_SK
WHERE PAYMT_SUM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtComp)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    (df_SrcTrgtComp["SRC_PAYMT_REF_ID"].isNull())
    | (df_SrcTrgtComp["SRC_PAYMT_SUM_LOB_CD_SK"].isNull())
    | (df_SrcTrgtComp["SRC_SRC_SYS_CD_SK"].isNull())
    | (df_SrcTrgtComp["TRGT_PAYMT_REF_ID"].isNull())
    | (df_SrcTrgtComp["TRGT_PAYMT_SUM_LOB_CD_SK"].isNull())
    | (df_SrcTrgtComp["TRGT_SRC_SYS_CD_SK"].isNull())
).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_PAYMT_REF_ID",
    "TRGT_PAYMT_SUM_LOB_CD_SK",
    "SRC_SRC_SYS_CD_SK",
    "SRC_PAYMT_REF_ID",
    "SRC_PAYMT_SUM_LOB_CD_SK"
)

df_firstRow = df_SrcTrgtComp.limit(1).withColumn(
    "NOTIFICATION", F.lit("ROW COUNT BALANCING FACETS - IDS PAYMT SUM OUT OF TOLERANCE")
)
df_Notify = df_firstRow if ToleranceCd == 'OUT' else spark.createDataFrame([], df_firstRow.schema)
df_Notify = df_Notify.select(
    F.rpad("NOTIFICATION", 70, " ").alias("NOTIFICATION")
)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FctsIdsPaymtSumResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    '"',
    None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/FinanceBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    '"',
    None
)