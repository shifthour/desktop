# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 10/24/07 09:59:24 Batch  14542_35967 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/24/07 09:54:15 Batch  14542_35658 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_2 10/17/07 13:59:14 Batch  14535_50357 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_2 10/17/07 13:54:06 Batch  14535_50077 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 10/16/07 09:44:13 Batch  14534_35065 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   FctsIdsProvCapBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                     ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              04/20/2007          3264                              Originally Programmed                                      devlIDS30    
# MAGIC 
# MAGIC Parikshith Chada               8/28/2007         3264                              Modified the balancing process,                        devlIDS30                    Steph Goddard             09/19/2007
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

extract_query = f"""
SELECT 
PROV_CAP.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
PROV_CAP.PD_DT_SK AS SRC_PD_DT_SK,
PROV_CAP.CAP_PROV_ID AS SRC_CAP_PROV_ID,
PROV_CAP.PD_PROV_ID AS SRC_PD_PROV_ID,
PROV_CAP.PROV_CAP_PAYMT_LOB_CD_SK AS SRC_PROV_CAP_LOB_CD_SK,
PROV_CAP.PROV_CAP_PAYMT_METH_CD_SK AS SRC_PROV_CAP_PAYMT_METH_CD_SK,
PROV_CAP.PROV_CAP_PAYMT_CAP_TYP_CD_SK AS SRC_PROV_CAP_PAYMT_CAP_TYP_CD_SK,
B_PROV_CAP.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
B_PROV_CAP.PD_DT_SK AS TRGT_PD_DT_SK,
B_PROV_CAP.CAP_PROV_ID AS TRGT_CAP_PROV_ID,
B_PROV_CAP.PD_PROV_ID AS TRGT_PD_PROV_ID,
B_PROV_CAP.PROV_CAP_PAYMT_LOB_CD_SK AS TRGT_PROV_CAP_PAYMT_LOB_CD_SK,
B_PROV_CAP.PROV_CAP_PAYMT_METH_CD_SK AS TRGT_PROV_CAP_PAYMT_METH_CD_SK,
B_PROV_CAP.PROV_CAP_PAYMT_CAP_TYP_CD_SK AS TRGT_PROV_CAP_PAYMT_CAP_TYP_CD_SK
FROM {IDSOwner}.PROV_CAP PROV_CAP
FULL OUTER JOIN {IDSOwner}.B_PROV_CAP B_PROV_CAP
ON PROV_CAP.SRC_SYS_CD_SK = B_PROV_CAP.SRC_SYS_CD_SK
AND PROV_CAP.PD_DT_SK = B_PROV_CAP.PD_DT_SK
AND PROV_CAP.CAP_PROV_ID = B_PROV_CAP.CAP_PROV_ID
AND PROV_CAP.PD_PROV_ID = B_PROV_CAP.PD_PROV_ID
AND PROV_CAP.PROV_CAP_PAYMT_LOB_CD_SK = B_PROV_CAP.PROV_CAP_PAYMT_LOB_CD_SK
AND PROV_CAP.PROV_CAP_PAYMT_METH_CD_SK = B_PROV_CAP.PROV_CAP_PAYMT_METH_CD_SK
AND PROV_CAP.PROV_CAP_PAYMT_CAP_TYP_CD_SK = B_PROV_CAP.PROV_CAP_PAYMT_CAP_TYP_CD_SK
WHERE PROV_CAP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_research = df_SrcTrgtComp.filter(
    F.col("SRC_CAP_PROV_ID").isNull()
    | F.col("SRC_PD_DT_SK").isNull()
    | F.col("SRC_PD_PROV_ID").isNull()
    | F.col("SRC_PROV_CAP_LOB_CD_SK").isNull()
    | F.col("SRC_PROV_CAP_PAYMT_METH_CD_SK").isNull()
    | F.col("SRC_PROV_CAP_PAYMT_CAP_TYP_CD_SK").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_CAP_PROV_ID").isNull()
    | F.col("TRGT_PD_DT_SK").isNull()
    | F.col("TRGT_PD_PROV_ID").isNull()
    | F.col("TRGT_PROV_CAP_PAYMT_LOB_CD_SK").isNull()
    | F.col("TRGT_PROV_CAP_PAYMT_METH_CD_SK").isNull()
    | F.col("TRGT_PROV_CAP_PAYMT_CAP_TYP_CD_SK").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
)

df_research = df_research.withColumn(
    "TRGT_PD_DT_SK",
    F.rpad(F.col("TRGT_PD_DT_SK"), 10, " ")
).withColumn(
    "SRC_PD_DT_SK",
    F.rpad(F.col("SRC_PD_DT_SK"), 10, " ")
)

df_research = df_research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_PD_DT_SK",
    "TRGT_CAP_PROV_ID",
    "TRGT_PD_PROV_ID",
    "TRGT_PROV_CAP_PAYMT_LOB_CD_SK",
    "TRGT_PROV_CAP_PAYMT_METH_CD_SK",
    "TRGT_PROV_CAP_PAYMT_CAP_TYP_CD_SK",
    "SRC_SRC_SYS_CD_SK",
    "SRC_PD_DT_SK",
    "SRC_CAP_PROV_ID",
    "SRC_PD_PROV_ID",
    "SRC_PROV_CAP_LOB_CD_SK",
    "SRC_PROV_CAP_PAYMT_METH_CD_SK",
    "SRC_PROV_CAP_PAYMT_CAP_TYP_CD_SK"
)

write_files(
    df_research,
    f"{adls_path}/balancing/research/FacetsIdsProvCapResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

if ToleranceCd == 'OUT':
    df_notify_temp = df_SrcTrgtComp.limit(1)
else:
    df_notify_temp = spark.createDataFrame([], df_SrcTrgtComp.schema)

df_notify = df_notify_temp.withColumn(
    "NOTIFICATION",
    F.lit("ROW COUNT BALANCING FACETS - IDS PROV CAP OUT OF TOLERANCE")
)

df_notify = df_notify.withColumn(
    "NOTIFICATION",
    F.rpad(F.col("NOTIFICATION"), 70, " ")
).select("NOTIFICATION")

write_files(
    df_notify,
    f"{adls_path}/balancing/notify/CapitationBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)