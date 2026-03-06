# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 04/01/09 11:15:41 Batch  15067_40549 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/28/08 13:39:53 Batch  14638_49197 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 13:00:58 Batch  14605_46863 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/23/07 09:25:41 Batch  14572_33950 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 10/09/07 10:10:10 Batch  14527_36614 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/01/07 10:17:35 Batch  14519_37064 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 10/01/07 09:47:40 Batch  14519_35267 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_2 09/26/07 17:31:58 Batch  14514_63125 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 09/26/07 17:07:37 Batch  14514_61663 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 09/25/07 15:06:34 Batch  14513_54400 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:       NabpIdsProvLocBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                        Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada             05/21/2007          3264                              Originally Programmed                                    devlIDS30       
# MAGIC 
# MAGIC Parikshith Chada               8/17/2007         3264                              Modified the balancing process,                      devlIDS30                     Steph Goddard            9/6/07
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


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""SELECT
PROV_LOC.SRC_SYS_CD_SK,
PROV_LOC.PROV_ID,
PROV_LOC.PROV_ADDR_ID,
PROV_LOC.PROV_ADDR_TYP_CD_SK,
PROV_LOC.PROV_ADDR_EFF_DT_SK,
B_PROV_LOC.SRC_SYS_CD_SK,
B_PROV_LOC.PROV_ID,
B_PROV_LOC.PROV_ADDR_ID,
B_PROV_LOC.PROV_ADDR_TYP_CD_SK,
B_PROV_LOC.PROV_ADDR_EFF_DT_SK
FROM
{IDSOwner}.CD_MPPNG MPPNG,
{IDSOwner}.PROV_LOC PROV_LOC
     FULL OUTER JOIN {IDSOwner}.B_PROV_LOC B_PROV_LOC
              ON PROV_LOC.SRC_SYS_CD_SK = B_PROV_LOC.SRC_SYS_CD_SK
              AND PROV_LOC.PROV_ID = B_PROV_LOC.PROV_ID
              AND PROV_LOC.PROV_ADDR_ID = B_PROV_LOC.PROV_ADDR_ID
              AND PROV_LOC.PROV_ADDR_TYP_CD_SK = B_PROV_LOC.PROV_ADDR_TYP_CD_SK
              AND PROV_LOC.PROV_ADDR_EFF_DT_SK = B_PROV_LOC.PROV_ADDR_EFF_DT_SK
WHERE
PROV_LOC.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND PROV_LOC.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
AND MPPNG.TRGT_CD = 'NABP'
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_SrcTrgtComp = (
    df_SrcTrgtComp
    .withColumnRenamed("SRC_SYS_CD_SK", "SRC_SRC_SYS_CD_SK")
    .withColumnRenamed("PROV_ID", "SRC_PROV_ID")
    .withColumnRenamed("PROV_ADDR_ID", "SRC_PROV_ADDR_ID")
    .withColumnRenamed("PROV_ADDR_TYP_CD_SK", "SRC_PROV_ADDR_TYP_CD_SK")
    .withColumnRenamed("PROV_ADDR_EFF_DT_SK", "SRC_PROV_ADDR_EFF_DT_SK")
    .withColumnRenamed("SRC_SYS_CD_SK_1", "TRGT_SRC_SYS_CD_SK")
    .withColumnRenamed("PROV_ID_1", "TRGT_PROV_ID")
    .withColumnRenamed("PROV_ADDR_ID_1", "TRGT_PROV_ADDR_ID")
    .withColumnRenamed("PROV_ADDR_TYP_CD_SK_1", "TRGT_PROV_ADDR_TYP_CD_SK")
    .withColumnRenamed("PROV_ADDR_EFF_DT_SK_1", "TRGT_PROV_ADDR_EFF_DT_SK")
)

df_Research = df_SrcTrgtComp.filter(
    col("SRC_SRC_SYS_CD_SK").isNull()
    | col("SRC_PROV_ID").isNull()
    | col("SRC_PROV_ADDR_ID").isNull()
    | col("SRC_PROV_ADDR_TYP_CD_SK").isNull()
    | col("SRC_PROV_ADDR_EFF_DT_SK").isNull()
    | col("TRGT_SRC_SYS_CD_SK").isNull()
    | col("TRGT_PROV_ID").isNull()
    | col("TRGT_PROV_ADDR_ID").isNull()
    | col("TRGT_PROV_ADDR_TYP_CD_SK").isNull()
    | col("TRGT_PROV_ADDR_EFF_DT_SK").isNull()
)

df_Research_final = df_Research.select(
    col("TRGT_SRC_SYS_CD_SK"),
    col("TRGT_PROV_ID"),
    col("TRGT_PROV_ADDR_ID"),
    col("TRGT_PROV_ADDR_TYP_CD_SK"),
    rpad(col("TRGT_PROV_ADDR_EFF_DT_SK"), 10, " ").alias("TRGT_PROV_ADDR_EFF_DT_SK"),
    col("SRC_SRC_SYS_CD_SK"),
    col("SRC_PROV_ID"),
    col("SRC_PROV_ADDR_ID"),
    col("SRC_PROV_ADDR_TYP_CD_SK"),
    rpad(col("SRC_PROV_ADDR_EFF_DT_SK"), 10, " ").alias("SRC_PROV_ADDR_EFF_DT_SK")
)

write_files(
    df_Research_final,
    f"{adls_path}/balancing/research/NabpIdsProvLocResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

if ToleranceCd == "OUT":
    df_Notify_pre = df_SrcTrgtComp.limit(1)
else:
    df_Notify_pre = spark.createDataFrame([], df_SrcTrgtComp.schema)

df_Notify = df_Notify_pre.select(
    rpad(lit("ROW COUNT BALANCING NABP - IDS PROV LOC OUT OF TOLERANCE"), 70, " ").alias("NOTIFICATION")
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/ProvidersBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)