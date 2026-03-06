# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/31/07 13:54:42 Batch  14549_50100 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/31/07 13:39:50 Batch  14549_49193 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/30/07 07:53:48 Batch  14548_28437 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/30/07 07:44:34 Batch  14548_27877 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    FctsIdsUmBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada             04/11/2007          3264                              Originally Programmed                                     devlIDS30    
# MAGIC 
# MAGIC Parikshith Chada               8/23/2007         3264                              Modified the balancing process,                       devlIDS30                      Steph Goddard             9/14/07
# MAGIC                                                                                                           by changing snapshot file to snapshot table

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
    UM.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
    UM.UM_REF_ID AS SRC_UM_REF_ID,
    B_UM.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
    B_UM.UM_REF_ID AS TRGT_UM_REF_ID
FROM {IDSOwner}.UM UM
FULL OUTER JOIN {IDSOwner}.B_UM B_UM
    ON UM.SRC_SYS_CD_SK = B_UM.SRC_SYS_CD_SK
    AND UM.UM_REF_ID = B_UM.UM_REF_ID
WHERE UM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_research = df_SrcTrgtComp.filter(
    F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("SRC_UM_REF_ID").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_UM_REF_ID").isNull()
)
df_research = df_research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_UM_REF_ID",
    "SRC_SRC_SYS_CD_SK",
    "SRC_UM_REF_ID"
)

df_SrcTrgtComp_notify = df_SrcTrgtComp.withColumn(
    "row_num", F.row_number().over(Window.orderBy(F.lit(1)))
).filter(
    (F.col("row_num") == 1) & (F.lit(ToleranceCd) == "OUT")
)
df_notify = df_SrcTrgtComp_notify.select(
    F.lit("ROW COUNT BALANCING FACETS - IDS UM OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_notify = df_notify.withColumn("NOTIFICATION", F.rpad("NOTIFICATION", 70, " "))

write_files(
    df_research,
    f"{adls_path}/balancing/research/FacetsIdsUmResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_notify,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.txt",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)