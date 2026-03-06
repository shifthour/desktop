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
# MAGIC CALLED BY:    FctsIdsSubGrpLvlAgntBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               04/04/2007         3264                              Originally Programmed                                    devlIDS30                     Brent Leland                08-30-2007
# MAGIC                                                                                                            Modified the balancing process, 
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
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
  SUBGRP_LVL_AGNT.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  SUBGRP_LVL_AGNT.AGNT_ID AS SRC_AGNT_ID,
  SUBGRP_LVL_AGNT.CLS_PLN_ID AS SRC_CLS_PLN_ID,
  SUBGRP_LVL_AGNT.GRP_ID AS SRC_GRP_ID,
  SUBGRP_LVL_AGNT.SUBGRP_ID AS SRC_SUBGRP_ID,
  SUBGRP_LVL_AGNT.SUBGRP_LVL_AGNT_ROLE_TYP_CD_SK AS SRC_SUBGRP_LVL_AGNT_ROLE_TYP_CD_SK,
  SUBGRP_LVL_AGNT.EFF_DT_SK AS SRC_EFF_DT_SK,
  SUBGRP_LVL_AGNT.TERM_DT_SK AS SRC_TERM_DT_SK,
  B_SUBGRP_LVL_AGNT.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
  B_SUBGRP_LVL_AGNT.AGNT_ID AS TRGT_AGNT_ID,
  B_SUBGRP_LVL_AGNT.CLS_PLN_ID AS TRGT_CLS_PLN_ID,
  B_SUBGRP_LVL_AGNT.GRP_ID AS TRGT_GRP_ID,
  B_SUBGRP_LVL_AGNT.SUBGRP_ID AS TRGT_SUBGRP_ID,
  B_SUBGRP_LVL_AGNT.SUBGRP_LVL_AGNT_ROLE_TYP_CD_SK AS TRGT_SUBGRP_LVL_AGNT_ROLE_TYP_CD_SK,
  B_SUBGRP_LVL_AGNT.EFF_DT_SK AS TRGT_EFF_DT_SK,
  B_SUBGRP_LVL_AGNT.TERM_DT_SK AS TRGT_TERM_DT_SK
FROM {IDSOwner}.SUBGRP_LVL_AGNT SUBGRP_LVL_AGNT
FULL OUTER JOIN {IDSOwner}.B_SUBGRP_LVL_AGNT B_SUBGRP_LVL_AGNT
  ON SUBGRP_LVL_AGNT.SRC_SYS_CD_SK = B_SUBGRP_LVL_AGNT.SRC_SYS_CD_SK
  AND SUBGRP_LVL_AGNT.AGNT_ID = B_SUBGRP_LVL_AGNT.AGNT_ID
  AND SUBGRP_LVL_AGNT.CLS_PLN_ID = B_SUBGRP_LVL_AGNT.CLS_PLN_ID
  AND SUBGRP_LVL_AGNT.GRP_ID = B_SUBGRP_LVL_AGNT.GRP_ID
  AND SUBGRP_LVL_AGNT.SUBGRP_ID = B_SUBGRP_LVL_AGNT.SUBGRP_ID
  AND SUBGRP_LVL_AGNT.SUBGRP_LVL_AGNT_ROLE_TYP_CD_SK = B_SUBGRP_LVL_AGNT.SUBGRP_LVL_AGNT_ROLE_TYP_CD_SK
  AND SUBGRP_LVL_AGNT.EFF_DT_SK = B_SUBGRP_LVL_AGNT.EFF_DT_SK
  AND SUBGRP_LVL_AGNT.TERM_DT_SK = B_SUBGRP_LVL_AGNT.TERM_DT_SK
WHERE SUBGRP_LVL_AGNT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_ResearchFilter = df_SrcTrgtComp.filter(
    F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("SRC_AGNT_ID").isNull()
    | F.col("SRC_CLS_PLN_ID").isNull()
    | F.col("SRC_GRP_ID").isNull()
    | F.col("SRC_SUBGRP_ID").isNull()
    | F.col("SRC_SUBGRP_LVL_AGNT_ROLE_TYP_CD_SK").isNull()
    | F.col("SRC_EFF_DT_SK").isNull()
    | F.col("SRC_TERM_DT_SK").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_AGNT_ID").isNull()
    | F.col("TRGT_CLS_PLN_ID").isNull()
    | F.col("TRGT_GRP_ID").isNull()
    | F.col("TRGT_SUBGRP_ID").isNull()
    | F.col("TRGT_SUBGRP_LVL_AGNT_ROLE_TYP_CD_SK").isNull()
    | F.col("TRGT_EFF_DT_SK").isNull()
    | F.col("TRGT_TERM_DT_SK").isNull()
)

df_Research = df_ResearchFilter.select(
    F.col("TRGT_SRC_SYS_CD_SK"),
    F.col("TRGT_AGNT_ID"),
    F.col("TRGT_CLS_PLN_ID"),
    F.col("TRGT_GRP_ID"),
    F.col("TRGT_SUBGRP_ID"),
    F.col("TRGT_SUBGRP_LVL_AGNT_ROLE_TYP_CD_SK"),
    F.rpad(F.col("TRGT_EFF_DT_SK"), 10, " ").alias("TRGT_EFF_DT_SK"),
    F.rpad(F.col("TRGT_TERM_DT_SK"), 10, " ").alias("TRGT_TERM_DT_SK"),
    F.col("SRC_SRC_SYS_CD_SK"),
    F.col("SRC_AGNT_ID"),
    F.col("SRC_CLS_PLN_ID"),
    F.col("SRC_GRP_ID"),
    F.col("SRC_SUBGRP_ID"),
    F.col("SRC_SUBGRP_LVL_AGNT_ROLE_TYP_CD_SK"),
    F.rpad(F.col("SRC_EFF_DT_SK"), 10, " ").alias("SRC_EFF_DT_SK"),
    F.rpad(F.col("SRC_TERM_DT_SK"), 10, " ").alias("SRC_TERM_DT_SK")
)

if ToleranceCd == "OUT":
    w = Window.orderBy(F.lit(1))
    df_t = df_SrcTrgtComp.withColumn("row_num", F.row_number().over(w))
    df_t = df_t.filter(F.col("row_num") == 1)
    df_Notify = df_t.select(
        F.rpad(
            F.lit("ROW COUNT BALANCING FACETS - IDS SUBGRP LVL AGNT OUT OF TOLERANCE"),
            70,
            " "
        ).alias("NOTIFICATION")
    )
else:
    emptySchema = StructType([StructField("NOTIFICATION", StringType(), True)])
    df_Notify = spark.createDataFrame([], emptySchema)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FacetsIdsSubGrpLvlAgntResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MbrshpBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)