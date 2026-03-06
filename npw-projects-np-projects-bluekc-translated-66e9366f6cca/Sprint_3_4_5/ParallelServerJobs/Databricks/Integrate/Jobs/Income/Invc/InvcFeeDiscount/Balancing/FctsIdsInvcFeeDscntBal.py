# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 11/12/07 10:00:11 Batch  14561_36015 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 11/02/07 13:07:35 Batch  14551_47275 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 11/02/07 12:52:45 Batch  14551_46368 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/31/07 13:17:35 Batch  14549_47857 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/31/07 13:09:37 Batch  14549_47387 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     FctsIdsInvcFeeDscntBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/14/2007          3264                              Originally Programmed                                    devlIDS30            
# MAGIC 
# MAGIC Parikshith Chada               8/23/2007         3264                              Modified the balancing process,                        devlIDS30                  Steph Goddard               09/15/2007
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
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT 
INVC_FEE_DSCNT.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
INVC_FEE_DSCNT.BILL_INVC_ID AS SRC_BILL_INVC_ID,
INVC_FEE_DSCNT.FEE_DSCNT_ID AS SRC_FEE_DSCNT_ID,
INVC_FEE_DSCNT.INVC_FEE_DSCNT_SRC_CD_SK AS SRC_INVC_FEE_DSCNT_SRC_CD_SK,
INVC_FEE_DSCNT.INVC_FEE_DSCNT_BILL_DISP_CD_SK AS SRC_INVC_FEE_DSCNT_BILL_DISP_CD_SK,
B_INVC_FEE_DSCNT.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
B_INVC_FEE_DSCNT.BILL_INVC_ID AS TRGT_BILL_INVC_ID,
B_INVC_FEE_DSCNT.FEE_DSCNT_ID AS TRGT_FEE_DSCNT_ID,
B_INVC_FEE_DSCNT.INVC_FEE_DSCNT_SRC_CD_SK AS TRGT_INVC_FEE_DSCNT_SRC_CD_SK,
B_INVC_FEE_DSCNT.INVC_FEE_DSCNT_BILL_DISP_CD_SK AS TRGT_INVC_FEE_DSCNT_BILL_DISP_CD_SK
FROM #$IDSOwner#.INVC_FEE_DSCNT INVC_FEE_DSCNT
FULL OUTER JOIN #$IDSOwner#.B_INVC_FEE_DSCNT B_INVC_FEE_DSCNT
ON INVC_FEE_DSCNT.SRC_SYS_CD_SK = B_INVC_FEE_DSCNT.SRC_SYS_CD_SK
AND INVC_FEE_DSCNT.BILL_INVC_ID = B_INVC_FEE_DSCNT.BILL_INVC_ID
AND INVC_FEE_DSCNT.FEE_DSCNT_ID = B_INVC_FEE_DSCNT.FEE_DSCNT_ID
AND INVC_FEE_DSCNT.INVC_FEE_DSCNT_SRC_CD_SK = B_INVC_FEE_DSCNT.INVC_FEE_DSCNT_SRC_CD_SK
AND INVC_FEE_DSCNT.INVC_FEE_DSCNT_BILL_DISP_CD_SK = B_INVC_FEE_DSCNT.INVC_FEE_DSCNT_BILL_DISP_CD_SK
WHERE INVC_FEE_DSCNT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

research_condition = (
    col("SRC_BILL_INVC_ID").isNull()
    | col("SRC_FEE_DSCNT_ID").isNull()
    | col("SRC_INVC_FEE_DSCNT_BILL_DISP_CD_SK").isNull()
    | col("SRC_INVC_FEE_DSCNT_SRC_CD_SK").isNull()
    | col("SRC_SRC_SYS_CD_SK").isNull()
    | col("TRGT_BILL_INVC_ID").isNull()
    | col("TRGT_FEE_DSCNT_ID").isNull()
    | col("TRGT_INVC_FEE_DSCNT_BILL_DISP_CD_SK").isNull()
    | col("TRGT_INVC_FEE_DSCNT_SRC_CD_SK").isNull()
    | col("TRGT_SRC_SYS_CD_SK").isNull()
)

df_Research = df_SrcTrgtComp.filter(research_condition).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_BILL_INVC_ID",
    "TRGT_FEE_DSCNT_ID",
    "TRGT_INVC_FEE_DSCNT_SRC_CD_SK",
    "TRGT_INVC_FEE_DSCNT_BILL_DISP_CD_SK",
    "SRC_SRC_SYS_CD_SK",
    "SRC_BILL_INVC_ID",
    "SRC_FEE_DSCNT_ID",
    "SRC_INVC_FEE_DSCNT_SRC_CD_SK",
    "SRC_INVC_FEE_DSCNT_BILL_DISP_CD_SK"
)

if ToleranceCd == "OUT":
    w = Window.orderBy(lit(1))
    df_notify_temp = df_SrcTrgtComp.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1)
    df_Notify = df_notify_temp.select(
        rpad(
            lit("ROW COUNT BALANCING FACETS - IDS INVC FEE DSCNT OUT OF TOLERANCE"), 
            70, 
            " "
        ).alias("NOTIFICATION")
    )
else:
    empty_schema = StructType([StructField("NOTIFICATION", StringType(), True)])
    df_Notify = spark.createDataFrame([], empty_schema)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FctsIdsInvcFeeDscntResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/IncomeBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)