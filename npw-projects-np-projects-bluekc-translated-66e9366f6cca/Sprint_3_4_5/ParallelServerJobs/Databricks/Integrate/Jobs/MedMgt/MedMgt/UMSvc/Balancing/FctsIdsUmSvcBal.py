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
# MAGIC CALLED BY:   FctsIdsUmSvcBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                 ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              2007-04-12          3264                              Originally Programmed                                     devlIDS30    
# MAGIC 
# MAGIC Parikshith Chada               8/23/2007         3264                              Modified the balancing process,                      devlIDS30                    Steph Goddard             9/14/07
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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Collecting parameters (including database secret naming rule for "IDSOwner")
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

# Retrieve DB configuration for IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Build the query for stage "SrcTrgtComp"
extract_query = f"""
SELECT
  UM_SVC.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  UM_SVC.UM_REF_ID AS SRC_UM_REF_ID,
  UM_SVC.UM_SVC_SEQ_NO AS SRC_UM_SVC_SEQ_NO,
  B_UM_SVC.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
  B_UM_SVC.UM_REF_ID AS TRGT_UM_REF_ID,
  B_UM_SVC.UM_SVC_SEQ_NO AS TRGT_UM_SVC_SEQ_NO
FROM {IDSOwner}.UM_SVC UM_SVC
FULL OUTER JOIN {IDSOwner}.B_UM_SVC B_UM_SVC
  ON UM_SVC.SRC_SYS_CD_SK = B_UM_SVC.SRC_SYS_CD_SK
  AND UM_SVC.UM_REF_ID = B_UM_SVC.UM_REF_ID
  AND UM_SVC.UM_SVC_SEQ_NO = B_UM_SVC.UM_SVC_SEQ_NO
WHERE UM_SVC.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

# Read the data into df_SrcTrgtComp
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Transformer logic for "Research" link
df_Research = (
    df_SrcTrgtComp.filter(
        (F.col("SRC_SRC_SYS_CD_SK").isNull())
        | (F.col("SRC_UM_REF_ID").isNull())
        | (F.col("SRC_UM_SVC_SEQ_NO").isNull())
        | (F.col("TRGT_SRC_SYS_CD_SK").isNull())
        | (F.col("TRGT_UM_REF_ID").isNull())
        | (F.col("TRGT_UM_SVC_SEQ_NO").isNull())
    )
    .select(
        "TRGT_SRC_SYS_CD_SK",
        "TRGT_UM_REF_ID",
        "TRGT_UM_SVC_SEQ_NO",
        "SRC_SRC_SYS_CD_SK",
        "SRC_UM_REF_ID",
        "SRC_UM_SVC_SEQ_NO"
    )
)

# Transformer logic for "Notify" link
# Constraint: @INROWNUM = 1 And ToleranceCd = 'OUT'
# We produce only if ToleranceCd == 'OUT' and take the first row
notify_schema = StructType([StructField("NOTIFICATION", StringType(), True)])
if ToleranceCd == "OUT":
    df_Notify_temp = df_SrcTrgtComp.limit(1).withColumn(
        "NOTIFICATION",
        F.lit("ROW COUNT BALANCING FACETS - IDS UM SVC OUT OF TOLERANCE")
    )
    df_Notify_temp = df_Notify_temp.select("NOTIFICATION")
    # Column type is char(70), so rpad to length 70
    df_Notify = df_Notify_temp.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))
else:
    # Produce an empty dataframe with the same column for consistency
    df_Notify = spark.createDataFrame([], notify_schema)

# Stage "ResearchFile": Write df_Research to a .dat file
write_files(
    df_Research,
    f"{adls_path}/balancing/research/FacetsIdsUmSvcResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage "ErrorNotificationFile": Write df_Notify to a .txt file
write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)