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
# MAGIC CALLED BY:    NabpIdsProvAddrBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                        Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               05/18/2007          3264                              Originally Programmed                                       devlIDS30            
# MAGIC 
# MAGIC Parikshith Chada               8/16/2007         3264                              Modified the balancing process,                          devlIDS30                 Steph Goddard            9/6/07
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
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Acquire DB Config
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from DB2Connector Stage: "SrcTrgtComp"
extract_query = (
    f"SELECT \n"
    f"PROV_ADDR.SRC_SYS_CD_SK,\n"
    f"PROV_ADDR.PROV_ADDR_ID,\n"
    f"PROV_ADDR.PROV_ADDR_TYP_CD_SK,\n"
    f"PROV_ADDR.PROV_ADDR_EFF_DT_SK,\n"
    f"B_PROV_ADDR.SRC_SYS_CD_SK,\n"
    f"B_PROV_ADDR.PROV_ADDR_ID,\n"
    f"B_PROV_ADDR.PROV_ADDR_TYP_CD_SK,\n"
    f"B_PROV_ADDR.PROV_ADDR_EFF_DT_SK \n"
    f"FROM {IDSOwner}.CD_MPPNG MPPNG,{IDSOwner}.PROV_ADDR PROV_ADDR FULL OUTER JOIN {IDSOwner}.B_PROV_ADDR B_PROV_ADDR \n"
    f"ON PROV_ADDR.SRC_SYS_CD_SK = B_PROV_ADDR.SRC_SYS_CD_SK \n"
    f"AND PROV_ADDR.PROV_ADDR_ID = B_PROV_ADDR.PROV_ADDR_ID \n"
    f"AND PROV_ADDR.PROV_ADDR_TYP_CD_SK = B_PROV_ADDR.PROV_ADDR_TYP_CD_SK \n"
    f"AND PROV_ADDR.PROV_ADDR_EFF_DT_SK = B_PROV_ADDR.PROV_ADDR_EFF_DT_SK \n"
    f"WHERE \n"
    f"PROV_ADDR.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle} \n"
    f"AND PROV_ADDR.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK AND MPPNG.TRGT_CD = 'NABP'"
)
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Rename Columns to match DataStage output link ("Missing")
# The query returns 8 columns in order. Assign them accordingly:
df_SrcTrgtComp = df_SrcTrgtComp.toDF(
    "SRC_SRC_SYS_CD_SK",
    "SRC_PROV_ADDR_ID",
    "SRC_PROV_ADDR_TYP_CD_SK",
    "SRC_PROV_ADDR_EFF_DT_SK",
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_PROV_ADDR_ID",
    "TRGT_PROV_ADDR_TYP_CD_SK",
    "TRGT_PROV_ADDR_EFF_DT_SK"
)

# Transformer Stage: "TransformLogic"

# 1) Output link "Research" with constraint:
#    ISNULL(Missing.SRC_PROV_ADDR_EFF_DT_SK) = @TRUE OR
#    ISNULL(Missing.SRC_PROV_ADDR_ID) = @TRUE OR
#    ISNULL(Missing.SRC_PROV_ADDR_TYP_CD_SK) = @TRUE OR
#    ISNULL(Missing.SRC_SRC_SYS_CD_SK) = @TRUE OR
#    ISNULL(Missing.TRGT_PROV_ADDR_EFF_DT_SK) = @TRUE OR
#    ISNULL(Missing.TRGT_PROV_ADDR_ID) = @TRUE OR
#    ISNULL(Missing.TRGT_PROV_ADDR_TYP_CD_SK) = @TRUE OR
#    ISNULL(Missing.TRGT_SRC_SYS_CD_SK) = @TRUE
df_Research = df_SrcTrgtComp.filter(
    (F.col("SRC_PROV_ADDR_EFF_DT_SK").isNull()) |
    (F.col("SRC_PROV_ADDR_ID").isNull()) |
    (F.col("SRC_PROV_ADDR_TYP_CD_SK").isNull()) |
    (F.col("SRC_SRC_SYS_CD_SK").isNull()) |
    (F.col("TRGT_PROV_ADDR_EFF_DT_SK").isNull()) |
    (F.col("TRGT_PROV_ADDR_ID").isNull()) |
    (F.col("TRGT_PROV_ADDR_TYP_CD_SK").isNull()) |
    (F.col("TRGT_SRC_SYS_CD_SK").isNull())
)

# Select columns in correct order; rpad the char columns
df_Research = df_Research.select(
    F.col("TRGT_SRC_SYS_CD_SK"),
    F.col("TRGT_PROV_ADDR_ID"),
    F.col("TRGT_PROV_ADDR_TYP_CD_SK"),
    F.rpad(F.col("TRGT_PROV_ADDR_EFF_DT_SK"), 10, " ").alias("TRGT_PROV_ADDR_EFF_DT_SK"),
    F.col("SRC_SRC_SYS_CD_SK"),
    F.col("SRC_PROV_ADDR_ID"),
    F.col("SRC_PROV_ADDR_TYP_CD_SK"),
    F.rpad(F.col("SRC_PROV_ADDR_EFF_DT_SK"), 10, " ").alias("SRC_PROV_ADDR_EFF_DT_SK")
)

# 2) Output link "Notify" with constraint:
#    @INROWNUM = 1 And ToleranceCd = 'OUT'
# Produce exactly 1 row if ToleranceCd == 'OUT'; otherwise produce an empty DataFrame
if ToleranceCd == "OUT":
    df_Notify = spark.createDataFrame(
        [("ROW COUNT BALANCING NABP - IDS PROV ADDR OUT OF TOLERANCE",)],
        ["NOTIFICATION"]
    )
else:
    df_Notify = spark.createDataFrame(
        [],
        StructType([StructField("NOTIFICATION", StringType(), True)])
    )

# rpad for the char(70) column
df_Notify = df_Notify.select(F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION"))

# Write Stage: "ResearchFile" -> CSeqFileStage
# Path => "balancing/research/FctsIdsNabpProvAddrResearch.dat.#RunID#"
research_file_path = f"{adls_path}/balancing/research/FctsIdsNabpProvAddrResearch.dat.{RunID}"
write_files(
    df_Research,
    research_file_path,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Write Stage: "ErrorNotificationFile" -> CSeqFileStage
# Path => "balancing/notify/ProvidersBalancingNotification.dat"
notify_file_path = f"{adls_path}/balancing/notify/ProvidersBalancingNotification.dat"
write_files(
    df_Notify,
    notify_file_path,
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)