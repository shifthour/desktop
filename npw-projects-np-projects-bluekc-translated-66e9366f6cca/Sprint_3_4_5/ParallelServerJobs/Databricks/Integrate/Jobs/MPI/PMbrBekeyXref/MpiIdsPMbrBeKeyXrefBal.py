# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2012 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:       MPI_CONV_OUTP_RQST_WRHS_000
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	                 Change Description	         Development Project	Code Reviewer	Date Reviewed       
# MAGIC ------------------              --------------------     	------------------------	                 ---------------------------------------------------     --------------------------------	-------------------------------	----------------------------       
# MAGIC Rick Henry              2012-07-11              4426 - ICW MPI                        Written                                             IntegrateNewDevl           Bhoomi Dasari        09/19/2012

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
RunID = get_widget_value('RunID','6666')
ToleranceCd = get_widget_value('ToleranceCd','OUT')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrDate = get_widget_value('CurrDate','2012-07-27')
ExtrRunCycle = get_widget_value('ExtrRunCycle','100')

# Step: SrcTrgtComp (DB2Connector) - Reading from IDS database
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
query_SrcTrgtComp = f"""
SELECT
    TRGT.SRC_SYS_CD,
    TRGT.MBR_UNIQ_KEY,
    TRGT.INDV_BE_KEY,
    TRGT.SUB_INDV_BE_KEY,
    TRGT.SUB_UNIQ_KEY,
    SRC.SRC_SYS_CD,
    SRC.MBR_UNIQ_KEY,
    SRC.INDV_BE_KEY,
    SRC.SUB_INDV_BE_KEY,
    SRC.SUB_UNIQ_KEY
FROM {IDSOwner}.P_MBR_BE_KEY_XREF TRGT
FULL OUTER JOIN {IDSOwner}.B_P_MBR_BE_KEY_XREF SRC
    ON TRGT.SRC_SYS_CD = SRC.SRC_SYS_CD
    AND TRGT.MBR_UNIQ_KEY = SRC.MBR_UNIQ_KEY
    AND TRGT.INDV_BE_KEY = SRC.INDV_BE_KEY
    AND TRGT.SUB_INDV_BE_KEY = SRC.SUB_INDV_BE_KEY
    AND TRGT.SUB_UNIQ_KEY = SRC.SUB_UNIQ_KEY
    AND TRGT.LAST_UPDT_DT_SK >= '{ExtrRunCycle}'
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtComp)
    .load()
)

# Rename columns to match DataStage output links
df_SrcTrgtComp = df_SrcTrgtComp \
    .withColumnRenamed("SRC_SYS_CD", "TRGT_SRC_SYS_CD") \
    .withColumnRenamed("MBR_UNIQ_KEY", "TRGT_MBR_UNIQ_KEY") \
    .withColumnRenamed("INDV_BE_KEY", "TRGT_INDV_BE_KEY") \
    .withColumnRenamed("SUB_INDV_BE_KEY", "TRGT_SUB_INDV_BE_KEY") \
    .withColumnRenamed("SUB_UNIQ_KEY", "TRGT_SUB_UNIQ_KEY") \
    .withColumnRenamed("SRC_SYS_CD_1", "SRC_SRC_SYS_CD") \
    .withColumnRenamed("MBR_UNIQ_KEY_1", "SRC_MBR_UNIQ_KEY") \
    .withColumnRenamed("INDV_BE_KEY_1", "SRC_INDV_BE_KEY") \
    .withColumnRenamed("SUB_INDV_BE_KEY_1", "SRC_SUB_INDV_BE_KEY") \
    .withColumnRenamed("SUB_UNIQ_KEY_1", "SRC_SUB_UNIQ_KEY")

# Ensure columns TRGT_MPI_SUB_ID and SRC_MPI_SUB_ID exist (as they appear in the Transformer stage outputs)
df_Missing = df_SrcTrgtComp \
    .withColumn("TRGT_MPI_SUB_ID", F.lit(None).cast(StringType())) \
    .withColumn("SRC_MPI_SUB_ID", F.lit(None).cast(StringType()))

# Step: TransformLogic - Output link "Research"
df_Research = df_Missing.filter(
    (F.col("SRC_INDV_BE_KEY").isNull()) | (F.col("TRGT_INDV_BE_KEY").isNull())
).select(
    "TRGT_SRC_SYS_CD",
    "TRGT_MBR_UNIQ_KEY",
    "TRGT_INDV_BE_KEY",
    "TRGT_SUB_INDV_BE_KEY",
    "TRGT_MPI_SUB_ID",
    "SRC_SRC_SYS_CD",
    "SRC_MBR_UNIQ_KEY",
    "SRC_INDV_BE_KEY",
    "SRC_SUB_INDV_BE_KEY",
    "SRC_MPI_SUB_ID"
)

# Step: TransformLogic - Output link "Notify"
# Constraint: @INROWNUM = 1 And ToleranceCd = 'OUT'
# Return only first row if ToleranceCd is 'OUT'
if ToleranceCd == 'OUT':
    df_tmp_notify = df_Missing.limit(1)
    df_Notify = df_tmp_notify.select(
        F.rpad(
            F.lit("ROW COUNT BALANCING MPI BCBS EXTENSION - IDS P MBR BE KEY XREF OUT OF TOLERANCE"),
            70,
            " "
        ).alias("NOTIFICATION")
    )
else:
    empty_schema = StructType([StructField("NOTIFICATION", StringType(), True)])
    df_Notify = spark.createDataFrame([], empty_schema)

# Step: ResearchFile (CSeqFileStage)
researchFilePath = f"{adls_path}/balancing/research/MpiBcbsExtIdsPMbrBeKeyXrefResearch.dat.{RunID}"
write_files(
    df_Research,
    researchFilePath,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Step: ErrorNotificationFile (CSeqFileStage)
errorNotificationFilePath = f"{adls_path}/balancing/notify/PMbrBeKeyXrefBalancingNotification.txt"
write_files(
    df_Notify,
    errorNotificationFilePath,
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)