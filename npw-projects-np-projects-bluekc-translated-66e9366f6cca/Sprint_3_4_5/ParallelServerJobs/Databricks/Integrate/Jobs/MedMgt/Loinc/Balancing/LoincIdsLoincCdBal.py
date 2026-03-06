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
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          LoincIdsLoincCdBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               08/10/2007          3264                              Originally Programmed                           devlIDS30                 Steph Goddard            9/14/07

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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# PARAMETERS
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

# DATABASE READ (DB2Connector Stage: SrcTrgtRowComp)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    f"SELECT "
    f"B_LOINC_CD.LOINC_CD as SRC_LOINC_CD, "
    f"LOINC_CD.LOINC_CD as TRGT_LOINC_CD "
    f"FROM {IDSOwner}.LOINC_CD LOINC_CD "
    f"FULL OUTER JOIN {IDSOwner}.B_LOINC_CD B_LOINC_CD "
    f"ON LOINC_CD.LOINC_CD = B_LOINC_CD.LOINC_CD "
    f"WHERE LOINC_CD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"
)
df_SrcTrgtRowComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# TRANSFORM LOGIC (CTransformerStage: TransformLogic)
dfMissing = df_SrcTrgtRowComp

# Output link: Research
dfResearch = dfMissing.filter(
    (F.col("SRC_LOINC_CD").isNull()) | (F.col("TRGT_LOINC_CD").isNull())
).select(
    F.col("SRC_LOINC_CD"),
    F.col("TRGT_LOINC_CD")
)

# Output link: Notify
dfNotify = dfMissing.limit(1).select(
    F.lit("ROW COUNT BALANCING LOINC - IDS LOINC CD OUT OF TOLERANCE").alias("NOTIFICATION")
).withColumn(
    "NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " ")
)

# RESEARCH FILE (CSeqFileStage: ResearchFile)
dfResearchFinal = dfResearch.select("SRC_LOINC_CD","TRGT_LOINC_CD")
research_file_path = f"{adls_path}/balancing/research/LoincIdsLoincCdRowCountResearch.dat.#RunID#"
write_files(
    dfResearchFinal,
    research_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ERROR NOTIFICATION FILE (CSeqFileStage: ErrorNotificationFile)
dfNotifyFinal = dfNotify.select("NOTIFICATION")
notify_file_path = f"{adls_path}/balancing/notify/MedMgtBalancingNotification.dat"
write_files(
    dfNotifyFinal,
    notify_file_path,
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)