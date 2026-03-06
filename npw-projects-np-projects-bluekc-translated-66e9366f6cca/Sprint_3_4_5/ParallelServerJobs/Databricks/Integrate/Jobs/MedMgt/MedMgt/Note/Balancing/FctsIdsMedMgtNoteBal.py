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
# MAGIC CALLED BY:     FctsIdsMedMgtNoteBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                              Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                         ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada             2007-04-09          3264                              Originally Programmed                                           devlIDS30      
# MAGIC 
# MAGIC Parikshith Chada               8/23/2007         3264                              Modified the balancing process,                            devlIDS30                     Steph Goddard            9/14/07
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
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

# DB Config
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Stage: SrcTrgtComp (DB2Connector)
extract_query = f"""SELECT 
MED_MGT_NOTE.SRC_SYS_CD_SK,
MED_MGT_NOTE.MED_MGT_NOTE_DTM,
MED_MGT_NOTE.MED_MGT_NOTE_INPT_DTM,
B_MED_MGT_NOTE.SRC_SYS_CD_SK,
B_MED_MGT_NOTE.MED_MGT_NOTE_DTM,
B_MED_MGT_NOTE.MED_MGT_NOTE_INPT_DTM

FROM 
{IDSOwner}.MED_MGT_NOTE MED_MGT_NOTE
FULL OUTER JOIN {IDSOwner}.B_MED_MGT_NOTE B_MED_MGT_NOTE
    ON MED_MGT_NOTE.SRC_SYS_CD_SK = B_MED_MGT_NOTE.SRC_SYS_CD_SK
    AND MED_MGT_NOTE.MED_MGT_NOTE_DTM = B_MED_MGT_NOTE.MED_MGT_NOTE_DTM
    AND MED_MGT_NOTE.MED_MGT_NOTE_INPT_DTM = B_MED_MGT_NOTE.MED_MGT_NOTE_INPT_DTM

WHERE 
MED_MGT_NOTE.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Rename columns to match Transformer references
df_SrcTrgtComp = df_SrcTrgtComp.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    F.col("MED_MGT_NOTE_DTM").alias("SRC_MED_MGT_NOTE_DTM"),
    F.col("MED_MGT_NOTE_INPT_DTM").alias("SRC_MED_MGT_NOTE_INPT_DTM"),
    F.col("SRC_SYS_CD_SK_1").alias("TRGT_SRC_SYS_CD_SK"),
    F.col("MED_MGT_NOTE_DTM_1").alias("TRGT_MED_MGT_NOTE_DTM"),
    F.col("MED_MGT_NOTE_INPT_DTM_1").alias("TRGT_MED_MGT_NOTE_INPT_DTM")
)

# Stage: TransformLogic (CTransformerStage)
# Output pin "Research"
df_Research = df_SrcTrgtComp.filter(
    F.col("SRC_MED_MGT_NOTE_DTM").isNull()
    | F.col("SRC_MED_MGT_NOTE_INPT_DTM").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_MED_MGT_NOTE_DTM").isNull()
    | F.col("TRGT_MED_MGT_NOTE_INPT_DTM").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_MED_MGT_NOTE_DTM",
    "TRGT_MED_MGT_NOTE_INPT_DTM",
    "SRC_SRC_SYS_CD_SK",
    "SRC_MED_MGT_NOTE_DTM",
    "SRC_MED_MGT_NOTE_INPT_DTM"
)

# Output pin "Notify"
df_Notify = df_SrcTrgtComp.filter(F.lit(ToleranceCd) == "OUT").limit(1)
df_Notify = df_Notify.select(
    F.rpad(
        F.lit("ROW COUNT BALANCING FACETS - IDS MED MGT NOTE OUT OF TOLERANCE"),
        70,
        " "
    ).alias("NOTIFICATION")
)

# Stage: ResearchFile (CSeqFileStage)
write_files(
    df_Research,
    f"{adls_path}/balancing/research/FacetsIdsMedMgtNoteResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# Stage: ErrorNotificationFile (CSeqFileStage)
write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_parqruet=False,
    header=False,
    quote='"',
    nullValue=None
)