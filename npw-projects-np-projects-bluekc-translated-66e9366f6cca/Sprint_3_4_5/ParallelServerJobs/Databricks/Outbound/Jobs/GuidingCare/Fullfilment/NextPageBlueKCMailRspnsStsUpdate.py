# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2017 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : NextPageBlueKCGdgCareRspnsDsptSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING : Job updates Mail response status in COMM table.
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                               Date                         Project                                   Change Description           \(9)\(9)\(9) Development Project          Code Reviewer                   Date Reviewed       
# MAGIC ------------------------------              ------------------        ----------------------             ---------------------------------------       \(9)\(9)\(9)------------------------------\(9)----------------------------------         -------------------------------        
# MAGIC Bharani Chalamalasetty      04/01/2020             Fullfillment                       Original Programming              \(9)\(9)\(9)OutboundDev3                    Jaideep Mankala              04/27/2020
# MAGIC 
# MAGIC Bharani Chalamalasetty      08/07/2020             262790                 Update the COMM_STTUS_CD in COMM Table               \(9)OutboundDev3                    Jaideep Mankala              08/08/2020
# MAGIC 
# MAGIC Bharani Chalamalasetty      05/18/2021             262790                 Changed job to accept different file and updated dateime       OutboundDev3                    Jaideep Mankala           05/19/2021


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Outbound
# COMMAND ----------


# Retrieve job parameters
NXTPGInboundFile = get_widget_value("NXTPGInboundFile", "")
RunID = get_widget_value("RunID", "")
CommTrnsmsnOwner = get_widget_value("$CommTrnsmsnOwner", "")
commtrnsmsn_secret_name = get_widget_value("commtrnsmsn_secret_name", "")
CurrDateTm = get_widget_value("CurrDateTm", "")

# Read "Seq_MailResponse" (PxSequentialFile)
schema_Seq_MailResponse = StructType([
    StructField("LetterQueueId", StringType(), False),
    StructField("MailStatus", StringType(), False),
    StructField("MailDate", StringType(), True)
])
# Assuming the inbound file is in a "landing" area if not explicitly stated
file_path_Seq_MailResponse = f"{adls_path_raw}/landing/{NXTPGInboundFile}"
df_Seq_MailResponse = (
    spark.read
    .option("header", True)
    .option("delimiter", "|")
    .schema(schema_Seq_MailResponse)
    .csv(file_path_Seq_MailResponse)
    .select("LetterQueueId", "MailStatus", "MailDate")
)

# Read "COMM" (ODBCConnectorPX) using the provided ODBC connection info
jdbc_url_COMM, jdbc_props_COMM = get_db_config(commtrnsmsn_secret_name)
extract_query_COMM = f"""
select COMM_SK, COMM_ID
from
(
  SELECT COMM_SK, COMM_ID,
  rank() over(partition by COMM_ID order by COMM_SK desc) as ranknum
  from {CommTrnsmsnOwner}.[COMM]
  where COMM_TRGT_SYS_MDUL_CD='NXTPG'
) a
where a.ranknum=1
"""
df_COMM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_COMM)
    .options(**jdbc_props_COMM)
    .option("query", extract_query_COMM)
    .load()
)

# "lkp_COMM" (PxLookup) - assume intended join condition based on matching LetterQueueId = COMM_ID
df_lkp_COMM_temp = df_Seq_MailResponse.alias("lnk_MailResponse").join(
    df_COMM.alias("lnk_COMM"),
    on=(F.col("lnk_MailResponse.LetterQueueId") == F.col("lnk_COMM.COMM_ID")),
    how="left"
)

# Rows that matched (COMM_SK not null)
df_lkp_COMM_matched = df_lkp_COMM_temp.filter(F.col("lnk_COMM.COMM_SK").isNotNull())
# Rows that did not match (COMM_SK null) -> reject link
df_lkp_COMM_reject = df_lkp_COMM_temp.filter(F.col("lnk_COMM.COMM_SK").isNull())

df_lkp_COMM = df_lkp_COMM_matched.select(
    F.col("lnk_COMM.COMM_SK").alias("COMM_SK"),
    F.col("lnk_MailResponse.LetterQueueId").alias("LetterQueueID"),
    F.col("lnk_MailResponse.MailStatus").alias("MailStatus"),
    F.col("lnk_MailResponse.MailDate").alias("MailDate")
)

# Write RejectFile (PxSequentialFile)
rejectFilePath = f"{adls_path_publish}/external/Reject_{NXTPGInboundFile}_{RunID}.txt"
df_lkp_COMM_reject_out = df_lkp_COMM_reject.select(
    F.col("lnk_MailResponse.LetterQueueId").alias("LetterQueueId"),
    F.col("lnk_MailResponse.MailStatus").alias("MailStatus"),
    F.col("lnk_MailResponse.MailDate").alias("MailDate")
)
write_files(
    df_lkp_COMM_reject_out.select("LetterQueueId", "MailStatus", "MailDate"),
    rejectFilePath,
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# "xfm_RequiredFields" (CTransformerStage)
df_xfm_RequiredFields = (
    df_lkp_COMM
    .withColumn("COMM_SRC_SYS_CD", F.lit("NXTPG"))
    .withColumn("COMM_TRGT_SYS_CD", F.lit("COMMTRNSMSN"))
    .withColumn(
        "COMM_STTUS_CD",
        F.when(F.trim(F.col("MailStatus")) == F.lit("MAIL"), F.lit("SUCCESS")).otherwise(F.lit("FAILED"))
    )
    .withColumn("COMM_TYP_CD", F.lit("MAILRSPNS"))
    .withColumn(
        "COMM_STTUS_CMNT_TX",
        F.when(F.trim(F.col("MailStatus")) == F.lit("MAIL"),
               F.lit("Mail Response was Successful"))
         .otherwise(F.lit("Mail Response was Unsuccessful"))
    )
    .withColumn("LAST_UPDT_DTM", F.lit(CurrDateTm))
    .withColumn(
        "COMM_SENT_DTM",
        F.when(
            (F.trim(F.col("MailDate")) == F.lit("NULL")) |
            (F.trim(F.col("MailDate")) == F.lit("")) |
            (F.col("MailDate").isNull()),
            F.lit(CurrDateTm)
        ).otherwise(F.to_timestamp(F.col("MailDate"), "yyyyMMddHHmmss"))
    )
)

# "Upd_COMM" (ODBCConnectorPX) - Merge to #$CommTrnsmsnOwner#.comm
# Prepare final DF with correct column order and rpad for varchar/char
df_final_Upd_COMM = df_xfm_RequiredFields.select(
    F.col("COMM_SK"),
    F.rpad(F.col("COMM_SRC_SYS_CD"), <...>, " ").alias("COMM_SRC_SYS_CD"),
    F.rpad(F.col("COMM_TRGT_SYS_CD"), <...>, " ").alias("COMM_TRGT_SYS_CD"),
    F.rpad(F.col("COMM_STTUS_CD"), <...>, " ").alias("COMM_STTUS_CD"),
    F.rpad(F.col("COMM_TYP_CD"), <...>, " ").alias("COMM_TYP_CD"),
    F.rpad(F.col("COMM_STTUS_CMNT_TX"), <...>, " ").alias("COMM_STTUS_CMNT_TX"),
    F.col("LAST_UPDT_DTM"),
    F.col("COMM_SENT_DTM")
)

jdbc_url_updCOMM, jdbc_props_updCOMM = get_db_config(commtrnsmsn_secret_name)
spark.sql(f"DROP TABLE IF EXISTS STAGING.NextPageBlueKCMailRspnsStsUpdate_Upd_COMM_temp")
(
    df_final_Upd_COMM.write
    .format("jdbc")
    .option("url", jdbc_url_updCOMM)
    .options(**jdbc_props_updCOMM)
    .option("dbtable", "STAGING.NextPageBlueKCMailRspnsStsUpdate_Upd_COMM_temp")
    .mode("overwrite")
    .save()
)

merge_sql_Upd_COMM = f"""
MERGE INTO {CommTrnsmsnOwner}.comm AS T
USING STAGING.NextPageBlueKCMailRspnsStsUpdate_Upd_COMM_temp AS S
ON T.COMM_SK = S.COMM_SK
WHEN MATCHED THEN UPDATE SET
  T.COMM_SRC_SYS_CD = S.COMM_SRC_SYS_CD,
  T.COMM_TRGT_SYS_CD = S.COMM_TRGT_SYS_CD,
  T.COMM_STTUS_CD = S.COMM_STTUS_CD,
  T.COMM_TYP_CD = S.COMM_TYP_CD,
  T.COMM_STTUS_CMNT_TX = S.COMM_STTUS_CMNT_TX,
  T.LAST_UPDT_DTM = S.LAST_UPDT_DTM,
  T.COMM_SENT_DTM = S.COMM_SENT_DTM
WHEN NOT MATCHED THEN
  INSERT (COMM_SK, COMM_SRC_SYS_CD, COMM_TRGT_SYS_CD, COMM_STTUS_CD, COMM_TYP_CD, COMM_STTUS_CMNT_TX, LAST_UPDT_DTM, COMM_SENT_DTM)
  VALUES(S.COMM_SK, S.COMM_SRC_SYS_CD, S.COMM_TRGT_SYS_CD, S.COMM_STTUS_CD, S.COMM_TYP_CD, S.COMM_STTUS_CMNT_TX, S.LAST_UPDT_DTM, S.COMM_SENT_DTM);
"""
execute_dml(merge_sql_Upd_COMM, jdbc_url_updCOMM, jdbc_props_updCOMM)