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
# MAGIC Bharani Chalamalasetty      05/18/2021             Fulfillment                      new code                                                                 \(9)OutboundDev3                    Jaideep Mankala             05/19/2021 
# MAGIC 
# MAGIC Saranya A                                2021-06-30             US370660                        Added Lookup Stage to separate \(9)          OutboundDev3                   Jaideep Mankala                 06/30/2021 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)not processed records
# MAGIC \(9)\(9)\(9)\(9)\(9)


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, when, length, to_timestamp
# COMMAND ----------
# MAGIC %run ../../../../Utility_Outbound
# COMMAND ----------


# Parameters
NXTPGInboundFile = get_widget_value('NXTPGInboundFile','')
RunID = get_widget_value('RunID','')
CommTrnsmsnOwner = get_widget_value('CommTrnsmsnOwner','')
commtrnsmsn_secret_name = get_widget_value('commtrnsmsn_secret_name','')
CurrDateTm = get_widget_value('CurrDateTm','')
NXTPGInboundRspnFile = get_widget_value('NXTPGInboundRspnFile','')

# Stages

# COMM (ODBCConnectorPX)
jdbc_url_comm, jdbc_props_comm = get_db_config(commtrnsmsn_secret_name)
extract_query_comm = (
    f"select COMM_SK, COMM_ID\n"
    f"from\n"
    f"(\n"
    f"SELECT COMM_SK, COMM_ID, \n"
    f"rank() over(partition by COMM_ID order by COMM_SK desc) as ranknum\n"
    f"from {CommTrnsmsnOwner}.[COMM]\n"
    f"where COMM_TRGT_SYS_MDUL_CD='NXTPG'\n"
    f") a where a.ranknum=1"
)
df_COMM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_comm)
    .options(**jdbc_props_comm)
    .option("query", extract_query_comm)
    .load()
)

# Seq_MailAcknowledgement (PxSequentialFile)
schema_Ack = StructType([
    StructField("LetterQueueId", StringType(), nullable=False),
    StructField("MailStatus", StringType(), nullable=False),
    StructField("MailDate", StringType(), nullable=True)
])
df_Seq_MailAcknowledgement = (
    spark.read
    .schema(schema_Ack)
    .option("header", True)
    .option("sep", "|")
    .csv(f"{adls_path_raw}/landing/{NXTPGInboundFile}")
)

# Seq_MailResponse (PxSequentialFile)
schema_Rsp = StructType([
    StructField("LetterQueueId", StringType(), nullable=True),
    StructField("MailStatus", StringType(), nullable=True),
    StructField("MailDate", StringType(), nullable=True)
])
df_Seq_MailResponse = (
    spark.read
    .schema(schema_Rsp)
    .option("header", True)
    .option("sep", "|")
    .csv(f"{adls_path_raw}/landing/{NXTPGInboundRspnFile}")
)

# Lkup_CommID (PxLookup) - Primary: df_Seq_MailAcknowledgement, Lookup: df_Seq_MailResponse
df_Lkup_CommID = (
    df_Seq_MailAcknowledgement.alias("lnk_MailAck")
    .join(
        df_Seq_MailResponse.alias("lnk_MailRsp"),
        col("lnk_MailAck.LetterQueueId") == col("lnk_MailRsp.LetterQueueId"),
        "left"
    )
    .select(
        col("lnk_MailAck.LetterQueueId").alias("LetterQueueId"),
        col("lnk_MailAck.MailStatus").alias("MailStatus"),
        col("lnk_MailAck.MailDate").alias("MailDate"),
        col("lnk_MailRsp.LetterQueueId").alias("LetterQueueId_Mail")
    )
)

# lkp_COMM (PxLookup) - Primary: df_Lkup_CommID, Lookup: df_COMM with no join condition
df_lkp_COMM_join = (
    df_Lkup_CommID.alias("lnk_MailResponse")
    .join(df_COMM.alias("lnk_COMM"), lit(False), "left")
)

df_lkp_COMM_primary = (
    df_lkp_COMM_join
    .filter(col("lnk_COMM.COMM_SK").isNotNull())
    .select(
        col("lnk_COMM.COMM_SK").alias("COMM_SK"),
        col("lnk_MailResponse.LetterQueueId").alias("LetterQueueID"),
        col("lnk_MailResponse.MailStatus").alias("MailStatus"),
        col("lnk_MailResponse.MailDate").alias("MailDate"),
        col("lnk_MailResponse.LetterQueueId_Mail").alias("LetterQueueId_Mail")
    )
)

df_lkp_COMM_reject = (
    df_lkp_COMM_join
    .filter(col("lnk_COMM.COMM_SK").isNull())
    .select(
        col("lnk_MailResponse.LetterQueueId").alias("LetterQueueID"),
        col("lnk_MailResponse.MailStatus").alias("MailStatus"),
        col("lnk_MailResponse.MailDate").alias("MailDate"),
        col("lnk_MailResponse.LetterQueueId_Mail").alias("LetterQueueId_Mail")
    )
)

# RejectFile (PxSequentialFile)
df_RejectFile_out = (
    df_lkp_COMM_reject
    .withColumn("LetterQueueID", rpad(col("LetterQueueID"), <...>, " "))
    .withColumn("MailStatus", rpad(col("MailStatus"), <...>, " "))
    .withColumn("MailDate", rpad(col("MailDate"), <...>, " "))
    .withColumn("LetterQueueId_Mail", rpad(col("LetterQueueId_Mail"), <...>, " "))
)
write_files(
    df_RejectFile_out,
    f"{adls_path_publish}/external/Reject_{NXTPGInboundFile}_{RunID}.txt",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# xfm_RequiredFields (CTransformerStage)
df_enriched_1 = df_lkp_COMM_primary.filter(
    (col("LetterQueueId_Mail").isNotNull())
    & (trim(col("LetterQueueId_Mail")) != "")
    & (length(trim(col("LetterQueueId_Mail"))) > 0)
)

df_enriched_2 = df_lkp_COMM_primary.filter(
    (col("LetterQueueId_Mail").isNull())
    | (trim(col("LetterQueueId_Mail")) == "")
    | (length(trim(col("LetterQueueId_Mail"))) == 0)
)

df_Upd_COMM = (
    df_enriched_1
    .select(
        col("COMM_SK").alias("COMM_SK"),
        lit("NXTPG").alias("COMM_SRC_SYS_CD"),
        lit("COMMTRNSMSN").alias("COMM_TRGT_SYS_CD"),
        when(trim(col("MailStatus")) == "MAIL", "PDFRCVD").otherwise("NO-PDF").alias("COMM_STTUS_CD"),
        lit("MAILACK").alias("COMM_TYP_CD"),
        when(trim(col("MailStatus")) == "MAIL", "NextPage received PDF file").otherwise("NextPage did not received the PDF file").alias("COMM_STTUS_CMNT_TX"),
        lit(CurrDateTm).alias("LAST_UPDT_DTM"),
        when(
            (trim(col("MailDate")) == "NULL") | (trim(col("MailDate")) == "") | col("MailDate").isNull(),
            lit(CurrDateTm)
        ).otherwise(to_timestamp(col("MailDate"), "yyyyMMdd")).alias("COMM_SENT_DTM")
    )
)

# For the REST "Not Processed" data
df_Rest_AckData = (
    df_enriched_2
    .select(
        col("LetterQueueID"),
        col("MailStatus"),
        col("MailDate")
    )
)

# Upd_COMM (ODBCConnectorPX) - Merge into #$CommTrnsmsnOwner#.comm
jdbc_url_upd_comm, jdbc_props_upd_comm = get_db_config(commtrnsmsn_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.NextPageBlueKCMailAcknlgmntStsUpdate_Upd_COMM_temp",
    jdbc_url_upd_comm,
    jdbc_props_upd_comm
)

df_Upd_COMM_out = (
    df_Upd_COMM
    .withColumn("COMM_SK", rpad(col("COMM_SK"), <...>, " "))  # If it were varchar, unknown length
    .withColumn("COMM_SRC_SYS_CD", rpad(col("COMM_SRC_SYS_CD"), <...>, " "))
    .withColumn("COMM_TRGT_SYS_CD", rpad(col("COMM_TRGT_SYS_CD"), <...>, " "))
    .withColumn("COMM_STTUS_CD", rpad(col("COMM_STTUS_CD"), <...>, " "))
    .withColumn("COMM_TYP_CD", rpad(col("COMM_TYP_CD"), <...>, " "))
    .withColumn("COMM_STTUS_CMNT_TX", rpad(col("COMM_STTUS_CMNT_TX"), <...>, " "))
    .withColumn("LAST_UPDT_DTM", rpad(col("LAST_UPDT_DTM"), <...>, " "))
    .withColumn("COMM_SENT_DTM", rpad(col("COMM_SENT_DTM"), <...>, " "))
)

(
    df_Upd_COMM_out.write
    .format("jdbc")
    .option("url", jdbc_url_upd_comm)
    .options(**jdbc_props_upd_comm)
    .option("dbtable", "STAGING.NextPageBlueKCMailAcknlgmntStsUpdate_Upd_COMM_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE {CommTrnsmsnOwner}.comm AS T
USING STAGING.NextPageBlueKCMailAcknlgmntStsUpdate_Upd_COMM_temp AS S
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
    INSERT (
        COMM_SK,
        COMM_SRC_SYS_CD,
        COMM_TRGT_SYS_CD,
        COMM_STTUS_CD,
        COMM_TYP_CD,
        COMM_STTUS_CMNT_TX,
        LAST_UPDT_DTM,
        COMM_SENT_DTM
    )
    VALUES (
        S.COMM_SK,
        S.COMM_SRC_SYS_CD,
        S.COMM_TRGT_SYS_CD,
        S.COMM_STTUS_CD,
        S.COMM_TYP_CD,
        S.COMM_STTUS_CMNT_TX,
        S.LAST_UPDT_DTM,
        S.COMM_SENT_DTM
    );
"""
execute_dml(merge_sql, jdbc_url_upd_comm, jdbc_props_upd_comm)

# Rest_AckData (PxSequentialFile)
df_Rest_AckData_out = (
    df_Rest_AckData
    .withColumn("LetterQueueID", rpad(col("LetterQueueID"), <...>, " "))
    .withColumn("MailStatus", rpad(col("MailStatus"), <...>, " "))
    .withColumn("MailDate", rpad(col("MailDate"), <...>, " "))
)
write_files(
    df_Rest_AckData_out,
    f"{adls_path_raw}/landing/NotProcessed_{NXTPGInboundFile}_{RunID}.txt",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)