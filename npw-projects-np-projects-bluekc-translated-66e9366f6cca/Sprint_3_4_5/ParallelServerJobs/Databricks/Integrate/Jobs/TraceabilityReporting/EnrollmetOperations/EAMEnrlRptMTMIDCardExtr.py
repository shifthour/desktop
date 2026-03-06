# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Calling Job: EAMEnrollmentOpsRptCntl
# MAGIC 
# MAGIC This Job extracts Mbr ID Cards data from MTM db and load to EAM table which will be used to update the base table.
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ================================================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                                                      Development Project                         Reviewer                     Review Date
# MAGIC ================================================================================================================================================================================================
# MAGIC ReddySanam               2021-05-24                  US384030                                  Original Programming                                                                                       IntegrateDev2                                    Jeyaprasanna              2021-06-05
# MAGIC ReddySanam               2021-05-24                  US384030                                  Added sequence file for FiServ history load (Oct2020-Jan5th2021)                 IntegrateDev2                                    Goutham K                  2021-07-22
# MAGIC                                                                                                                            Added before ExecSH command to copy file to external dir

# MAGIC This Job extracts Mbr ID Cards data from MTM and loads to EAM database table
# MAGIC MTM has data on or after Jan6th 2021. None of the data from Oct 2020 to Jan5th2021 is not available in MTM database. So this historical data is loaded through a file that is provided by Fiserv.
# MAGIC 
# MAGIC This file is put in #$FilePath#/update/ dir and copied to #$FilePath#/external dir and cleaned up after the run from external dir
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, coalesce, concat, to_timestamp, substring, unionByName, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RUNID = get_widget_value('RUNID','')
CurrDt = get_widget_value('CurrDt','')
MTMOwner = get_widget_value('MTMOwner','')
mtm_secret_name = get_widget_value('mtm_secret_name','')

# Stage: MTMIDCardData (ODBCConnectorPX)
query_MTMIDCardData = (
    f"SELECT GRP_ID,SUB_ID,MBR_SFX_NO,CARD_RSN_CD,RQST_DT,VNDR_MAIL_DT,SRC_SYS_CD,MBR_NM,BUS_SRC_NM,SUBGRP_ID,ACA_ID,RUN_DT "
    f"FROM {MTMOwner}.MBR_ID_CARD_RQST IDCARDS "
    f"WHERE IDCARDS.BUS_SRC_NM = 'MEDICARE ADVANTAGE' "
    f"OR (IDCARDS.GRP_ID = '10001000' AND (IDCARDS.SUBGRP_ID IN('0001', '0002')))"
)
jdbc_url, jdbc_props = get_db_config(mtm_secret_name)
df_MTMIDCardData = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_MTMIDCardData)
    .load()
)

# Stage: BRules (CTransformerStage)
df_MTMTable = df_MTMIDCardData.withColumn("GRP_ID", trim(col("GRP_ID"))) \
    .withColumn("SUB_ID", trim(col("SUB_ID"))) \
    .withColumn("MBR_SFX_NO", trim(col("MBR_SFX_NO"))) \
    .withColumn("CARD_RSN_CD", trim(col("CARD_RSN_CD"))) \
    .withColumn("RQST_DT", col("RQST_DT")) \
    .withColumn("VNDR_MAIL_DT", col("VNDR_MAIL_DT")) \
    .withColumn("SRC_SYS_CD", trim(col("SRC_SYS_CD"))) \
    .withColumn("MBR_NM", trim(coalesce(col("MBR_NM"), lit("")))) \
    .withColumn("BUS_SRC_NM", trim(coalesce(col("BUS_SRC_NM"), lit("")))) \
    .withColumn("SUBGRP_ID", trim(coalesce(col("SUBGRP_ID"), lit("")))) \
    .withColumn("ACA_ID", trim(coalesce(col("ACA_ID"), lit("")))) \
    .withColumn(
        "PRCS_RUN_DT",
        to_timestamp(concat(lit(CurrDt), lit(" 00:00:00")), "yyyy-MM-dd HH:mm:ss")
    ) \
    .withColumn("RUN_DT", col("RUN_DT"))

df_MTMTable = df_MTMTable.select(
    "GRP_ID",
    "SUB_ID",
    "MBR_SFX_NO",
    "CARD_RSN_CD",
    "RQST_DT",
    "VNDR_MAIL_DT",
    "SRC_SYS_CD",
    "MBR_NM",
    "BUS_SRC_NM",
    "SUBGRP_ID",
    "ACA_ID",
    "PRCS_RUN_DT",
    "RUN_DT"
)

# Stage: FiservHistory (PxSequentialFile)
schema_FiservHistory = StructType([
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("SUB_ID", StringType(), nullable=False),
    StructField("MBR_SFX_NO", StringType(), nullable=False),
    StructField("CARD_RSN_CD", StringType(), nullable=False),
    StructField("RQST_DT", StringType(), nullable=False),
    StructField("VNDR_MAIL_DT", StringType(), nullable=True),
    StructField("MBR_NM", StringType(), nullable=True),
    StructField("SUBGRP_ID", StringType(), nullable=True),
    StructField("RUN_DT", StringType(), nullable=False)
])
csvFilePath_FiservHistory = f"{adls_path_publish}/external/EAM_ENRL_OPS_MTMIDCARD_History.csv.{RUNID}"
df_FiservHistory = (
    spark.read.format("csv")
    .option("header", "true")
    .option("sep", ",")
    .schema(schema_FiservHistory)
    .load(csvFilePath_FiservHistory)
)

# Stage: Map_HistoryData (CTransformerStage)
df_HistoryFile = df_FiservHistory.withColumn("GRP_ID", trim(col("GRP_ID"))) \
    .withColumn("SUB_ID", trim(col("SUB_ID"))) \
    .withColumn("MBR_SFX_NO", trim(col("MBR_SFX_NO"))) \
    .withColumn("CARD_RSN_CD", trim(col("CARD_RSN_CD"))) \
    .withColumn(
        "RQST_DT",
        to_timestamp(
            concat(
                substring(col("RQST_DT"), 7, 4),
                substring(col("RQST_DT"), 1, 2),
                substring(col("RQST_DT"), 4, 2),
                lit(" 00:00:00")
            ),
            "yyyyMMdd HH:mm:ss"
        )
    ) \
    .withColumn(
        "VNDR_MAIL_DT",
        to_timestamp(
            concat(
                substring(col("VNDR_MAIL_DT"), 7, 4),
                substring(col("VNDR_MAIL_DT"), 1, 2),
                substring(col("VNDR_MAIL_DT"), 4, 2),
                lit(" 00:00:00")
            ),
            "yyyyMMdd HH:mm:ss"
        )
    ) \
    .withColumn("SRC_SYS_CD", lit("FACETS")) \
    .withColumn("MBR_NM", trim(coalesce(col("MBR_NM"), lit("")))) \
    .withColumn("BUS_SRC_NM", lit("MEDICARE ADVANTAGE")) \
    .withColumn("SUBGRP_ID", trim(coalesce(col("SUBGRP_ID"), lit("")))) \
    .withColumn("ACA_ID", lit("Non-ACA")) \
    .withColumn(
        "PRCS_RUN_DT",
        to_timestamp(concat(lit(CurrDt), lit(" 00:00:00")), "yyyy-MM-dd HH:mm:ss")
    ) \
    .withColumn(
        "RUN_DT",
        to_timestamp(
            concat(
                substring(col("RUN_DT"), 1, 4),
                substring(col("RUN_DT"), 5, 2),
                substring(col("RUN_DT"), 7, 2),
                lit(" 00:00:00")
            ),
            "yyyyMMdd HH:mm:ss"
        )
    )

df_HistoryFile = df_HistoryFile.select(
    "GRP_ID",
    "SUB_ID",
    "MBR_SFX_NO",
    "CARD_RSN_CD",
    "RQST_DT",
    "VNDR_MAIL_DT",
    "SRC_SYS_CD",
    "MBR_NM",
    "BUS_SRC_NM",
    "SUBGRP_ID",
    "ACA_ID",
    "PRCS_RUN_DT",
    "RUN_DT"
)

# Stage: Fnl (PxFunnel)
df_Fnl = df_HistoryFile.unionByName(df_MTMTable)
df_Fnl = df_Fnl.select(
    "GRP_ID",
    "SUB_ID",
    "MBR_SFX_NO",
    "CARD_RSN_CD",
    "RQST_DT",
    "VNDR_MAIL_DT",
    "SRC_SYS_CD",
    "MBR_NM",
    "BUS_SRC_NM",
    "SUBGRP_ID",
    "ACA_ID",
    "PRCS_RUN_DT",
    "RUN_DT"
)

# Stage: New (PxSequentialFile) - Write output
df_final = df_Fnl.select(
    rpad(col("GRP_ID"), <...>, " ").alias("GRP_ID"),
    rpad(col("SUB_ID"), <...>, " ").alias("SUB_ID"),
    rpad(col("MBR_SFX_NO"), <...>, " ").alias("MBR_SFX_NO"),
    rpad(col("CARD_RSN_CD"), <...>, " ").alias("CARD_RSN_CD"),
    rpad(col("RQST_DT"), <...>, " ").alias("RQST_DT"),
    rpad(col("VNDR_MAIL_DT"), <...>, " ").alias("VNDR_MAIL_DT"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("MBR_NM"), <...>, " ").alias("MBR_NM"),
    rpad(col("BUS_SRC_NM"), <...>, " ").alias("BUS_SRC_NM"),
    rpad(col("SUBGRP_ID"), <...>, " ").alias("SUBGRP_ID"),
    rpad(col("ACA_ID"), <...>, " ").alias("ACA_ID"),
    rpad(col("PRCS_RUN_DT"), <...>, " ").alias("PRCS_RUN_DT"),
    rpad(col("RUN_DT"), <...>, " ").alias("RUN_DT")
)

write_files(
    df_final,
    f"{adls_path}/load/EAM_ENRL_OPS_MTMIDCARD_LOAD.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)