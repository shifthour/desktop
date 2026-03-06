# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 10/12/09 10:26:07 Batch  15261_37578 PROMOTE bckcetl:31540 edw10 dsadm bls for rt
# MAGIC ^1_2 10/12/09 10:20:46 Batch  15261_37248 INIT bckcett:31540 testEDW dsadm BLS FOR RT
# MAGIC ^1_4 09/28/09 11:45:32 Batch  15247_42360 PROMOTE bckcett:31540 testEDW u150906 TTR583-UMMedMgt_Ralph_testEDW              Maddy
# MAGIC ^1_4 09/28/09 11:36:19 Batch  15247_41826 INIT bckcett:31540 devlEDW u150906 TTR583-UMMedMgt_Ralph_devlEDW              Maddy
# MAGIC ^1_3 09/02/09 09:53:22 Batch  15221_35654 INIT bckcett:31540 devlEDW u150906 TTR583-UMMedMgt_Ralph_devlEDW                     Maddy
# MAGIC ^1_2 08/20/09 14:03:45 Batch  15208_50662 INIT bckcett:31540 devlEDW u150906 TTR583-UMMedMgt_Ralph_devlEDW                         Maddy
# MAGIC ^1_1 05/12/09 14:25:07 Batch  15108_51913 PROMOTE bckcetl edw10 dsadm bls for rt
# MAGIC ^1_1 05/12/09 14:16:44 Batch  15108_51406 INIT bckcett:31540 testEDW dsadm BLS FOR RT
# MAGIC ^1_1 05/01/09 08:54:06 Batch  15097_32051 PROMOTE bckcett testEDW u03651 steph for Ralph
# MAGIC ^1_1 05/01/09 08:51:22 Batch  15097_31885 INIT bckcett devlEDW u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC Â© COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwUmSvcSttusDExtr
# MAGIC 
# MAGIC DESCRIPTION:  Extracts from IDS UM_SVC_STTUS to flatfile UM_SVC_STTUS_D.dat
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC The following IDS tables are used:
# MAGIC UM_SVC_STTUS
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_um_app_user - Stores USER_ID's for lookup
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Flatfile: UM_SVC_STTUS_D.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC Developer                       Date               Project/Altiris #               Change Description                                                                                Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Tracy Davis                    3/27/2009      3808                               Original Programming.                                                                             devlEDW
# MAGIC Ralph Tucker                 8/04/20          15 - Prod Support            Renamed hash file to hf_um_svc_sttus_app_user                                 devlEDW                      Steph Goddard             08/11/2009

# MAGIC Extract IDS Data
# MAGIC Apply business logic.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','2009-04-01')
EdwRunCycle = get_widget_value('EdwRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','100')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_UM_SVC_STTUS = f"""
SELECT
  STTUS.UM_SVC_STTUS_SK as UM_SVC_STTUS_SK,
  STTUS.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
  STTUS.UM_REF_ID as UM_REF_ID,
  STTUS.UM_SVC_SEQ_NO as UM_SVC_SEQ_NO,
  STTUS.UM_SVC_STTUS_SEQ_NO as UM_SVC_STTUS_SEQ_NO,
  STTUS.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,
  STTUS.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,
  STTUS.STTUS_USER_SK as STTUS_USER_SK,
  STTUS.UM_SVC_SK as UM_SVC_SK,
  STTUS.UM_SVC_STTUS_CD_SK as UM_SVC_STTUS_CD_SK,
  STTUS.UM_SVC_STTUS_RSN_CD_SK as UM_SVC_STTUS_RSN_CD_SK,
  STTUS.STTUS_DT_SK as STTUS_DT_SK
FROM {IDSOwner}.UM_SVC_STTUS STTUS
WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
   OR LAST_UPDT_RUN_CYC_EXCTN_SK = 0
   OR LAST_UPDT_RUN_CYC_EXCTN_SK = 1
"""

df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_UM_SVC_STTUS)
    .load()
)

extract_query_APP_USER = f"""
SELECT
  APP_USER.USER_SK as USER_SK,
  APP_USER.USER_ID as USER_ID
FROM {IDSOwner}.APP_USER APP_USER
"""

df_Extract_App_User = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_APP_USER)
    .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

df_hf_um_svc_sttus_app_user = dedup_sort(
    df_Extract_App_User,
    ["USER_SK"],
    [("USER_SK","A")]
)

df_ExtractAlias = df_Extract.alias("Extract")
df_refSrcSys = df_hf_cdma_codes.alias("refSrcSys")
df_refUmSvcSttus = df_hf_cdma_codes.alias("refUmSvcSttus")
df_refUmSvcSttusRsn = df_hf_cdma_codes.alias("refUmSvcSttusRsn")
df_Sttus_User_Sk_Lookup = df_hf_um_svc_sttus_app_user.alias("Sttus_User_Sk_Lookup")

df_brules = (
    df_ExtractAlias
    .join(
        df_refSrcSys,
        (F.col("Extract.SRC_SYS_CD_SK") == F.col("refSrcSys.CD_MPPNG_SK")),
        "left"
    )
    .join(
        df_refUmSvcSttus,
        (F.col("Extract.UM_SVC_STTUS_CD_SK") == F.col("refUmSvcSttus.CD_MPPNG_SK")),
        "left"
    )
    .join(
        df_refUmSvcSttusRsn,
        (F.col("Extract.UM_SVC_STTUS_RSN_CD_SK") == F.col("refUmSvcSttusRsn.CD_MPPNG_SK")),
        "left"
    )
    .join(
        df_Sttus_User_Sk_Lookup,
        (F.col("Extract.STTUS_USER_SK") == F.col("Sttus_User_Sk_Lookup.USER_SK")),
        "left"
    )
)

df_final = df_brules.select(
    F.col("Extract.UM_SVC_STTUS_SK").alias("UM_SVC_STTUS_SK"),
    rpad(
        F.when(
            (F.col("refSrcSys.TRGT_CD").isNull()) |
            (F.length(trim(F.col("refSrcSys.TRGT_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("refSrcSys.TRGT_CD")),
        <...>,
        " "
    ).alias("SRC_SYS_CD"),
    rpad(
        F.col("Extract.UM_REF_ID"),
        <...>,
        " "
    ).alias("UM_REF_ID"),
    F.col("Extract.UM_SVC_SEQ_NO").alias("UM_SVC_SEQ_NO"),
    F.col("Extract.UM_SVC_STTUS_SEQ_NO").alias("UM_SVC_STTUS_SEQ_NO"),
    rpad(F.lit(CurrentDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(F.lit(CurrentDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Extract.STTUS_USER_SK").alias("STTUS_USER_SK"),
    rpad(F.col("Extract.STTUS_DT_SK"), 10, " ").alias("STTUS_DT_SK"),
    rpad(
        F.when(
            F.col("Sttus_User_Sk_Lookup.USER_ID").isNull(),
            F.lit(" ")
        ).otherwise(F.col("Sttus_User_Sk_Lookup.USER_ID")),
        10,
        " "
    ).alias("STTUS_USER_ID"),
    F.col("Extract.UM_SVC_SK").alias("UM_SVC_SK"),
    rpad(
        F.when(
            (F.col("refUmSvcSttus.TRGT_CD").isNull()) |
            (F.length(trim(F.col("refUmSvcSttus.TRGT_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("refUmSvcSttus.TRGT_CD")),
        <...>,
        " "
    ).alias("UM_SVC_STTUS_CD"),
    rpad(
        F.when(
            (F.col("refUmSvcSttus.TRGT_CD_NM").isNull()) |
            (F.length(trim(F.col("refUmSvcSttus.TRGT_CD_NM"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("refUmSvcSttus.TRGT_CD_NM")),
        <...>,
        " "
    ).alias("UM_SVC_STTUS_NM"),
    rpad(
        F.when(
            (F.col("refUmSvcSttusRsn.TRGT_CD").isNull()) |
            (F.length(trim(F.col("refUmSvcSttusRsn.TRGT_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("refUmSvcSttusRsn.TRGT_CD")),
        <...>,
        " "
    ).alias("UM_SVC_STTUS_RSN_CD"),
    rpad(
        F.when(
            (F.col("refUmSvcSttusRsn.TRGT_CD_NM").isNull()) |
            (F.length(trim(F.col("refUmSvcSttusRsn.TRGT_CD_NM"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("refUmSvcSttusRsn.TRGT_CD_NM")),
        <...>,
        " "
    ).alias("UM_SVC_STTUS_RSN_NM"),
    F.col("Extract.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EdwRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Extract.UM_SVC_STTUS_CD_SK").alias("UM_SVC_STTUS_CD_SK"),
    F.col("Extract.UM_SVC_STTUS_RSN_CD_SK").alias("UM_SVC_STTUS_RSN_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/UM_SVC_STTUS_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)