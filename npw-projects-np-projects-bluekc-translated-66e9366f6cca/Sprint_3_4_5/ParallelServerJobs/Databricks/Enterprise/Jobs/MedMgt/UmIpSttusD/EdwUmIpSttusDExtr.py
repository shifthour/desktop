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
# MAGIC JOB NAME:     EdwUmIpSttusDExtr
# MAGIC 
# MAGIC DESCRIPTION:  Extracts from IDS UM_IP_STTUS to flatfile UM_IP_STTUS_D.dat
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC The following IDS tables are used:
# MAGIC UM_IP_STTUS
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
# MAGIC                     Flatfile: UM_IP_STTUS_D.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC Developer                       Date               Project/Altiris #               Change Description                                                                                Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Tracy Davis                    3/27/2009      3808                               Original Programming.                                                                             devlEDW                     Steph Goddard             04/03/2009
# MAGIC Ralph Tucker                 7/22/20          15 - Prod Support            Renamed hash file to hf_um_ip_sttus_app_user                                    devlEDW                     Steph Goddard             08/11/2009

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, when, length, rpad, trim
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------

jdbc_url_ids, jdbc_props_ids = None, None

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
EdwRunCycle = get_widget_value('EdwRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
  UM_IP_STTUS.UM_IP_STTUS_SK AS UM_IP_STTUS_SK,
  UM_IP_STTUS.SRC_SYS_CD_SK AS SRC_SYS_CD_SK,
  UM_IP_STTUS.UM_REF_ID AS UM_REF_ID,
  UM_IP_STTUS.UM_IP_STTUS_SEQ_NO AS UM_IP_STTUS_SEQ_NO,
  UM_IP_STTUS.CRT_RUN_CYC_EXCTN_SK AS CRT_RUN_CYC_EXCTN_SK,
  UM_IP_STTUS.LAST_UPDT_RUN_CYC_EXCTN_SK AS LAST_UPDT_RUN_CYC_EXCTN_SK,
  UM_IP_STTUS.STTUS_USER_SK AS STTUS_USER_SK,
  UM_IP_STTUS.UM_SK AS UM_SK,
  UM_IP_STTUS.UM_IP_STTUS_CD_SK AS UM_IP_STTUS_CD_SK,
  UM_IP_STTUS.UM_IP_STTUS_RSN_CD_SK AS UM_IP_STTUS_RSN_CD_SK,
  UM_IP_STTUS.STTUS_DT_SK AS STTUS_DT_SK
FROM {IDSOwner}.UM_IP_STTUS UM_IP_STTUS
WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
   OR LAST_UPDT_RUN_CYC_EXCTN_SK = 0
   OR LAST_UPDT_RUN_CYC_EXCTN_SK = 1
"""

df_IDS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

extract_app_user_query = f"""
SELECT
  APP_USER.USER_SK AS USER_SK,
  APP_USER.USER_ID AS USER_ID
FROM {IDSOwner}.APP_USER APP_USER
"""

df_IDS_Extract_App_User = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_app_user_query)
    .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

df_hf_um_ip_sttus_app_user = dedup_sort(df_IDS_Extract_App_User, ["USER_SK"], [("USER_SK","A")])

df_businessrules = (
    df_IDS_Extract.alias("Extract")
    .join(
        df_hf_cdma_codes.alias("refSrcSys"),
        col("Extract.SRC_SYS_CD_SK") == col("refSrcSys.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("refUmIpSttus"),
        col("Extract.UM_IP_STTUS_CD_SK") == col("refUmIpSttus.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("refUmIpSttusRsn"),
        col("Extract.UM_IP_STTUS_RSN_CD_SK") == col("refUmIpSttusRsn.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_um_ip_sttus_app_user.alias("Sttus_User_Sk_Lookup"),
        col("Extract.STTUS_USER_SK") == col("Sttus_User_Sk_Lookup.USER_SK"),
        "left"
    )
)

df_businessrules = (
    df_businessrules
    .withColumn("UM_IP_STTUS_SK", col("Extract.UM_IP_STTUS_SK"))
    .withColumn(
        "SRC_SYS_CD",
        when(
            col("refSrcSys.TRGT_CD").isNull() | (length(trim(col("refSrcSys.TRGT_CD"))) == 0),
            lit("NA")
        ).otherwise(col("refSrcSys.TRGT_CD"))
    )
    .withColumn("UM_REF_ID", col("Extract.UM_REF_ID"))
    .withColumn("UM_IP_STTUS_SEQ_NO", col("Extract.UM_IP_STTUS_SEQ_NO"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(CurrentDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(CurrentDate))
    .withColumn("STATUS_USER_SK", col("Extract.STTUS_USER_SK"))
    .withColumn("UM_SK", col("Extract.UM_SK"))
    .withColumn("STTUS_DT_SK", col("Extract.STTUS_DT_SK"))
    .withColumn(
        "STTUS_USER_ID",
        when(col("Sttus_User_Sk_Lookup.USER_ID").isNull(), lit(" ")).otherwise(col("Sttus_User_Sk_Lookup.USER_ID"))
    )
    .withColumn(
        "UM_IP_STTUS_CD",
        when(
            col("refUmIpSttus.TRGT_CD").isNull() | (length(trim(col("refUmIpSttus.TRGT_CD"))) == 0),
            lit("NA")
        ).otherwise(col("refUmIpSttus.TRGT_CD"))
    )
    .withColumn(
        "UM_IP_STTUS_NM",
        when(
            col("refUmIpSttus.TRGT_CD_NM").isNull() | (length(trim(col("refUmIpSttus.TRGT_CD_NM"))) == 0),
            lit("NA")
        ).otherwise(col("refUmIpSttus.TRGT_CD_NM"))
    )
    .withColumn(
        "UM_IP_STTUS_RSN_CD",
        when(
            col("refUmIpSttusRsn.TRGT_CD").isNull() | (length(trim(col("refUmIpSttusRsn.TRGT_CD"))) == 0),
            lit("NA")
        ).otherwise(col("refUmIpSttusRsn.TRGT_CD"))
    )
    .withColumn(
        "UM_IP_STTUS_RSN_NM",
        when(
            col("refUmIpSttusRsn.TRGT_CD_NM").isNull() | (length(trim(col("refUmIpSttusRsn.TRGT_CD_NM"))) == 0),
            lit("NA")
        ).otherwise(col("refUmIpSttusRsn.TRGT_CD_NM"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", col("Extract.CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(EdwRunCycle))
    .withColumn("UM_IP_STTUS_CD_SK", col("Extract.UM_IP_STTUS_CD_SK"))
    .withColumn("UM_IP_STTUS_RSN_CD_SK", col("Extract.UM_IP_STTUS_RSN_CD_SK"))
)

df_UM_IP_STTUS_D = df_businessrules.select(
    "UM_IP_STTUS_SK",
    "SRC_SYS_CD",
    "UM_REF_ID",
    "UM_IP_STTUS_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "STATUS_USER_SK",
    "UM_SK",
    "STTUS_DT_SK",
    "STTUS_USER_ID",
    "UM_IP_STTUS_CD",
    "UM_IP_STTUS_NM",
    "UM_IP_STTUS_RSN_CD",
    "UM_IP_STTUS_RSN_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "UM_IP_STTUS_CD_SK",
    "UM_IP_STTUS_RSN_CD_SK"
)

df_UM_IP_STTUS_D = (
    df_UM_IP_STTUS_D
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " "))
    .withColumn("UM_REF_ID", rpad(col("UM_REF_ID"), <...>, " "))
    .withColumn("UM_IP_STTUS_SEQ_NO", rpad(col("UM_IP_STTUS_SEQ_NO"), <...>, " "))
    .withColumn("STATUS_USER_SK", rpad(col("STATUS_USER_SK"), <...>, " "))
    .withColumn("UM_SK", rpad(col("UM_SK"), <...>, " "))
    .withColumn("UM_IP_STTUS_CD", rpad(col("UM_IP_STTUS_CD"), <...>, " "))
    .withColumn("UM_IP_STTUS_NM", rpad(col("UM_IP_STTUS_NM"), <...>, " "))
    .withColumn("UM_IP_STTUS_RSN_CD", rpad(col("UM_IP_STTUS_RSN_CD"), <...>, " "))
    .withColumn("UM_IP_STTUS_RSN_NM", rpad(col("UM_IP_STTUS_RSN_NM"), <...>, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", rpad(col("CRT_RUN_CYC_EXCTN_SK"), <...>, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_SK"), <...>, " "))
    .withColumn("UM_IP_STTUS_CD_SK", rpad(col("UM_IP_STTUS_CD_SK"), <...>, " "))
    .withColumn("UM_IP_STTUS_RSN_CD_SK", rpad(col("UM_IP_STTUS_RSN_CD_SK"), <...>, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("STTUS_DT_SK", rpad(col("STTUS_DT_SK"), 10, " "))
    .withColumn("STTUS_USER_ID", rpad(col("STTUS_USER_ID"), 10, " "))
)

write_files(
    df_UM_IP_STTUS_D,
    f"{adls_path}/load/UM_IP_STTUS_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)