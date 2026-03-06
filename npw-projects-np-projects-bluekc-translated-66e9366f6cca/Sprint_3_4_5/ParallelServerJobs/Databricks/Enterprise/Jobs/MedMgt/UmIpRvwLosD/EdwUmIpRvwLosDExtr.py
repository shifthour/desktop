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
# MAGIC JOB NAME:     EdwUmIpRvwLosDExtr
# MAGIC 
# MAGIC DESCRIPTION:  Extracts from IDS UM_IP_RVW_LOS to flatfile UM_IP_RVW_LOS_D.dat
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC The following IDS tables are used:
# MAGIC UM_IP_RVW_LOS
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
# MAGIC                     Flatfile: UM_IP_RVW_LOS_D.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC Developer                       Date               Project/Altiris #               Change Description                                                                                Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Tracy Davis                    3/27/2009      3808                               Original Programming.                                                                             devlEDW
# MAGIC Ralph Tucker                 8/04/20          15 - Prod Support            Renamed hash file to hf_um_ip_rvw_los_app_user                               devlEDW                       Steph Goddard            08/11/2009

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
from pyspark.sql.functions import col, when, length, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EdwRunCycle = get_widget_value('EdwRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','100')
CurrentDate = get_widget_value('CurrentDate','2009-04-01')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
 UM_IP_RVW_LOS.UM_IP_RVW_LOS_SK as UM_IP_RVW_LOS_SK,
 UM_IP_RVW_LOS.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
 UM_IP_RVW_LOS.UM_REF_ID as UM_REF_ID,
 UM_IP_RVW_LOS.UM_IP_RVW_SEQ_NO as UM_IP_RVW_SEQ_NO,
 UM_IP_RVW_LOS.UM_IP_RVW_LOS_SEQ_NO as UM_IP_RVW_LOS_SEQ_NO,
 UM_IP_RVW_LOS.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,
 UM_IP_RVW_LOS.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,
 UM_IP_RVW_LOS.UM_IP_RVW_SK as UM_IP_RVW_SK,
 UM_IP_RVW_LOS.AUTH_LOS_NO as AUTH_LOS_NO,
 UM_IP_RVW_LOS.RQST_LOS_NO as RQST_LOS_NO,
 UM_IP_RVW_LOS.UM_IP_RVW_LOS_DENIAL_RSN_CD_SK as UM_IP_RVW_LOS_DENIAL_RSN_CD_SK,
 UM_IP_RVW_LOS.DENIAL_USER_SK as DENIAL_USER_SK,
 UM_IP_RVW_LOS.PD_DAYS_NO as PD_DAYS_NO,
 UM_IP_RVW_LOS.ALW_DAYS_NO as ALW_DAYS_NO
FROM {IDSOwner}.UM_IP_RVW_LOS UM_IP_RVW_LOS
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
 APP_USER.USER_SK as USER_SK,
 APP_USER.USER_ID as USER_ID
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

write_files(
    df_IDS_Extract_App_User,
    "hf_um_ip_rvw_los_app_user.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_hf_um_ip_rvw_los_app_user = spark.read.parquet("hf_um_ip_rvw_los_app_user.parquet")

df_BusinessRules = (
    df_IDS_Extract.alias("Extract")
    .join(
        df_hf_cdma_codes.alias("refSrcSys"),
        col("Extract.SRC_SYS_CD_SK") == col("refSrcSys.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("refUmIpRvwLosDenialRsn"),
        col("Extract.UM_IP_RVW_LOS_DENIAL_RSN_CD_SK") == col("refUmIpRvwLosDenialRsn.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_um_ip_rvw_los_app_user.alias("Sttus_User_Sk_Lookup"),
        col("Extract.DENIAL_USER_SK") == col("Sttus_User_Sk_Lookup.USER_SK"),
        "left"
    )
)

df_BusinessRules_enriched = (
    df_BusinessRules
    .withColumn(
        "UM_IP_RVW_LOS_SK",
        col("Extract.UM_IP_RVW_LOS_SK")
    )
    .withColumn(
        "SRC_SYS_CD",
        when(
            (col("refSrcSys.TRGT_CD").isNull()) |
            (length(trim(col("refSrcSys.TRGT_CD")))==0),
            lit("NA")
        ).otherwise(col("refSrcSys.TRGT_CD"))
    )
    .withColumn(
        "UM_REF_ID",
        col("Extract.UM_REF_ID")
    )
    .withColumn(
        "UM_IP_RVW_SEQ_NO",
        col("Extract.UM_IP_RVW_SEQ_NO")
    )
    .withColumn(
        "UM_IP_RVW_LOS_SEQ_NO",
        col("Extract.UM_IP_RVW_LOS_SEQ_NO")
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        lit(CurrentDate)
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        lit(CurrentDate)
    )
    .withColumn(
        "DENIAL_USER_SK",
        when(col("Extract.DENIAL_USER_SK").isNull(), lit(0)).otherwise(col("Extract.DENIAL_USER_SK"))
    )
    .withColumn(
        "UM_IP_RVW_SK",
        col("Extract.UM_IP_RVW_SK")
    )
    .withColumn(
        "DENIAL_USER_ID",
        when(col("Sttus_User_Sk_Lookup.USER_ID").isNull(), lit(" ")).otherwise(col("Sttus_User_Sk_Lookup.USER_ID"))
    )
    .withColumn(
        "UM_IP_RVW_LOS_ALW_DAYS_NO",
        col("Extract.ALW_DAYS_NO")
    )
    .withColumn(
        "UM_IP_RVW_LOS_AUTH_LOS_NO",
        col("Extract.AUTH_LOS_NO")
    )
    .withColumn(
        "UM_IP_RVW_LOS_DENIAL_RSN_CD",
        when(
            (col("refUmIpRvwLosDenialRsn.TRGT_CD").isNull()) |
            (length(trim(col("refUmIpRvwLosDenialRsn.TRGT_CD")))==0),
            lit("NA")
        ).otherwise(col("refUmIpRvwLosDenialRsn.TRGT_CD"))
    )
    .withColumn(
        "UM_IP_RVW_LOS_DENIAL_RSN_NM",
        when(
            (col("refUmIpRvwLosDenialRsn.TRGT_CD_NM").isNull()) |
            (length(trim(col("refUmIpRvwLosDenialRsn.TRGT_CD_NM")))==0),
            lit("NA")
        ).otherwise(col("refUmIpRvwLosDenialRsn.TRGT_CD_NM"))
    )
    .withColumn(
        "UM_IP_RVW_LOS_PD_DAYS_NO",
        col("Extract.PD_DAYS_NO")
    )
    .withColumn(
        "UM_IP_RVW_LOS_RQST_LOS_NO",
        col("Extract.RQST_LOS_NO")
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        col("Extract.CRT_RUN_CYC_EXCTN_SK")
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        lit(EdwRunCycle)
    )
    .withColumn(
        "UM_IP_RVW_LOS_DENIAL_RSN_CD_SK",
        col("Extract.UM_IP_RVW_LOS_DENIAL_RSN_CD_SK")
    )
)

df_final = df_BusinessRules_enriched.select(
    "UM_IP_RVW_LOS_SK",
    "SRC_SYS_CD",
    "UM_REF_ID",
    "UM_IP_RVW_SEQ_NO",
    "UM_IP_RVW_LOS_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "DENIAL_USER_SK",
    "UM_IP_RVW_SK",
    "DENIAL_USER_ID",
    "UM_IP_RVW_LOS_ALW_DAYS_NO",
    "UM_IP_RVW_LOS_AUTH_LOS_NO",
    "UM_IP_RVW_LOS_DENIAL_RSN_CD",
    "UM_IP_RVW_LOS_DENIAL_RSN_NM",
    "UM_IP_RVW_LOS_PD_DAYS_NO",
    "UM_IP_RVW_LOS_RQST_LOS_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "UM_IP_RVW_LOS_DENIAL_RSN_CD_SK"
).withColumn(
    "DENIAL_USER_ID",
    rpad(col("DENIAL_USER_ID"), 10, " ")
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

write_files(
    df_final,
    f"{adls_path}/load/UM_IP_RVW_LOS_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)