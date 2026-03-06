# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 10/12/09 10:26:07 Batch  15261_37578 PROMOTE bckcetl:31540 edw10 dsadm bls for rt
# MAGIC ^1_2 10/12/09 10:20:46 Batch  15261_37248 INIT bckcett:31540 testEDW dsadm BLS FOR RT
# MAGIC ^1_2 09/28/09 11:45:32 Batch  15247_42360 PROMOTE bckcett:31540 testEDW u150906 TTR583-UMMedMgt_Ralph_testEDW              Maddy
# MAGIC ^1_2 09/28/09 11:36:19 Batch  15247_41826 INIT bckcett:31540 devlEDW u150906 TTR583-UMMedMgt_Ralph_devlEDW              Maddy
# MAGIC ^1_1 05/12/09 14:25:07 Batch  15108_51913 PROMOTE bckcetl edw10 dsadm bls for rt
# MAGIC ^1_1 05/12/09 14:16:44 Batch  15108_51406 INIT bckcett:31540 testEDW dsadm BLS FOR RT
# MAGIC ^1_1 05/01/09 08:54:06 Batch  15097_32051 PROMOTE bckcett testEDW u03651 steph for Ralph
# MAGIC ^1_1 05/01/09 08:51:22 Batch  15097_31885 INIT bckcett devlEDW u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:             
# MAGIC                                                                                                                                                                                    
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Ralph Tucker                 03/30/2009       3808 - BICC                 Original Programming.                                                                             devlEDW                     Steph Goddard             04/03/2009
# MAGIC Ralph Tucker                  09/15/2009     TTR-583                       Added EdwRunCycle to last updt run cyc                                              devlEDW                     Steph Goddard             09/16/2009


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
CurrRunDt = get_widget_value('CurrRunDt','2006-03-01')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EdwRunCycle = get_widget_value('EdwRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','100')

# Database configuration for IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Extract (UM_IP_LTR) from IDS
extract_query_Extract = (
    f"SELECT UM_IP_LTR_SK,SRC_SYS_CD_SK,UM_REF_ID,LTR_SEQ_NO,UM_IP_LTR_STYLE_CD,LTR_DEST_ID,"
    f"CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,CRT_BY_USER_SK,LAST_UPDT_USER_SK,UM_SK,"
    f"UM_IP_LTR_TYP_CD_SK,ATCHMT_SRC_DTM,CRT_DT_SK,LAST_UPDT_DT_SK,UM_IP_LTR_STYLE_CD_SK "
    f"FROM {IDSOwner}.UM_IP_LTR UIL "
    f"WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle} OR LAST_UPDT_RUN_CYC_EXCTN_SK=0 OR LAST_UPDT_RUN_CYC_EXCTN_SK=1"
)
df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Extract)
    .load()
)

# Extract (APP_USER) from IDS
extract_query_Extract_App_User = (
    f"SELECT APP_USER.USER_SK as USER_SK, APP_USER.USER_ID as USER_ID "
    f"FROM {IDSOwner}.APP_USER APP_USER"
)
df_Extract_App_User = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Extract_App_User)
    .load()
)

# Read hf_cdma_codes (CHashedFileStage) - Scenario C
df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

# hf_um_ip_ltr_app_user (CHashedFileStage) - Scenario A (intermediate hashed file)
df_Extract_App_User_dedup = dedup_sort(
    df_Extract_App_User,
    partition_cols=["USER_SK"],
    sort_cols=[]
)

# Business_Rules (Transformer) with multiple lookups
df_Business_Rules = (
    df_Extract.alias("Extract")
    .join(
        df_hf_cdma_codes.alias("Src_Sys_Cd_Sk_Lookup"),
        col("Extract.SRC_SYS_CD_SK") == col("Src_Sys_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("Um_Ip_Ltr_Style_Lookup"),
        col("Extract.UM_IP_LTR_STYLE_CD_SK") == col("Um_Ip_Ltr_Style_Lookup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("Um_Ip_Ltr_Typ_Cd_Lookup"),
        col("Extract.UM_IP_LTR_TYP_CD_SK") == col("Um_Ip_Ltr_Typ_Cd_Lookup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Extract_App_User_dedup.alias("Crt_User_Sk_Lookup"),
        col("Extract.CRT_BY_USER_SK") == col("Crt_User_Sk_Lookup.USER_SK"),
        "left"
    )
    .join(
        df_Extract_App_User_dedup.alias("Pri_Resp_User_Sk_Lookup"),
        col("Extract.LAST_UPDT_USER_SK") == col("Pri_Resp_User_Sk_Lookup.USER_SK"),
        "left"
    )
)

df_Business_Rules_final = df_Business_Rules.select(
    col("Extract.UM_IP_LTR_SK").alias("UM_IP_LTR_SK"),
    rpad(
        when(col("Src_Sys_Cd_Sk_Lookup.TRGT_CD").isNull(), lit("0"))
        .otherwise(col("Src_Sys_Cd_Sk_Lookup.TRGT_CD")),
        <...>,
        " "
    ).alias("SRC_SYS_CD"),
    col("Extract.UM_REF_ID").alias("UM_REF_ID"),
    col("Extract.LTR_SEQ_NO").alias("UM_IP_LTR_SEQ_NO"),
    col("Extract.UM_IP_LTR_STYLE_CD").alias("UM_IP_LTR_STYLE_CD"),
    col("Extract.LTR_DEST_ID").alias("UM_IP_LTR_DEST_ID"),
    rpad(lit(CurrRunDt), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(lit(CurrRunDt), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("Extract.CRT_BY_USER_SK").alias("CRT_BY_USER_SK"),
    col("Extract.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    col("Extract.UM_SK").alias("UM_SK"),
    rpad(col("Crt_User_Sk_Lookup.USER_ID"), 10, " ").alias("CRT_BY_USER_ID"),
    rpad(col("Pri_Resp_User_Sk_Lookup.USER_ID"), 10, " ").alias("LAST_UPDT_USER_ID"),
    col("Extract.ATCHMT_SRC_DTM").alias("UM_IP_LTR_ATCHMT_SRC_DTM"),
    rpad(col("Extract.CRT_DT_SK"), 10, " ").alias("UM_IP_LTR_CRT_DT_SK"),
    rpad(col("Extract.LAST_UPDT_DT_SK"), 10, " ").alias("UM_IP_LTR_LAST_UPDT_DT_SK"),
    rpad(
        when(col("Um_Ip_Ltr_Style_Lookup.TRGT_CD_NM").isNull(), lit(" "))
        .otherwise(col("Um_Ip_Ltr_Style_Lookup.TRGT_CD_NM")),
        <...>,
        " "
    ).alias("UM_IP_LTR_STYLE_NM"),
    rpad(
        when(col("Um_Ip_Ltr_Typ_Cd_Lookup.TRGT_CD").isNull(), lit(" "))
        .otherwise(col("Um_Ip_Ltr_Typ_Cd_Lookup.TRGT_CD")),
        <...>,
        " "
    ).alias("UM_IP_LTR_TYP_CD"),
    rpad(
        when(col("Um_Ip_Ltr_Typ_Cd_Lookup.TRGT_CD_NM").isNull(), lit(" "))
        .otherwise(col("Um_Ip_Ltr_Typ_Cd_Lookup.TRGT_CD_NM")),
        <...>,
        " "
    ).alias("UM_IP_LTR_TYP_NM"),
    col("Extract.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EdwRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Extract.UM_IP_LTR_STYLE_CD_SK").alias("UM_IP_LTR_STYLE_CD_SK"),
    col("Extract.UM_IP_LTR_TYP_CD_SK").alias("UM_IP_LTR_TYP_CD_SK")
)

# Write the final file (UM_IP_LTR_D.dat)
write_files(
    df_Business_Rules_final,
    f"{adls_path}/load/UM_IP_LTR_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)