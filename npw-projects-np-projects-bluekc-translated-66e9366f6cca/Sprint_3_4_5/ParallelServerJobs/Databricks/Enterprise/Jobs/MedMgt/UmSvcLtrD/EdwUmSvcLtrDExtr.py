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
# MAGIC Ralph Tucker                 03/31/2009    3808 - BICC                    Original Programming.                                                                             devlEDW
# MAGIC Ralph Tucker                 09/15/2009    TTR-583                         Added EdwRunCycle to last updt run cyc                                              devlEDW                      Steph Goddard            09/16/2009


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import when, lit, col, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EdwRunCycle = get_widget_value('EdwRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','100')
CurrRunDt = get_widget_value('CurrRunDt','2006-03-01')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"SELECT UM_SVC_LTR_SK,SRC_SYS_CD_SK,UM_REF_ID,UM_SVC_SEQ_NO,LTR_SEQ_NO,UM_SVC_LTR_STYLE_CD,LTR_DEST_ID,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,CRT_BY_USER_SK,LAST_UPDT_USER_SK,UM_SVC_SK,UM_SVC_LTR_TYP_CD_SK,ATCHMT_SRC_DTM,CRT_DT_SK,LAST_UPDT_DT_SK,UM_SVC_LTR_STYLE_CD_SK FROM {IDSOwner}.UM_SVC_LTR WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle} or LAST_UPDT_RUN_CYC_EXCTN_SK = 0 or LAST_UPDT_RUN_CYC_EXCTN_SK = 1"
df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_app_user_query = f"SELECT APP_USER.USER_SK as USER_SK,APP_USER.USER_ID as USER_ID FROM {IDSOwner}.APP_USER APP_USER"
df_Extract_App_User = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_app_user_query)
    .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

df_hf_um_svc_ltr_app_user = dedup_sort(df_Extract_App_User, ["USER_SK"], [])

df_enriched = (
    df_Extract.alias("Extract")
    .join(
        df_hf_cdma_codes.alias("Src_Sys_Cd_Sk_Lookup"),
        col("Extract.SRC_SYS_CD_SK") == col("Src_Sys_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Svc_Ltr_Style_Lookup"),
        col("Extract.UM_SVC_LTR_STYLE_CD_SK") == col("Um_Svc_Ltr_Style_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Svc_Ltr_Typ_Cd_Lookup"),
        col("Extract.UM_SVC_LTR_TYP_CD_SK") == col("Um_Svc_Ltr_Typ_Cd_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_um_svc_ltr_app_user.alias("Crt_User_Sk_Lookup"),
        col("Extract.CRT_BY_USER_SK") == col("Crt_User_Sk_Lookup.USER_SK"),
        "left",
    )
    .join(
        df_hf_um_svc_ltr_app_user.alias("Pri_Resp_User_Sk_Lookup"),
        col("Extract.LAST_UPDT_USER_SK") == col("Pri_Resp_User_Sk_Lookup.USER_SK"),
        "left",
    )
)

df_enriched = df_enriched.select(
    col("Extract.UM_SVC_LTR_SK").alias("UM_SVC_LTR_SK"),
    when(col("Src_Sys_Cd_Sk_Lookup.TRGT_CD").isNull(), lit(0)).otherwise(col("Src_Sys_Cd_Sk_Lookup.TRGT_CD")).alias("SRC_SYS_CD"),
    col("Extract.UM_REF_ID").alias("UM_REF_ID"),
    col("Extract.UM_SVC_SEQ_NO").alias("UM_SVC_SEQ_NO"),
    col("Extract.LTR_SEQ_NO").alias("UM_SVC_LTR_SEQ_NO"),
    col("Extract.UM_SVC_LTR_STYLE_CD").alias("UM_SVC_LTR_STYLE_CD"),
    col("Extract.LTR_DEST_ID").alias("UM_SVC_LTR_DEST_ID"),
    rpad(lit(CurrRunDt), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(lit(CurrRunDt), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("Extract.CRT_BY_USER_SK").alias("CRT_BY_USER_SK"),
    col("Extract.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    col("Extract.UM_SVC_SK").alias("UM_SVC_SK"),
    when(col("Crt_User_Sk_Lookup.USER_ID").isNull(), lit(0)).otherwise(col("Crt_User_Sk_Lookup.USER_ID")).alias("CRT_BY_USER_ID"),
    when(col("Pri_Resp_User_Sk_Lookup.USER_ID").isNull(), lit(0)).otherwise(col("Pri_Resp_User_Sk_Lookup.USER_ID")).alias("LAST_UPDT_USER_ID"),
    when(col("Extract.ATCHMT_SRC_DTM").isNull(), lit("1753-01-01")).otherwise(col("Extract.ATCHMT_SRC_DTM")).alias("UM_SVC_LTR_ATCHMT_SRC_DTM"),
    rpad(col("Extract.CRT_DT_SK"), 10, " ").alias("UM_SVC_LTR_CRT_DT_SK"),
    rpad(col("Extract.LAST_UPDT_DT_SK"), 10, " ").alias("UM_SVC_LTR_LAST_UPDT_DT_SK"),
    when(col("Um_Svc_Ltr_Style_Lookup.TRGT_CD_NM").isNull(), lit(" ")).otherwise(col("Um_Svc_Ltr_Style_Lookup.TRGT_CD_NM")).alias("UM_SVC_LTR_STYLE_NM"),
    when(col("Um_Svc_Ltr_Typ_Cd_Lookup.TRGT_CD").isNull(), lit(" ")).otherwise(col("Um_Svc_Ltr_Typ_Cd_Lookup.TRGT_CD")).alias("UM_SVC_LTR_TYP_CD"),
    when(col("Um_Svc_Ltr_Typ_Cd_Lookup.TRGT_CD_NM").isNull(), lit(" ")).otherwise(col("Um_Svc_Ltr_Typ_Cd_Lookup.TRGT_CD_NM")).alias("UM_SVC_LTR_TYP_NM"),
    col("Extract.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EdwRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Extract.UM_SVC_LTR_STYLE_CD_SK").alias("UM_SVC_LTR_STYLE_CD_SK"),
    col("Extract.UM_SVC_LTR_TYP_CD_SK").alias("UM_SVC_LTR_TYP_CD_SK"),
)

write_files(
    df_enriched,
    f"{adls_path}/load/UM_SVC_LTR_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)