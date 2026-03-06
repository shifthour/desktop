# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2016 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsEdwCaseMgtContactDtlLogDExtr
# MAGIC 
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                This Job extracts Data from IDS and creates a Load file for EDW TableCASE_MGT_CNTCT_DTL_LOG_D.         
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                            Development Project           Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                       ----------------------------------        ---------------------------------    -------------------------   
# MAGIC Akash Parsha                   10/07/2019      140165                       Original Program to Extract CM Contact Dtl log data from IDS            EnterpriseDev2      Jaideep Mankala             10/10/2019

# MAGIC Write CASE_MGT_CNTCT_DTL_LOG Data into a Sequential file for Load Job IdsEdwCaseMgtContactDtlLogLoad.
# MAGIC Read Most recent Data from IDS CASE_MGT_CNTCT_DTL_LOG_D Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsEdwCaseMgtContactDtlLogDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IdsCaseMgtContactRunCycle = get_widget_value('IdsCaseMgtContactRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""SELECT 
CASE_MGT_CNTCT_DTL_LOG_SK,
CASE_MGT_ID,
CNTCT_SEQ_NO,
LOG_SEQ_NO,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_SK,
LAST_UPDT_RUN_CYC_EXCTN_SK,
CASE_MGT_CNTCT_SK,
CASE_MGT_SK,
LOG_USER_SK,
LOG_CNTCT_METH_CD_SK,
LOG_CNTCT_TYP_CD_SK,
LOG_DTM,
LOG_SUM_TX
FROM {IDSOwner}.CASE_MGT_CNTCT_DTL_LOG
WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= '{IdsCaseMgtContactRunCycle}'"""
df_db2_CASE_MGT_CNTCT_DTL_LOG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""Select distinct 
USER_SK,
USER_ID
from {IDSOwner}.APP_USER
WHERE USER_SK NOT IN (0,1)"""
df_APP_USER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = f"""Select 
CASE_MGT_CNTCT_DTL_LOG_SK,
CRT_RUN_CYC_EXCTN_DT_SK 
from {EDWOwner}.CASE_MGT_CNTCT_DTL_LOG_D"""
df_db2_case_mgt_sts_d = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') as TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') as TRGT_CD_NM
from {IDSOwner}.CD_MPPNG"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_meth_cd = df_db2_CD_MPPNG_in.select("CD_MPPNG_SK", "TRGT_CD", "TRGT_CD_NM")
df_meth_nm = df_db2_CD_MPPNG_in.select("CD_MPPNG_SK", "TRGT_CD", "TRGT_CD_NM")
df_type_cd = df_db2_CD_MPPNG_in.select("CD_MPPNG_SK", "TRGT_CD", "TRGT_CD_NM")
df_type_nm = df_db2_CD_MPPNG_in.select("CD_MPPNG_SK", "TRGT_CD", "TRGT_CD_NM")

df_lkp_Codes1 = (
    df_db2_CASE_MGT_CNTCT_DTL_LOG_in.alias("lnk_IdsEdwCaseMgtContactDExtr_InABC")
    .join(
        df_meth_cd.alias("meth_cd"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.LOG_CNTCT_METH_CD_SK")
        == F.col("meth_cd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_meth_nm.alias("meth_nm"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.LOG_CNTCT_METH_CD_SK")
        == F.col("meth_nm.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_type_cd.alias("type_cd"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.LOG_CNTCT_TYP_CD_SK")
        == F.col("type_cd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_type_nm.alias("type_nm"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.LOG_CNTCT_TYP_CD_SK")
        == F.col("type_nm.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_APP_USER.alias("lnk_AppUserData_out"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.LOG_USER_SK")
        == F.col("lnk_AppUserData_out.USER_SK"),
        "left",
    )
    .join(
        df_db2_case_mgt_sts_d.alias("case_mgt_crt_dt"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CASE_MGT_CNTCT_DTL_LOG_SK")
        == F.col("case_mgt_crt_dt.CASE_MGT_CNTCT_DTL_LOG_SK"),
        "left",
    )
    .select(
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CASE_MGT_CNTCT_DTL_LOG_SK"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CASE_MGT_CNTCT_SK"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CASE_MGT_ID"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_SEQ_NO"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.LOG_SEQ_NO"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.SRC_SYS_CD"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CASE_MGT_SK"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.LOG_CNTCT_METH_CD_SK"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.LOG_CNTCT_TYP_CD_SK"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.LOG_DTM"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.LOG_SUM_TX"),
        F.col("meth_cd.TRGT_CD").alias("LOG_CNTCT_METH_CD"),
        F.col("meth_nm.TRGT_CD_NM").alias("LOG_CNTCT_METH_NM"),
        F.col("type_cd.TRGT_CD").alias("LOG_CNTCT_TYP_CD"),
        F.col("type_nm.TRGT_CD_NM").alias("LOG_CNTCT_TYP_NM"),
        F.col("case_mgt_crt_dt.CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("lnk_AppUserData_out.USER_ID").alias("LOG_USER_ID"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.LOG_USER_SK"),
    )
)

df_xfrm_BusinessLogic = (
    df_lkp_Codes1
    .withColumn(
        "CASE_MGT_ID",
        F.when(F.col("CASE_MGT_ID").isNull(), F.lit("NA")).otherwise(F.col("CASE_MGT_ID")),
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        F.when(
            F.col("CRT_RUN_CYC_EXCTN_DT_SK").isNull(), EDWRunCycleDate
        ).otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK")),
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn(
        "CASE_MGT_CNTCT_SK",
        F.when(F.col("CASE_MGT_CNTCT_SK").isNull(), F.lit(0)).otherwise(
            F.col("CASE_MGT_CNTCT_SK")
        ),
    )
    .withColumn(
        "CASE_MGT_SK",
        F.when(F.col("CASE_MGT_SK").isNull(), F.lit(0)).otherwise(F.col("CASE_MGT_SK")),
    )
    .withColumn(
        "LOG_CNTCT_METH_CD",
        F.when(F.col("LOG_CNTCT_METH_CD").isNull(), F.lit("NA")).otherwise(
            F.col("LOG_CNTCT_METH_CD")
        ),
    )
    .withColumn(
        "LOG_CNTCT_METH_NM",
        F.when(F.col("LOG_CNTCT_METH_NM").isNull(), F.lit("NA")).otherwise(
            F.col("LOG_CNTCT_METH_NM")
        ),
    )
    .withColumn(
        "LOG_CNTCT_TYP_CD",
        F.when(F.col("LOG_CNTCT_TYP_CD").isNull(), F.lit("NA")).otherwise(
            F.col("LOG_CNTCT_TYP_CD")
        ),
    )
    .withColumn(
        "LOG_CNTCT_TYP_NM",
        F.when(F.col("LOG_CNTCT_TYP_NM").isNull(), F.lit("NA")).otherwise(
            F.col("LOG_CNTCT_TYP_NM")
        ),
    )
    .withColumn("LOG_DTM", TimestampToDate(F.col("LOG_DTM")))
    .withColumn(
        "LOG_USER_ID",
        F.when(F.col("LOG_USER_ID").isNull(), F.lit("NA")).otherwise(F.col("LOG_USER_ID")),
    )
    .withColumn("LOG_SUM_TX", F.col("LOG_SUM_TX"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn(
        "LOG_CNTCT_METH_CD_SK",
        F.when(F.col("LOG_CNTCT_METH_CD_SK").isNull(), F.lit(0)).otherwise(
            F.col("LOG_CNTCT_METH_CD_SK")
        ),
    )
    .withColumn(
        "LOG_CNTCT_TYP_CD_SK",
        F.when(F.col("LOG_CNTCT_TYP_CD_SK").isNull(), F.lit(0)).otherwise(
            F.col("LOG_CNTCT_TYP_CD_SK")
        ),
    )
)

df_final = (
    df_xfrm_BusinessLogic
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    )
    .withColumn("LOG_USER_ID", rpad(F.col("LOG_USER_ID"), 10, " "))
    .select(
        "CASE_MGT_CNTCT_DTL_LOG_SK",
        "CASE_MGT_ID",
        "CNTCT_SEQ_NO",
        "LOG_SEQ_NO",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CASE_MGT_CNTCT_SK",
        "CASE_MGT_SK",
        "LOG_USER_SK",
        "LOG_CNTCT_METH_CD",
        "LOG_CNTCT_METH_NM",
        "LOG_CNTCT_TYP_CD",
        "LOG_CNTCT_TYP_NM",
        "LOG_DTM",
        "LOG_USER_ID",
        "LOG_SUM_TX",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "LOG_CNTCT_METH_CD_SK",
        "LOG_CNTCT_TYP_CD_SK",
    )
)

write_files(
    df_final,
    f"{adls_path}/load/CASE_MGT_CNTCT_DTL_LOG.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="^",
    nullValue=None
)