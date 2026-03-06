# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2016 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsEdwCaseMgtContacDExtr
# MAGIC 
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                This Job extracts Data from IDS and creates a Load file for EDW Table CASE_MGT_CNTCT_D.         
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                       ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Akash Parsha                  10/07/2019      140165                        Original Program to Extract CM Contact data from IDS                   EnterpriseDev2        Jaideep Mankala         10/10/2019

# MAGIC Write CASE_MGT_CNTCT Data into a Sequential file for Load Job IdsEdwCaseMgtContactLoad.
# MAGIC Read Most recent Data from IDS CASE_MGT_CNTCT Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsEdwCaseMgtContactDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
EDWOwner = get_widget_value('EDWOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

# Stage: db2_CASE_MGT_CNTCT_in
extract_query_db2_CASE_MGT_CNTCT_in = f"""select distinct
CASE_MGT_CNTCT_SK,
CASE_MGT_ID,
CNTCT_SEQ_NO,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_SK,
LAST_UPDT_RUN_CYC_EXCTN_SK,
CASE_MGT_SK,
CNTCT_LANG_CD_SK,
CNTCT_FIRST_NM,
CNTCT_LAST_NM,
CNTCT_MIDINIT,
CNTCT_TTL_NM,
CNTCT_ADDR_LN_1,
CNTCT_ADDR_LN_2,
CNTCT_ADDR_LN_3,
CNTCT_CITY_NM,
CNTCT_ST_CD_SK,
CNTCT_ZIP_CD,
CNTCT_CTRY_CD_SK,
CNTCT_CNTY_NM,
CNTCT_FAX_NO,
CNTCT_FAX_EXT_NO,
CNTCT_PHN_NO,
CNTCT_PHN_EXT_NO,
CNTCT_EMAIL_ADDR
from {IDSOwner}.CASE_MGT_CNTCT
"""
df_db2_CASE_MGT_CNTCT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CASE_MGT_CNTCT_in)
    .load()
)

# Stage: db2_case_mgt_sts_d
extract_query_db2_case_mgt_sts_d = f"""Select
CASE_MGT_CNTCT_SK,
CRT_RUN_CYC_EXCTN_DT_SK
from {EDWOwner}.CASE_MGT_CNTCT_D
"""
df_db2_case_mgt_sts_d = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_case_mgt_sts_d)
    .load()
)

# Stage: db2_CD_MPPNG_in
extract_query_db2_CD_MPPNG_in = f"""SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
from {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

# Stage: COPY (PxCopy)
df_COPY = df_db2_CD_MPPNG_in
df_st_cd = df_COPY.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_st_nm = df_COPY.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cntry_cd = df_COPY.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cntry_nm = df_COPY.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Lang_cd = df_COPY.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Lang_nm = df_COPY.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# Stage: lkp_Codes1 (PxLookup)
df_lkp_Codes1 = (
    df_db2_CASE_MGT_CNTCT_in.alias("lnk_IdsEdwCaseMgtContactDExtr_InABC")
    .join(
        df_st_cd.alias("st_cd"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_ST_CD_SK") 
        == F.col("st_cd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_st_nm.alias("st_nm"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_ST_CD_SK")
        == F.col("st_nm.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cntry_cd.alias("cntry_cd"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_CTRY_CD_SK")
        == F.col("cntry_cd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cntry_nm.alias("cntry_nm"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_CTRY_CD_SK")
        == F.col("cntry_nm.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_case_mgt_sts_d.alias("case_mgt_crt_dt"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CASE_MGT_CNTCT_SK")
        == F.col("case_mgt_crt_dt.CASE_MGT_CNTCT_SK"),
        "left"
    )
    .join(
        df_Lang_cd.alias("Lang_cd"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_LANG_CD_SK")
        == F.col("Lang_cd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Lang_nm.alias("Lang_nm"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_LANG_CD_SK")
        == F.col("Lang_nm.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CASE_MGT_CNTCT_SK").alias("CASE_MGT_CNTCT_SK"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CASE_MGT_ID").alias("CASE_MGT_ID"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_SEQ_NO").alias("CNTCT_SEQ_NO"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CASE_MGT_SK").alias("CASE_MGT_SK"),
        F.col("Lang_cd.TRGT_CD").alias("CNTCT_LANG_CD"),
        F.col("Lang_nm.TRGT_CD_NM").alias("CNTCT_LANG_NM"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_FIRST_NM").alias("CNTCT_FIRST_NM"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_LAST_NM").alias("CNTCT_LAST_NM"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_MIDINIT").alias("CNTCT_MIDINIT"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_TTL_NM").alias("CNTCT_TTL_NM"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_ADDR_LN_1").alias("CNTCT_ADDR_LN_1"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_ADDR_LN_2").alias("CNTCT_ADDR_LN_2"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_ADDR_LN_3").alias("CNTCT_ADDR_LN_3"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_CITY_NM").alias("CNTCT_CITY_NM"),
        F.col("st_cd.TRGT_CD").alias("CNTCT_ST_CD"),
        F.col("st_nm.TRGT_CD_NM").alias("CNTCT_ST_NM"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_ZIP_CD").alias("CNTCT_ZIP_CD"),
        F.col("cntry_cd.TRGT_CD").alias("CNTCT_CTRY_CD"),
        F.col("cntry_nm.TRGT_CD_NM").alias("CNTCT_CTRY_NM"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_FAX_NO").alias("CNTCT_FAX_NO"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_FAX_EXT_NO").alias("CNTCT_FAX_EXT_NO"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_CNTY_NM").alias("CNTCT_CNTY_NM"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_PHN_NO").alias("CNTCT_PHN_NO"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_PHN_EXT_NO").alias("CNTCT_PHN_EXT_NO"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_EMAIL_ADDR").alias("CNTCT_EMAIL_ADDR"),
        F.col("case_mgt_crt_dt.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_CTRY_CD_SK").alias("CNTCT_CTRY_CD_SK"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_LANG_CD_SK").alias("CNTCT_LANG_CD_SK"),
        F.col("lnk_IdsEdwCaseMgtContactDExtr_InABC.CNTCT_ST_CD_SK").alias("CNTCT_ST_CD_SK")
    )
)

# Stage: xfrm_BusinessLogic (CTransformerStage)
df_xfrm_BusinessLogic = (
    df_lkp_Codes1
    .withColumn(
        "CNTCT_LANG_NM",
        F.when(F.col("CNTCT_LANG_NM").isNull(), F.lit("")).otherwise(F.col("CNTCT_LANG_NM"))
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        F.when(F.col("CRT_RUN_CYC_EXCTN_DT_SK").isNull(), F.lit(EDWRunCycleDate))
        .otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK"))
    )
    .withColumn(
        "CNTCT_CTRY_CD_SK",
        F.when(F.col("CNTCT_CTRY_CD_SK").isNull(), F.lit(0))
        .otherwise(F.col("CNTCT_CTRY_CD_SK"))
    )
    .withColumn(
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        F.lit(EDWRunCycleDate)
    )
)

# Final select to maintain column order and apply rpad for char columns
df_final = df_xfrm_BusinessLogic.select(
    F.col("CASE_MGT_CNTCT_SK"),
    F.col("CASE_MGT_ID"),
    F.col("CNTCT_SEQ_NO"),
    F.col("SRC_SYS_CD"),
    F.col("CASE_MGT_SK"),
    F.col("CNTCT_LANG_CD"),
    F.col("CNTCT_LANG_NM"),
    F.col("CNTCT_FIRST_NM"),
    F.col("CNTCT_LAST_NM"),
    F.rpad(F.col("CNTCT_MIDINIT"), 1, " ").alias("CNTCT_MIDINIT"),
    F.col("CNTCT_TTL_NM"),
    F.col("CNTCT_ADDR_LN_1"),
    F.col("CNTCT_ADDR_LN_2"),
    F.col("CNTCT_ADDR_LN_3"),
    F.col("CNTCT_CITY_NM"),
    F.col("CNTCT_ST_CD"),
    F.col("CNTCT_ST_NM"),
    F.col("CNTCT_ZIP_CD"),
    F.col("CNTCT_CTRY_CD"),
    F.col("CNTCT_CTRY_NM"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CNTCT_FAX_NO"),
    F.col("CNTCT_FAX_EXT_NO"),
    F.col("CNTCT_CNTY_NM"),
    F.col("CNTCT_PHN_NO"),
    F.col("CNTCT_PHN_EXT_NO"),
    F.col("CNTCT_EMAIL_ADDR"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CNTCT_CTRY_CD_SK"),
    F.col("CNTCT_LANG_CD_SK"),
    F.col("CNTCT_ST_CD_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK")
)

# Stage: seq_CASE_MGT_CNTCT_D_csv_load (PxSequentialFile)
write_files(
    df_final,
    f"{adls_path}/load/CASE_MGT_CNTCT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="^",
    nullValue=None
)