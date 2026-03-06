# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Rama Kamjula       09/20/2013          5114                             Original Programming                                                                             IntegrateWrhsDevl     Peter Marshall              10/23/2013  
# MAGIC Kalyan Neelam         2013-11-27          5234 Compass             Added new columns CMPSS_COV_IN, ALNO_HOME_PGM_ID         IntegrateNewDevl      Bhoomi Dasari              11/28/2013 
# MAGIC Karthik Chintalapani  2013-01-21   5108                                   Added new column  PCMH_IN                                                             IntegrateNewDevl       Kalyan Neelam              2014-03-31
# MAGIC                                                                                                  as per the new mapping rules. 
# MAGIC 
# MAGIC Michael Harmon       2016-08-10         5541                            Added EMAIL_ADDR lookup from MBR_COMM to populate                   Integrate\\Dev1        Jag Yelavarthi                2016-08-16
# MAGIC                                                                                                new field MBR_PRFRD_EMAIL_ADDR_TX
# MAGIC Abhiram Dasarathy	2016-12-06         5217 Dental Network	Added DNTL_NTWRK_IN field to the end of the columns		IntegrateDev2              Kalyan Neelam          2016-12-06

# MAGIC Job Name: IdsDmMbrshDmMbrLoad
# MAGIC Read Load File created in the IdsDmMbrshDmMbrExtr Job
# MAGIC Null handling for Timestamp Fields - While loading into SQL Server table The Null Values are not properly handle in seq file if it is timestamp field. so Used transformer stage to handle NULLS for Timestamp fields.
# MAGIC Update and Insert the MBRSH_DM_MBR  Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
from pyspark.sql.functions import col, when, to_timestamp, lit, rpad, trim
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# =====================================================================
# Retrieve parameter values
# =====================================================================
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

# =====================================================================
# Stage: seq_MBRSH_DM_MBR (PxSequentialFile) - READ
# =====================================================================
schema_seq_MBRSH_DM_MBR = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("MBR_FIRST_NM", StringType(), True),
    StructField("MBR_MIDINIT", StringType(), True),
    StructField("MBR_LAST_NM", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("GRP_UNIQ_KEY", IntegerType(), True),
    StructField("MBR_BRTH_DT", StringType(), True),
    StructField("CLS_ID", StringType(), True),
    StructField("CLS_DESC", StringType(), True),
    StructField("GRP_NM", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("SUBGRP_NM", StringType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("MBR_GNDR_CD", StringType(), True),
    StructField("MBR_PBM_VNDR_CD", StringType(), True),
    StructField("SUB_ALPHA_PFX_CD", StringType(), True),
    StructField("MBR_RELSHP_CD", StringType(), True),
    StructField("MBR_ACTV_PROD_IN", StringType(), True),
    StructField("MBR_CONF_COMM_IN", StringType(), True),
    StructField("MBR_DP_MCARE_IN", StringType(), True),
    StructField("SUB_IN", StringType(), True),
    StructField("SUB_GRP_25_IN", StringType(), True),
    StructField("MBR_MCARE_NO", StringType(), True),
    StructField("MBR_HOME_ADDR_POSTAL_CD", StringType(), True),
    StructField("MBR_SSN", StringType(), True),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("MBR_HOME_ADDR_LN_1", StringType(), True),
    StructField("MBR_HOME_ADDR_LN_2", StringType(), True),
    StructField("MBR_HOME_ADDR_LN_3", StringType(), True),
    StructField("MBR_HOME_ADDR_CITY_NM", StringType(), True),
    StructField("MBR_HOME_ADDR_ST_CD", StringType(), True),
    StructField("MBR_HOME_ADDR_ST_NM", StringType(), True),
    StructField("MBR_HOME_ADDR_ZIP_CD_5", StringType(), True),
    StructField("MBR_HOME_ADDR_ZIP_CD_4", StringType(), True),
    StructField("MBR_HOME_ADDR_CNTY_NM", StringType(), True),
    StructField("MBR_HOME_ADDR_EMAIL_ADDR_TX", StringType(), True),
    StructField("MBR_HOME_ADDR_FAX_NO", StringType(), True),
    StructField("MBR_HOME_ADDR_FAX_NO_EXT", StringType(), True),
    StructField("MBR_HOME_ADDR_PHN_NO", StringType(), True),
    StructField("MBR_HOME_ADDR_PHN_NO_EXT", StringType(), True),
    StructField("MBR_MAIL_ADDR_LN_1", StringType(), True),
    StructField("MBR_MAIL_ADDR_LN_2", StringType(), True),
    StructField("MBR_MAIL_ADDR_LN_3", StringType(), True),
    StructField("MBR_MAIL_ADDR_CITY_NM", StringType(), True),
    StructField("MBR_MAIL_ADDR_ST_CD", StringType(), True),
    StructField("MBR_MAIL_ADDR_ST_NM", StringType(), True),
    StructField("MBR_MAIL_ADDR_POSTAL_CD", StringType(), True),
    StructField("MBR_MAIL_ADDR_ZIP_CD_5", StringType(), True),
    StructField("MBR_MAIL_ADDR_ZIP_CD_4", StringType(), True),
    StructField("MBR_MAIL_ADDR_CNTY_NM", StringType(), True),
    StructField("MBR_MAIL_ADDR_EMAIL_ADDR_TX", StringType(), True),
    StructField("MBR_MAIL_ADDR_FAX_NO", StringType(), True),
    StructField("MBR_MAIL_ADDR_FAX_NO_EXT", StringType(), True),
    StructField("MBR_MAIL_ADDR_PHN_NO", StringType(), True),
    StructField("MBR_MAIL_ADDR_PHN_NO_EXT", StringType(), True),
    StructField("MBR_MAIL_ADDR_CONF_COMM_IN", StringType(), True),
    StructField("SUB_FIRST_NM", StringType(), True),
    StructField("SUB_MIDINIT", StringType(), True),
    StructField("SUB_LAST_NM", StringType(), True),
    StructField("SUB_SSN", StringType(), True),
    StructField("MBR_HOME_ADDR_CNTGS_CNTY_CD", StringType(), True),
    StructField("MBR_HOME_ADDR_CNTGS_CNTY_NM", StringType(), True),
    StructField("MBR_RELSHP_SRC_CD", StringType(), True),
    StructField("MBR_RELSHP_SRC_NM", StringType(), True),
    StructField("MBR_WELNS_BNF_LVL_CD", StringType(), True),
    StructField("MBR_WELNS_BNF_LVL_NM", StringType(), True),
    StructField("MBR_DCSD_IN", StringType(), True),
    StructField("MBR_HOME_ADDR_IN_AREA_IN", StringType(), True),
    StructField("MBR_DCSD_DT", StringType(), True),
    StructField("MBR_UNIQ_KEY_ORIG_EFF_DT", StringType(), True),
    StructField("MBR_MNTL_HLTH_VNDR_ID", StringType(), True),
    StructField("MBR_WORK_PHN_NO", StringType(), True),
    StructField("MBR_WORK_PHN_NO_EXT", StringType(), True),
    StructField("CLNT_ID", StringType(), True),
    StructField("CLNT_NM", StringType(), True),
    StructField("MBR_WELNS_BNF_VNDR_ID", StringType(), True),
    StructField("MBR_EAP_CAT_CD", StringType(), True),
    StructField("MBR_EAP_CAT_DESC", StringType(), True),
    StructField("MBR_VSN_BNF_VNDR_ID", StringType(), True),
    StructField("MBR_VSN_RTN_EXAM_IN", StringType(), True),
    StructField("MBR_VSN_HRDWR_IN", StringType(), True),
    StructField("MBR_VSN_OUT_OF_NTWK_EXAM_IN", StringType(), True),
    StructField("MBR_OTHR_CAR_PRI_MED_IN", StringType(), True),
    StructField("MBR_PT_POD_DPLY_IN", StringType(), True),
    StructField("MBR_CELL_PHN_NO", StringType(), True),
    StructField("FEP_LOCAL_CNTR_IN", StringType(), True),
    StructField("MBR_PT_EFF_DT", StringType(), True),
    StructField("MBR_PT_TERM_DT", StringType(), True),
    StructField("VBB_ENR_IN", StringType(), True),
    StructField("VBB_MDL_CD", StringType(), True),
    StructField("CMPSS_COV_IN", StringType(), True),
    StructField("ALNO_HOME_PG_ID", StringType(), True),
    StructField("PCMH_IN", StringType(), True),
    StructField("MBR_PRFRD_EMAIL_ADDR_TX", StringType(), True),
    StructField("DNTL_RWRD_IN", StringType(), True)
])

df_seq_MBRSH_DM_MBR = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("escape", "^")
    .schema(schema_seq_MBRSH_DM_MBR)
    .csv(f"{adls_path}/load/MBRSH_DM_MBR.dat")
)

# =====================================================================
# Stage: xfm_NullHandling (CTransformerStage)
# =====================================================================
df_xfm_NullHandling = df_seq_MBRSH_DM_MBR

# MBR_BRTH_DT: If IsNull(...) OR Trim(...)='' Then Null Else to_timestamp(...)
df_xfm_NullHandling = df_xfm_NullHandling.withColumn(
    "MBR_BRTH_DT",
    when(
        col("MBR_BRTH_DT").isNull() | (trim(col("MBR_BRTH_DT")) == ''),
        lit(None)
    ).otherwise(to_timestamp(col("MBR_BRTH_DT"), "yyyy-MM-dd"))
)

# MBR_DCSD_DT: If Trim(...)='' Then Null Else to_timestamp(...)
df_xfm_NullHandling = df_xfm_NullHandling.withColumn(
    "MBR_DCSD_DT",
    when(
        trim(col("MBR_DCSD_DT")) == '',
        lit(None)
    ).otherwise(to_timestamp(col("MBR_DCSD_DT"), "yyyy-MM-dd"))
)

# MBR_UNIQ_KEY_ORIG_EFF_DT: If Trim(...)='' Then Null Else to_timestamp(...)
df_xfm_NullHandling = df_xfm_NullHandling.withColumn(
    "MBR_UNIQ_KEY_ORIG_EFF_DT",
    when(
        trim(col("MBR_UNIQ_KEY_ORIG_EFF_DT")) == '',
        lit(None)
    ).otherwise(to_timestamp(col("MBR_UNIQ_KEY_ORIG_EFF_DT"), "yyyy-MM-dd"))
)

# MBR_PT_EFF_DT: If Trim(...)='' Then Null Else to_timestamp(...)
df_xfm_NullHandling = df_xfm_NullHandling.withColumn(
    "MBR_PT_EFF_DT",
    when(
        trim(col("MBR_PT_EFF_DT")) == '',
        lit(None)
    ).otherwise(to_timestamp(col("MBR_PT_EFF_DT"), "yyyy-MM-dd"))
)

# MBR_PT_TERM_DT: If Trim(...)='' Then Null Else to_timestamp(...)
df_xfm_NullHandling = df_xfm_NullHandling.withColumn(
    "MBR_PT_TERM_DT",
    when(
        trim(col("MBR_PT_TERM_DT")) == '',
        lit(None)
    ).otherwise(to_timestamp(col("MBR_PT_TERM_DT"), "yyyy-MM-dd"))
)

# Select and preserve column order exactly as output pin
df_xfm_NullHandling = df_xfm_NullHandling.select(
    "SRC_SYS_CD",
    "MBR_UNIQ_KEY",
    "SUB_UNIQ_KEY",
    "MBR_FIRST_NM",
    "MBR_MIDINIT",
    "MBR_LAST_NM",
    "LAST_UPDT_RUN_CYC_NO",
    "GRP_ID",
    "GRP_UNIQ_KEY",
    "MBR_BRTH_DT",
    "CLS_ID",
    "CLS_DESC",
    "GRP_NM",
    "SUBGRP_ID",
    "SUBGRP_NM",
    "SUB_ID",
    "MBR_GNDR_CD",
    "MBR_PBM_VNDR_CD",
    "SUB_ALPHA_PFX_CD",
    "MBR_RELSHP_CD",
    "MBR_ACTV_PROD_IN",
    "MBR_CONF_COMM_IN",
    "MBR_DP_MCARE_IN",
    "SUB_IN",
    "SUB_GRP_25_IN",
    "MBR_MCARE_NO",
    "MBR_HOME_ADDR_POSTAL_CD",
    "MBR_SSN",
    "MBR_SFX_NO",
    "MBR_HOME_ADDR_LN_1",
    "MBR_HOME_ADDR_LN_2",
    "MBR_HOME_ADDR_LN_3",
    "MBR_HOME_ADDR_CITY_NM",
    "MBR_HOME_ADDR_ST_CD",
    "MBR_HOME_ADDR_ST_NM",
    "MBR_HOME_ADDR_ZIP_CD_5",
    "MBR_HOME_ADDR_ZIP_CD_4",
    "MBR_HOME_ADDR_CNTY_NM",
    "MBR_HOME_ADDR_EMAIL_ADDR_TX",
    "MBR_HOME_ADDR_FAX_NO",
    "MBR_HOME_ADDR_FAX_NO_EXT",
    "MBR_HOME_ADDR_PHN_NO",
    "MBR_HOME_ADDR_PHN_NO_EXT",
    "MBR_MAIL_ADDR_LN_1",
    "MBR_MAIL_ADDR_LN_2",
    "MBR_MAIL_ADDR_LN_3",
    "MBR_MAIL_ADDR_CITY_NM",
    "MBR_MAIL_ADDR_ST_CD",
    "MBR_MAIL_ADDR_ST_NM",
    "MBR_MAIL_ADDR_POSTAL_CD",
    "MBR_MAIL_ADDR_ZIP_CD_5",
    "MBR_MAIL_ADDR_ZIP_CD_4",
    "MBR_MAIL_ADDR_CNTY_NM",
    "MBR_MAIL_ADDR_EMAIL_ADDR_TX",
    "MBR_MAIL_ADDR_FAX_NO",
    "MBR_MAIL_ADDR_FAX_NO_EXT",
    "MBR_MAIL_ADDR_PHN_NO",
    "MBR_MAIL_ADDR_PHN_NO_EXT",
    "MBR_MAIL_ADDR_CONF_COMM_IN",
    "SUB_FIRST_NM",
    "SUB_MIDINIT",
    "SUB_LAST_NM",
    "SUB_SSN",
    "MBR_HOME_ADDR_CNTGS_CNTY_CD",
    "MBR_HOME_ADDR_CNTGS_CNTY_NM",
    "MBR_RELSHP_SRC_CD",
    "MBR_RELSHP_SRC_NM",
    "MBR_WELNS_BNF_LVL_CD",
    "MBR_WELNS_BNF_LVL_NM",
    "MBR_DCSD_IN",
    "MBR_HOME_ADDR_IN_AREA_IN",
    "MBR_DCSD_DT",
    "MBR_UNIQ_KEY_ORIG_EFF_DT",
    "MBR_MNTL_HLTH_VNDR_ID",
    "MBR_WORK_PHN_NO",
    "MBR_WORK_PHN_NO_EXT",
    "CLNT_ID",
    "CLNT_NM",
    "MBR_WELNS_BNF_VNDR_ID",
    "MBR_EAP_CAT_CD",
    "MBR_EAP_CAT_DESC",
    "MBR_VSN_BNF_VNDR_ID",
    "MBR_VSN_RTN_EXAM_IN",
    "MBR_VSN_HRDWR_IN",
    "MBR_VSN_OUT_OF_NTWK_EXAM_IN",
    "MBR_OTHR_CAR_PRI_MED_IN",
    "MBR_PT_POD_DPLY_IN",
    "MBR_CELL_PHN_NO",
    "FEP_LOCAL_CNTR_IN",
    "MBR_PT_EFF_DT",
    "MBR_PT_TERM_DT",
    "VBB_ENR_IN",
    "VBB_MDL_CD",
    "CMPSS_COV_IN",
    "ALNO_HOME_PG_ID",
    "PCMH_IN",
    "MBR_PRFRD_EMAIL_ADDR_TX",
    "DNTL_RWRD_IN"
)

# =====================================================================
# Stage: odbc_MBRSH_DM_MBR (ODBCConnectorPX) - MERGE (upsert)
# =====================================================================
jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmMbrLoad_odbc_MBRSH_DM_MBR_temp",
    jdbc_url,
    jdbc_props
)

df_xfm_NullHandling.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmMbrshDmMbrLoad_odbc_MBRSH_DM_MBR_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MBRSH_DM_MBR AS T
USING STAGING.IdsDmMbrshDmMbrLoad_odbc_MBRSH_DM_MBR_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
WHEN MATCHED THEN UPDATE SET
 T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY,
 T.MBR_FIRST_NM = S.MBR_FIRST_NM,
 T.MBR_MIDINIT = S.MBR_MIDINIT,
 T.MBR_LAST_NM = S.MBR_LAST_NM,
 T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
 T.GRP_ID = S.GRP_ID,
 T.GRP_UNIQ_KEY = S.GRP_UNIQ_KEY,
 T.MBR_BRTH_DT = S.MBR_BRTH_DT,
 T.CLS_ID = S.CLS_ID,
 T.CLS_DESC = S.CLS_DESC,
 T.GRP_NM = S.GRP_NM,
 T.SUBGRP_ID = S.SUBGRP_ID,
 T.SUBGRP_NM = S.SUBGRP_NM,
 T.SUB_ID = S.SUB_ID,
 T.MBR_GNDR_CD = S.MBR_GNDR_CD,
 T.MBR_PBM_VNDR_CD = S.MBR_PBM_VNDR_CD,
 T.SUB_ALPHA_PFX_CD = S.SUB_ALPHA_PFX_CD,
 T.MBR_RELSHP_CD = S.MBR_RELSHP_CD,
 T.MBR_ACTV_PROD_IN = S.MBR_ACTV_PROD_IN,
 T.MBR_CONF_COMM_IN = S.MBR_CONF_COMM_IN,
 T.MBR_DP_MCARE_IN = S.MBR_DP_MCARE_IN,
 T.SUB_IN = S.SUB_IN,
 T.SUB_GRP_25_IN = S.SUB_GRP_25_IN,
 T.MBR_MCARE_NO = S.MBR_MCARE_NO,
 T.MBR_HOME_ADDR_POSTAL_CD = S.MBR_HOME_ADDR_POSTAL_CD,
 T.MBR_SSN = S.MBR_SSN,
 T.MBR_SFX_NO = S.MBR_SFX_NO,
 T.MBR_HOME_ADDR_LN_1 = S.MBR_HOME_ADDR_LN_1,
 T.MBR_HOME_ADDR_LN_2 = S.MBR_HOME_ADDR_LN_2,
 T.MBR_HOME_ADDR_LN_3 = S.MBR_HOME_ADDR_LN_3,
 T.MBR_HOME_ADDR_CITY_NM = S.MBR_HOME_ADDR_CITY_NM,
 T.MBR_HOME_ADDR_ST_CD = S.MBR_HOME_ADDR_ST_CD,
 T.MBR_HOME_ADDR_ST_NM = S.MBR_HOME_ADDR_ST_NM,
 T.MBR_HOME_ADDR_ZIP_CD_5 = S.MBR_HOME_ADDR_ZIP_CD_5,
 T.MBR_HOME_ADDR_ZIP_CD_4 = S.MBR_HOME_ADDR_ZIP_CD_4,
 T.MBR_HOME_ADDR_CNTY_NM = S.MBR_HOME_ADDR_CNTY_NM,
 T.MBR_HOME_ADDR_EMAIL_ADDR_TX = S.MBR_HOME_ADDR_EMAIL_ADDR_TX,
 T.MBR_HOME_ADDR_FAX_NO = S.MBR_HOME_ADDR_FAX_NO,
 T.MBR_HOME_ADDR_FAX_NO_EXT = S.MBR_HOME_ADDR_FAX_NO_EXT,
 T.MBR_HOME_ADDR_PHN_NO = S.MBR_HOME_ADDR_PHN_NO,
 T.MBR_HOME_ADDR_PHN_NO_EXT = S.MBR_HOME_ADDR_PHN_NO_EXT,
 T.MBR_MAIL_ADDR_LN_1 = S.MBR_MAIL_ADDR_LN_1,
 T.MBR_MAIL_ADDR_LN_2 = S.MBR_MAIL_ADDR_LN_2,
 T.MBR_MAIL_ADDR_LN_3 = S.MBR_MAIL_ADDR_LN_3,
 T.MBR_MAIL_ADDR_CITY_NM = S.MBR_MAIL_ADDR_CITY_NM,
 T.MBR_MAIL_ADDR_ST_CD = S.MBR_MAIL_ADDR_ST_CD,
 T.MBR_MAIL_ADDR_ST_NM = S.MBR_MAIL_ADDR_ST_NM,
 T.MBR_MAIL_ADDR_POSTAL_CD = S.MBR_MAIL_ADDR_POSTAL_CD,
 T.MBR_MAIL_ADDR_ZIP_CD_5 = S.MBR_MAIL_ADDR_ZIP_CD_5,
 T.MBR_MAIL_ADDR_ZIP_CD_4 = S.MBR_MAIL_ADDR_ZIP_CD_4,
 T.MBR_MAIL_ADDR_CNTY_NM = S.MBR_MAIL_ADDR_CNTY_NM,
 T.MBR_MAIL_ADDR_EMAIL_ADDR_TX = S.MBR_MAIL_ADDR_EMAIL_ADDR_TX,
 T.MBR_MAIL_ADDR_FAX_NO = S.MBR_MAIL_ADDR_FAX_NO,
 T.MBR_MAIL_ADDR_FAX_NO_EXT = S.MBR_MAIL_ADDR_FAX_NO_EXT,
 T.MBR_MAIL_ADDR_PHN_NO = S.MBR_MAIL_ADDR_PHN_NO,
 T.MBR_MAIL_ADDR_PHN_NO_EXT = S.MBR_MAIL_ADDR_PHN_NO_EXT,
 T.MBR_MAIL_ADDR_CONF_COMM_IN = S.MBR_MAIL_ADDR_CONF_COMM_IN,
 T.SUB_FIRST_NM = S.SUB_FIRST_NM,
 T.SUB_MIDINIT = S.SUB_MIDINIT,
 T.SUB_LAST_NM = S.SUB_LAST_NM,
 T.SUB_SSN = S.SUB_SSN,
 T.MBR_HOME_ADDR_CNTGS_CNTY_CD = S.MBR_HOME_ADDR_CNTGS_CNTY_CD,
 T.MBR_HOME_ADDR_CNTGS_CNTY_NM = S.MBR_HOME_ADDR_CNTGS_CNTY_NM,
 T.MBR_RELSHP_SRC_CD = S.MBR_RELSHP_SRC_CD,
 T.MBR_RELSHP_SRC_NM = S.MBR_RELSHP_SRC_NM,
 T.MBR_WELNS_BNF_LVL_CD = S.MBR_WELNS_BNF_LVL_CD,
 T.MBR_WELNS_BNF_LVL_NM = S.MBR_WELNS_BNF_LVL_NM,
 T.MBR_DCSD_IN = S.MBR_DCSD_IN,
 T.MBR_HOME_ADDR_IN_AREA_IN = S.MBR_HOME_ADDR_IN_AREA_IN,
 T.MBR_DCSD_DT = S.MBR_DCSD_DT,
 T.MBR_UNIQ_KEY_ORIG_EFF_DT = S.MBR_UNIQ_KEY_ORIG_EFF_DT,
 T.MBR_MNTL_HLTH_VNDR_ID = S.MBR_MNTL_HLTH_VNDR_ID,
 T.MBR_WORK_PHN_NO = S.MBR_WORK_PHN_NO,
 T.MBR_WORK_PHN_NO_EXT = S.MBR_WORK_PHN_NO_EXT,
 T.CLNT_ID = S.CLNT_ID,
 T.CLNT_NM = S.CLNT_NM,
 T.MBR_WELNS_BNF_VNDR_ID = S.MBR_WELNS_BNF_VNDR_ID,
 T.MBR_EAP_CAT_CD = S.MBR_EAP_CAT_CD,
 T.MBR_EAP_CAT_DESC = S.MBR_EAP_CAT_DESC,
 T.MBR_VSN_BNF_VNDR_ID = S.MBR_VSN_BNF_VNDR_ID,
 T.MBR_VSN_RTN_EXAM_IN = S.MBR_VSN_RTN_EXAM_IN,
 T.MBR_VSN_HRDWR_IN = S.MBR_VSN_HRDWR_IN,
 T.MBR_VSN_OUT_OF_NTWK_EXAM_IN = S.MBR_VSN_OUT_OF_NTWK_EXAM_IN,
 T.MBR_OTHR_CAR_PRI_MED_IN = S.MBR_OTHR_CAR_PRI_MED_IN,
 T.MBR_PT_POD_DPLY_IN = S.MBR_PT_POD_DPLY_IN,
 T.MBR_CELL_PHN_NO = S.MBR_CELL_PHN_NO,
 T.FEP_LOCAL_CNTR_IN = S.FEP_LOCAL_CNTR_IN,
 T.MBR_PT_EFF_DT = S.MBR_PT_EFF_DT,
 T.MBR_PT_TERM_DT = S.MBR_PT_TERM_DT,
 T.VBB_ENR_IN = S.VBB_ENR_IN,
 T.VBB_MDL_CD = S.VBB_MDL_CD,
 T.CMPSS_COV_IN = S.CMPSS_COV_IN,
 T.ALNO_HOME_PG_ID = S.ALNO_HOME_PG_ID,
 T.PCMH_IN = S.PCMH_IN,
 T.MBR_PRFRD_EMAIL_ADDR_TX = S.MBR_PRFRD_EMAIL_ADDR_TX,
 T.DNTL_RWRD_IN = S.DNTL_RWRD_IN
WHEN NOT MATCHED THEN INSERT
(
 SRC_SYS_CD,
 MBR_UNIQ_KEY,
 SUB_UNIQ_KEY,
 MBR_FIRST_NM,
 MBR_MIDINIT,
 MBR_LAST_NM,
 LAST_UPDT_RUN_CYC_NO,
 GRP_ID,
 GRP_UNIQ_KEY,
 MBR_BRTH_DT,
 CLS_ID,
 CLS_DESC,
 GRP_NM,
 SUBGRP_ID,
 SUBGRP_NM,
 SUB_ID,
 MBR_GNDR_CD,
 MBR_PBM_VNDR_CD,
 SUB_ALPHA_PFX_CD,
 MBR_RELSHP_CD,
 MBR_ACTV_PROD_IN,
 MBR_CONF_COMM_IN,
 MBR_DP_MCARE_IN,
 SUB_IN,
 SUB_GRP_25_IN,
 MBR_MCARE_NO,
 MBR_HOME_ADDR_POSTAL_CD,
 MBR_SSN,
 MBR_SFX_NO,
 MBR_HOME_ADDR_LN_1,
 MBR_HOME_ADDR_LN_2,
 MBR_HOME_ADDR_LN_3,
 MBR_HOME_ADDR_CITY_NM,
 MBR_HOME_ADDR_ST_CD,
 MBR_HOME_ADDR_ST_NM,
 MBR_HOME_ADDR_ZIP_CD_5,
 MBR_HOME_ADDR_ZIP_CD_4,
 MBR_HOME_ADDR_CNTY_NM,
 MBR_HOME_ADDR_EMAIL_ADDR_TX,
 MBR_HOME_ADDR_FAX_NO,
 MBR_HOME_ADDR_FAX_NO_EXT,
 MBR_HOME_ADDR_PHN_NO,
 MBR_HOME_ADDR_PHN_NO_EXT,
 MBR_MAIL_ADDR_LN_1,
 MBR_MAIL_ADDR_LN_2,
 MBR_MAIL_ADDR_LN_3,
 MBR_MAIL_ADDR_CITY_NM,
 MBR_MAIL_ADDR_ST_CD,
 MBR_MAIL_ADDR_ST_NM,
 MBR_MAIL_ADDR_POSTAL_CD,
 MBR_MAIL_ADDR_ZIP_CD_5,
 MBR_MAIL_ADDR_ZIP_CD_4,
 MBR_MAIL_ADDR_CNTY_NM,
 MBR_MAIL_ADDR_EMAIL_ADDR_TX,
 MBR_MAIL_ADDR_FAX_NO,
 MBR_MAIL_ADDR_FAX_NO_EXT,
 MBR_MAIL_ADDR_PHN_NO,
 MBR_MAIL_ADDR_PHN_NO_EXT,
 MBR_MAIL_ADDR_CONF_COMM_IN,
 SUB_FIRST_NM,
 SUB_MIDINIT,
 SUB_LAST_NM,
 SUB_SSN,
 MBR_HOME_ADDR_CNTGS_CNTY_CD,
 MBR_HOME_ADDR_CNTGS_CNTY_NM,
 MBR_RELSHP_SRC_CD,
 MBR_RELSHP_SRC_NM,
 MBR_WELNS_BNF_LVL_CD,
 MBR_WELNS_BNF_LVL_NM,
 MBR_DCSD_IN,
 MBR_HOME_ADDR_IN_AREA_IN,
 MBR_DCSD_DT,
 MBR_UNIQ_KEY_ORIG_EFF_DT,
 MBR_MNTL_HLTH_VNDR_ID,
 MBR_WORK_PHN_NO,
 MBR_WORK_PHN_NO_EXT,
 CLNT_ID,
 CLNT_NM,
 MBR_WELNS_BNF_VNDR_ID,
 MBR_EAP_CAT_CD,
 MBR_EAP_CAT_DESC,
 MBR_VSN_BNF_VNDR_ID,
 MBR_VSN_RTN_EXAM_IN,
 MBR_VSN_HRDWR_IN,
 MBR_VSN_OUT_OF_NTWK_EXAM_IN,
 MBR_OTHR_CAR_PRI_MED_IN,
 MBR_PT_POD_DPLY_IN,
 MBR_CELL_PHN_NO,
 FEP_LOCAL_CNTR_IN,
 MBR_PT_EFF_DT,
 MBR_PT_TERM_DT,
 VBB_ENR_IN,
 VBB_MDL_CD,
 CMPSS_COV_IN,
 ALNO_HOME_PG_ID,
 PCMH_IN,
 MBR_PRFRD_EMAIL_ADDR_TX,
 DNTL_RWRD_IN
)
VALUES
(
 S.SRC_SYS_CD,
 S.MBR_UNIQ_KEY,
 S.SUB_UNIQ_KEY,
 S.MBR_FIRST_NM,
 S.MBR_MIDINIT,
 S.MBR_LAST_NM,
 S.LAST_UPDT_RUN_CYC_NO,
 S.GRP_ID,
 S.GRP_UNIQ_KEY,
 S.MBR_BRTH_DT,
 S.CLS_ID,
 S.CLS_DESC,
 S.GRP_NM,
 S.SUBGRP_ID,
 S.SUBGRP_NM,
 S.SUB_ID,
 S.MBR_GNDR_CD,
 S.MBR_PBM_VNDR_CD,
 S.SUB_ALPHA_PFX_CD,
 S.MBR_RELSHP_CD,
 S.MBR_ACTV_PROD_IN,
 S.MBR_CONF_COMM_IN,
 S.MBR_DP_MCARE_IN,
 S.SUB_IN,
 S.SUB_GRP_25_IN,
 S.MBR_MCARE_NO,
 S.MBR_HOME_ADDR_POSTAL_CD,
 S.MBR_SSN,
 S.MBR_SFX_NO,
 S.MBR_HOME_ADDR_LN_1,
 S.MBR_HOME_ADDR_LN_2,
 S.MBR_HOME_ADDR_LN_3,
 S.MBR_HOME_ADDR_CITY_NM,
 S.MBR_HOME_ADDR_ST_CD,
 S.MBR_HOME_ADDR_ST_NM,
 S.MBR_HOME_ADDR_ZIP_CD_5,
 S.MBR_HOME_ADDR_ZIP_CD_4,
 S.MBR_HOME_ADDR_CNTY_NM,
 S.MBR_HOME_ADDR_EMAIL_ADDR_TX,
 S.MBR_HOME_ADDR_FAX_NO,
 S.MBR_HOME_ADDR_FAX_NO_EXT,
 S.MBR_HOME_ADDR_PHN_NO,
 S.MBR_HOME_ADDR_PHN_NO_EXT,
 S.MBR_MAIL_ADDR_LN_1,
 S.MBR_MAIL_ADDR_LN_2,
 S.MBR_MAIL_ADDR_LN_3,
 S.MBR_MAIL_ADDR_CITY_NM,
 S.MBR_MAIL_ADDR_ST_CD,
 S.MBR_MAIL_ADDR_ST_NM,
 S.MBR_MAIL_ADDR_POSTAL_CD,
 S.MBR_MAIL_ADDR_ZIP_CD_5,
 S.MBR_MAIL_ADDR_ZIP_CD_4,
 S.MBR_MAIL_ADDR_CNTY_NM,
 S.MBR_MAIL_ADDR_EMAIL_ADDR_TX,
 S.MBR_MAIL_ADDR_FAX_NO,
 S.MBR_MAIL_ADDR_FAX_NO_EXT,
 S.MBR_MAIL_ADDR_PHN_NO,
 S.MBR_MAIL_ADDR_PHN_NO_EXT,
 S.MBR_MAIL_ADDR_CONF_COMM_IN,
 S.SUB_FIRST_NM,
 S.SUB_MIDINIT,
 S.SUB_LAST_NM,
 S.SUB_SSN,
 S.MBR_HOME_ADDR_CNTGS_CNTY_CD,
 S.MBR_HOME_ADDR_CNTGS_CNTY_NM,
 S.MBR_RELSHP_SRC_CD,
 S.MBR_RELSHP_SRC_NM,
 S.MBR_WELNS_BNF_LVL_CD,
 S.MBR_WELNS_BNF_LVL_NM,
 S.MBR_DCSD_IN,
 S.MBR_HOME_ADDR_IN_AREA_IN,
 S.MBR_DCSD_DT,
 S.MBR_UNIQ_KEY_ORIG_EFF_DT,
 S.MBR_MNTL_HLTH_VNDR_ID,
 S.MBR_WORK_PHN_NO,
 S.MBR_WORK_PHN_NO_EXT,
 S.CLNT_ID,
 S.CLNT_NM,
 S.MBR_WELNS_BNF_VNDR_ID,
 S.MBR_EAP_CAT_CD,
 S.MBR_EAP_CAT_DESC,
 S.MBR_VSN_BNF_VNDR_ID,
 S.MBR_VSN_RTN_EXAM_IN,
 S.MBR_VSN_HRDWR_IN,
 S.MBR_VSN_OUT_OF_NTWK_EXAM_IN,
 S.MBR_OTHR_CAR_PRI_MED_IN,
 S.MBR_PT_POD_DPLY_IN,
 S.MBR_CELL_PHN_NO,
 S.FEP_LOCAL_CNTR_IN,
 S.MBR_PT_EFF_DT,
 S.MBR_PT_TERM_DT,
 S.VBB_ENR_IN,
 S.VBB_MDL_CD,
 S.CMPSS_COV_IN,
 S.ALNO_HOME_PG_ID,
 S.PCMH_IN,
 S.MBR_PRFRD_EMAIL_ADDR_TX,
 S.DNTL_RWRD_IN
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

# =====================================================================
# Stage: seq_MBRSH_DM_MBR_Rej (PxSequentialFile) - WRITE (Rejects)
# =====================================================================
# Simulate a reject DataFrame (in real DataStage, rows failing DB operation appear here).
# We cannot truly capture DB merge failures in Spark. This placeholder ensures the schema matches.
all_cols_for_reject = df_xfm_NullHandling.columns + ["ERRORCODE", "ERRORTEXT"]
df_odbc_MBRSH_DM_MBR_Rej = (
    df_xfm_NullHandling
    .select(*df_xfm_NullHandling.columns)
    .withColumn("ERRORCODE", lit(None).cast(StringType()))
    .withColumn("ERRORTEXT", lit(None).cast(StringType()))
    .filter("1=0")  # Produce an empty DF with the correct schema
)

# For final file output, rpad all char/varchar columns. Use length=1 for known fixed chars, or 255 if unspecified.
df_odbc_MBRSH_DM_MBR_Rej_rpad = df_odbc_MBRSH_DM_MBR_Rej.select(
    rpad(col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY"),
    col("SUB_UNIQ_KEY"),
    rpad(col("MBR_FIRST_NM"), 255, " ").alias("MBR_FIRST_NM"),
    rpad(col("MBR_MIDINIT"), 1, " ").alias("MBR_MIDINIT"),
    rpad(col("MBR_LAST_NM"), 255, " ").alias("MBR_LAST_NM"),
    col("LAST_UPDT_RUN_CYC_NO"),
    rpad(col("GRP_ID"), 255, " ").alias("GRP_ID"),
    col("GRP_UNIQ_KEY"),
    col("MBR_BRTH_DT"),
    rpad(col("CLS_ID"), 255, " ").alias("CLS_ID"),
    rpad(col("CLS_DESC"), 255, " ").alias("CLS_DESC"),
    rpad(col("GRP_NM"), 255, " ").alias("GRP_NM"),
    rpad(col("SUBGRP_ID"), 255, " ").alias("SUBGRP_ID"),
    rpad(col("SUBGRP_NM"), 255, " ").alias("SUBGRP_NM"),
    rpad(col("SUB_ID"), 255, " ").alias("SUB_ID"),
    rpad(col("MBR_GNDR_CD"), 255, " ").alias("MBR_GNDR_CD"),
    rpad(col("MBR_PBM_VNDR_CD"), 255, " ").alias("MBR_PBM_VNDR_CD"),
    rpad(col("SUB_ALPHA_PFX_CD"), 255, " ").alias("SUB_ALPHA_PFX_CD"),
    rpad(col("MBR_RELSHP_CD"), 255, " ").alias("MBR_RELSHP_CD"),
    rpad(col("MBR_ACTV_PROD_IN"), 1, " ").alias("MBR_ACTV_PROD_IN"),
    rpad(col("MBR_CONF_COMM_IN"), 1, " ").alias("MBR_CONF_COMM_IN"),
    rpad(col("MBR_DP_MCARE_IN"), 1, " ").alias("MBR_DP_MCARE_IN"),
    rpad(col("SUB_IN"), 1, " ").alias("SUB_IN"),
    rpad(col("SUB_GRP_25_IN"), 1, " ").alias("SUB_GRP_25_IN"),
    rpad(col("MBR_MCARE_NO"), 255, " ").alias("MBR_MCARE_NO"),
    rpad(col("MBR_HOME_ADDR_POSTAL_CD"), 255, " ").alias("MBR_HOME_ADDR_POSTAL_CD"),
    rpad(col("MBR_SSN"), 255, " ").alias("MBR_SSN"),
    rpad(col("MBR_SFX_NO"), 255, " ").alias("MBR_SFX_NO"),
    rpad(col("MBR_HOME_ADDR_LN_1"), 255, " ").alias("MBR_HOME_ADDR_LN_1"),
    rpad(col("MBR_HOME_ADDR_LN_2"), 255, " ").alias("MBR_HOME_ADDR_LN_2"),
    rpad(col("MBR_HOME_ADDR_LN_3"), 255, " ").alias("MBR_HOME_ADDR_LN_3"),
    rpad(col("MBR_HOME_ADDR_CITY_NM"), 255, " ").alias("MBR_HOME_ADDR_CITY_NM"),
    rpad(col("MBR_HOME_ADDR_ST_CD"), 255, " ").alias("MBR_HOME_ADDR_ST_CD"),
    rpad(col("MBR_HOME_ADDR_ST_NM"), 255, " ").alias("MBR_HOME_ADDR_ST_NM"),
    rpad(col("MBR_HOME_ADDR_ZIP_CD_5"), 5, " ").alias("MBR_HOME_ADDR_ZIP_CD_5"),
    rpad(col("MBR_HOME_ADDR_ZIP_CD_4"), 4, " ").alias("MBR_HOME_ADDR_ZIP_CD_4"),
    rpad(col("MBR_HOME_ADDR_CNTY_NM"), 255, " ").alias("MBR_HOME_ADDR_CNTY_NM"),
    rpad(col("MBR_HOME_ADDR_EMAIL_ADDR_TX"), 255, " ").alias("MBR_HOME_ADDR_EMAIL_ADDR_TX"),
    rpad(col("MBR_HOME_ADDR_FAX_NO"), 255, " ").alias("MBR_HOME_ADDR_FAX_NO"),
    rpad(col("MBR_HOME_ADDR_FAX_NO_EXT"), 5, " ").alias("MBR_HOME_ADDR_FAX_NO_EXT"),
    rpad(col("MBR_HOME_ADDR_PHN_NO"), 255, " ").alias("MBR_HOME_ADDR_PHN_NO"),
    rpad(col("MBR_HOME_ADDR_PHN_NO_EXT"), 5, " ").alias("MBR_HOME_ADDR_PHN_NO_EXT"),
    rpad(col("MBR_MAIL_ADDR_LN_1"), 255, " ").alias("MBR_MAIL_ADDR_LN_1"),
    rpad(col("MBR_MAIL_ADDR_LN_2"), 255, " ").alias("MBR_MAIL_ADDR_LN_2"),
    rpad(col("MBR_MAIL_ADDR_LN_3"), 255, " ").alias("MBR_MAIL_ADDR_LN_3"),
    rpad(col("MBR_MAIL_ADDR_CITY_NM"), 255, " ").alias("MBR_MAIL_ADDR_CITY_NM"),
    rpad(col("MBR_MAIL_ADDR_ST_CD"), 255, " ").alias("MBR_MAIL_ADDR_ST_CD"),
    rpad(col("MBR_MAIL_ADDR_ST_NM"), 255, " ").alias("MBR_MAIL_ADDR_ST_NM"),
    rpad(col("MBR_MAIL_ADDR_POSTAL_CD"), 255, " ").alias("MBR_MAIL_ADDR_POSTAL_CD"),
    rpad(col("MBR_MAIL_ADDR_ZIP_CD_5"), 5, " ").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    rpad(col("MBR_MAIL_ADDR_ZIP_CD_4"), 4, " ").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    rpad(col("MBR_MAIL_ADDR_CNTY_NM"), 255, " ").alias("MBR_MAIL_ADDR_CNTY_NM"),
    rpad(col("MBR_MAIL_ADDR_EMAIL_ADDR_TX"), 255, " ").alias("MBR_MAIL_ADDR_EMAIL_ADDR_TX"),
    rpad(col("MBR_MAIL_ADDR_FAX_NO"), 255, " ").alias("MBR_MAIL_ADDR_FAX_NO"),
    rpad(col("MBR_MAIL_ADDR_FAX_NO_EXT"), 5, " ").alias("MBR_MAIL_ADDR_FAX_NO_EXT"),
    rpad(col("MBR_MAIL_ADDR_PHN_NO"), 255, " ").alias("MBR_MAIL_ADDR_PHN_NO"),
    rpad(col("MBR_MAIL_ADDR_PHN_NO_EXT"), 5, " ").alias("MBR_MAIL_ADDR_PHN_NO_EXT"),
    rpad(col("MBR_MAIL_ADDR_CONF_COMM_IN"), 1, " ").alias("MBR_MAIL_ADDR_CONF_COMM_IN"),
    rpad(col("SUB_FIRST_NM"), 255, " ").alias("SUB_FIRST_NM"),
    rpad(col("SUB_MIDINIT"), 1, " ").alias("SUB_MIDINIT"),
    rpad(col("SUB_LAST_NM"), 255, " ").alias("SUB_LAST_NM"),
    rpad(col("SUB_SSN"), 255, " ").alias("SUB_SSN"),
    rpad(col("MBR_HOME_ADDR_CNTGS_CNTY_CD"), 255, " ").alias("MBR_HOME_ADDR_CNTGS_CNTY_CD"),
    rpad(col("MBR_HOME_ADDR_CNTGS_CNTY_NM"), 255, " ").alias("MBR_HOME_ADDR_CNTGS_CNTY_NM"),
    rpad(col("MBR_RELSHP_SRC_CD"), 255, " ").alias("MBR_RELSHP_SRC_CD"),
    rpad(col("MBR_RELSHP_SRC_NM"), 255, " ").alias("MBR_RELSHP_SRC_NM"),
    rpad(col("MBR_WELNS_BNF_LVL_CD"), 255, " ").alias("MBR_WELNS_BNF_LVL_CD"),
    rpad(col("MBR_WELNS_BNF_LVL_NM"), 255, " ").alias("MBR_WELNS_BNF_LVL_NM"),
    rpad(col("MBR_DCSD_IN"), 1, " ").alias("MBR_DCSD_IN"),
    rpad(col("MBR_HOME_ADDR_IN_AREA_IN"), 1, " ").alias("MBR_HOME_ADDR_IN_AREA_IN"),
    col("MBR_DCSD_DT"),
    col("MBR_UNIQ_KEY_ORIG_EFF_DT"),
    rpad(col("MBR_MNTL_HLTH_VNDR_ID"), 255, " ").alias("MBR_MNTL_HLTH_VNDR_ID"),
    rpad(col("MBR_WORK_PHN_NO"), 255, " ").alias("MBR_WORK_PHN_NO"),
    rpad(col("MBR_WORK_PHN_NO_EXT"), 255, " ").alias("MBR_WORK_PHN_NO_EXT"),
    rpad(col("CLNT_ID"), 255, " ").alias("CLNT_ID"),
    rpad(col("CLNT_NM"), 255, " ").alias("CLNT_NM"),
    rpad(col("MBR_WELNS_BNF_VNDR_ID"), 255, " ").alias("MBR_WELNS_BNF_VNDR_ID"),
    rpad(col("MBR_EAP_CAT_CD"), 255, " ").alias("MBR_EAP_CAT_CD"),
    rpad(col("MBR_EAP_CAT_DESC"), 255, " ").alias("MBR_EAP_CAT_DESC"),
    rpad(col("MBR_VSN_BNF_VNDR_ID"), 255, " ").alias("MBR_VSN_BNF_VNDR_ID"),
    rpad(col("MBR_VSN_RTN_EXAM_IN"), 1, " ").alias("MBR_VSN_RTN_EXAM_IN"),
    rpad(col("MBR_VSN_HRDWR_IN"), 1, " ").alias("MBR_VSN_HRDWR_IN"),
    rpad(col("MBR_VSN_OUT_OF_NTWK_EXAM_IN"), 1, " ").alias("MBR_VSN_OUT_OF_NTWK_EXAM_IN"),
    rpad(col("MBR_OTHR_CAR_PRI_MED_IN"), 1, " ").alias("MBR_OTHR_CAR_PRI_MED_IN"),
    rpad(col("MBR_PT_POD_DPLY_IN"), 1, " ").alias("MBR_PT_POD_DPLY_IN"),
    rpad(col("MBR_CELL_PHN_NO"), 255, " ").alias("MBR_CELL_PHN_NO"),
    rpad(col("FEP_LOCAL_CNTR_IN"), 1, " ").alias("FEP_LOCAL_CNTR_IN"),
    col("MBR_PT_EFF_DT"),
    col("MBR_PT_TERM_DT"),
    rpad(col("VBB_ENR_IN"), 1, " ").alias("VBB_ENR_IN"),
    rpad(col("VBB_MDL_CD"), 255, " ").alias("VBB_MDL_CD"),
    rpad(col("CMPSS_COV_IN"), 1, " ").alias("CMPSS_COV_IN"),
    rpad(col("ALNO_HOME_PG_ID"), 255, " ").alias("ALNO_HOME_PG_ID"),
    rpad(col("PCMH_IN"), 1, " ").alias("PCMH_IN"),
    rpad(col("MBR_PRFRD_EMAIL_ADDR_TX"), 255, " ").alias("MBR_PRFRD_EMAIL_ADDR_TX"),
    rpad(col("DNTL_RWRD_IN"), 1, " ").alias("DNTL_RWRD_IN"),
    rpad(col("ERRORCODE"), 255, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), 255, " ").alias("ERRORTEXT")
)

write_files(
    df_odbc_MBRSH_DM_MBR_Rej_rpad,
    f"{adls_path}/load/MBRSH_DM_MBR_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)