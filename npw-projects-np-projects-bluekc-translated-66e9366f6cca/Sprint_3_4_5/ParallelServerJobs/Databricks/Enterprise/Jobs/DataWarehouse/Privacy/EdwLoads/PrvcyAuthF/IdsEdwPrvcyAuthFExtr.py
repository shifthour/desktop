# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                             Date                  Project/Altiris #               Change Description                             Development Project     Code Reviewer            Date Reviewed
# MAGIC ----------------------------------        -------------------      -----------------------------------    ---------------------------------------------------------      ----------------------------------    ---------------------------------    -------------------------   
# MAGIC Parikshith Chada                 2/26/2007                                                   Originally Programmed                        devlEDW10                  Steph Goddard             
# MAGIC 
# MAGIC Bhoomi Dasari                   2009-04-09           Prod Supp/15                  Updated logic at PRVCY_MBR_ID     devlEDW     
# MAGIC 
# MAGIC 
# MAGIC DEVELOPER                         DATE                 PROJECT                                                DESCRIPTION                                            DATASTAGE                                            CODE                            REVIEW
# MAGIC                                                                                                                                                                                                               ENVIRONMENT                                       REVIEWER                   DATE                                              
# MAGIC ------------------------------------------   ----------------------      ------------------------------------------------                ---------------------------------------------------------------     ---------------------------------------------------              ------------------------------          --------------------
# MAGIC Balkarn                                10/08/2013                          5114                                       Original Programming                                   EnterpriseWrhsDevl                                  Peter Marshall                  1/6/2014
# MAGIC                                                                                                                                           (Server to Parallel Conversion)

# MAGIC Job Name:
# MAGIC IdsEdwPrvcyAuthFExtr
# MAGIC Read from source table FCLTY_CLM_PROC , PROC_CD, CLM and W_EDW_ETL_DRVR
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Member and Privacy External Member Lookup
# MAGIC Add Defaults and Null Handling.
# MAGIC Write PRVCY_AUTH_F Data into a Sequential file for Load Job IdsEdwPrvcyAuthFLoad
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, trim, length, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
EDWOwner = get_widget_value('EDWOwner', '')
edw_secret_name = get_widget_value('edw_secret_name', '')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate', '')
CurrRunCycle = get_widget_value('CurrRunCycle', '')

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_PRVCY_AUTH_in (IDS)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""SELECT AUTH.PRVCY_AUTH_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
AUTH.PRVCY_MBR_UNIQ_KEY,
AUTH.SEQ_NO,
AUTH.PRVCY_MBR_SRC_CD_SK,
AUTH.CRT_RUN_CYC_EXCTN_SK,
AUTH.LAST_UPDT_RUN_CYC_EXCTN_SK,
AUTH.MBR_SK,
AUTH.PRVCY_AUTH_METH_SK,
AUTH.PRVCY_EXTRNL_MBR_SK,
AUTH.PRVCY_AUTH_RQSTR_TYP_CD_SK,
AUTH.PRVCY_AUTH_RECPNT_TYP_CD_SK,
AUTH.PRVCY_AUTH_TERM_RSN_CD_SK,
AUTH.AUTH_RVKD_IN,
AUTH.CRT_DT_SK,
AUTH.EFF_DT_SK,
AUTH.ORIG_END_DT_SK,
AUTH.TERM_DT_SK,
AUTH.AUTH_DESC,
AUTH.RECPNT_ID,
AUTH.SRC_SYS_LAST_UPDT_DT_SK,
AUTH.SRC_SYS_LAST_UPDT_USER_SK
FROM {IDSOwner}.PRVCY_AUTH AUTH
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON AUTH.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""
df_db2_PRVCY_AUTH_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_PRVCY_EXTRNL_MBR_D (EDW)
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = f"""SELECT
EXTRNL_MBR.PRVCY_EXTRNL_MBR_SK,
EXTRNL_MBR.PRVCY_EXTRNL_MBR_ID
FROM {EDWOwner}.PRVCY_EXTRNL_MBR_D EXTRNL_MBR
"""
df_db2_PRVCY_EXTRNL_MBR_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_MBR_D (EDW)
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = f"""SELECT
MBR_D.MBR_SK,
MBR_D.MBR_ID
FROM {EDWOwner}.MBR_D MBR_D
"""
df_db2_MBR_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_CD_MPPNG_Extr (IDS)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""SELECT CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# PxCopy: cpy
df_cpy = df_db2_CD_MPPNG_Extr

df_Ref_PrvcyAuthMbrSrcCdLkup = df_cpy.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_PrvcyAuthTermRsnCdLkup = df_cpy.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_PrvcyAuthRqstrTypCdLkup = df_cpy.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_PrvcyAuthRecpntTypCdLkup = df_cpy.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# --------------------------------------------------------------------------------
# PxLookup: lkp_Codes
df_lkp_Codes = df_db2_PRVCY_AUTH_in.alias("lnk_IdsEdwPrvcyAuthFExtr_InABC") \
    .join(
        df_Ref_PrvcyAuthTermRsnCdLkup.alias("Ref_PrvcyAuthTermRsnCdLkup"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.PRVCY_AUTH_TERM_RSN_CD_SK") == col("Ref_PrvcyAuthTermRsnCdLkup.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_Ref_PrvcyAuthRqstrTypCdLkup.alias("Ref_PrvcyAuthRqstrTypCdLkup"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.PRVCY_AUTH_RQSTR_TYP_CD_SK") == col("Ref_PrvcyAuthRqstrTypCdLkup.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_Ref_PrvcyAuthRecpntTypCdLkup.alias("Ref_PrvcyAuthRecpntTypCdLkup"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.PRVCY_AUTH_RECPNT_TYP_CD_SK") == col("Ref_PrvcyAuthRecpntTypCdLkup.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_Ref_PrvcyAuthMbrSrcCdLkup.alias("Ref_PrvcyAuthMbrSrcCdLkup"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.PRVCY_MBR_SRC_CD_SK") == col("Ref_PrvcyAuthMbrSrcCdLkup.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_db2_PRVCY_EXTRNL_MBR_D.alias("Ref_Extrnal_Mbr_D"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.PRVCY_EXTRNL_MBR_SK") == col("Ref_Extrnal_Mbr_D.PRVCY_EXTRNL_MBR_SK"),
        "left"
    ) \
    .join(
        df_db2_MBR_D.alias("Ref_Mbr_D"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.MBR_SK") == col("Ref_Mbr_D.MBR_SK"),
        "left"
    ) \
    .select(
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.PRVCY_AUTH_SK").alias("PRVCY_AUTH_SK"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.SEQ_NO").alias("SEQ_NO"),
        col("Ref_PrvcyAuthMbrSrcCdLkup.TRGT_CD").alias("PRVCY_MBR_SRC_CD"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.PRVCY_AUTH_METH_SK").alias("PRVCY_AUTH_METH_SK"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.MBR_SK").alias("MBR_SK"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.CRT_DT_SK").alias("CRT_DT_SK"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.AUTH_DESC").alias("AUTH_DESC"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.EFF_DT_SK").alias("EFF_DT_SK"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.ORIG_END_DT_SK").alias("ORIG_END_DT_SK"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.RECPNT_ID").alias("RECPNT_ID"),
        col("Ref_PrvcyAuthRecpntTypCdLkup.TRGT_CD").alias("PRVCY_AUTH_RECPNT_TYP_CD"),
        col("Ref_PrvcyAuthRecpntTypCdLkup.TRGT_CD_NM").alias("PRVCY_AUTH_RECPNT_TYP_NM"),
        col("Ref_PrvcyAuthRqstrTypCdLkup.TRGT_CD").alias("PRVCY_AUTH_RQSTR_TYP_CD"),
        col("Ref_PrvcyAuthRqstrTypCdLkup.TRGT_CD_NM").alias("PRVCY_AUTH_RQSTR_TYP_NM"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.AUTH_RVKD_IN").alias("AUTH_RVKD_IN"),
        col("Ref_PrvcyAuthTermRsnCdLkup.TRGT_CD").alias("PRVCY_AUTH_TERM_RSN_CD"),
        col("Ref_PrvcyAuthTermRsnCdLkup.TRGT_CD_NM").alias("PRVCY_AUTH_TERM_RSN_NM"),
        col("Ref_Extrnal_Mbr_D.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK_1"),
        col("Ref_Extrnal_Mbr_D.PRVCY_EXTRNL_MBR_ID").alias("PRVCY_EXTRNL_MBR_ID"),
        col("Ref_Mbr_D.MBR_ID").alias("MBR_ID"),
        col("Ref_PrvcyAuthMbrSrcCdLkup.TRGT_CD_NM").alias("PRVCY_MBR_SRC_NM"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.PRVCY_AUTH_RQSTR_TYP_CD_SK").alias("PRVCY_AUTH_RQSTR_TYP_CD_SK"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.PRVCY_AUTH_RECPNT_TYP_CD_SK").alias("PRVCY_AUTH_RECPNT_TYP_CD_SK"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.PRVCY_AUTH_TERM_RSN_CD_SK").alias("PRVCY_AUTH_TERM_RSN_CD_SK"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK"),
        col("lnk_IdsEdwPrvcyAuthFExtr_InABC.TERM_DT_SK").alias("TERM_DT_SK")
    )

# --------------------------------------------------------------------------------
# CTransformerStage: xfm_BusinessLogic

# -- lnk_Main --
df_lnk_Main = df_lkp_Codes.filter(
    (col("PRVCY_AUTH_SK") != 0) & (col("PRVCY_AUTH_SK") != 1)
)

df_lnk_Main = df_lnk_Main.withColumn(
    "SRC_SYS_CD",
    when(trim(col("SRC_SYS_CD")) == "", lit("NA")).otherwise(col("SRC_SYS_CD"))
).withColumn(
    "PRVCY_MBR_SRC_CD",
    when(col("PRVCY_MBR_SRC_CD").isNull() | (length(trim(col("PRVCY_MBR_SRC_CD"))) == 0), lit("NA")).otherwise(col("PRVCY_MBR_SRC_CD"))
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", lit(CurrRunCycleDate)
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(CurrRunCycleDate)
).withColumn(
    "PRVCY_AUTH_SEQ_NO", col("SEQ_NO")
).withColumn(
    "PRVCY_AUTH_CRT_DT_SK", col("CRT_DT_SK")
).withColumn(
    "PRVCY_AUTH_DESC", col("AUTH_DESC")
).withColumn(
    "PRVCY_AUTH_EFF_DT_SK", col("EFF_DT_SK")
).withColumn(
    "PRVCY_AUTH_ORIG_END_DT_SK", col("ORIG_END_DT_SK")
).withColumn(
    "PRVCY_AUTH_RECPNT_ID", col("RECPNT_ID")
).withColumn(
    "PRVCY_AUTH_RECPNT_TYP_CD",
    when(col("PRVCY_AUTH_RECPNT_TYP_CD").isNull() | (length(trim(col("PRVCY_AUTH_RECPNT_TYP_CD"))) == 0), lit("NA")).otherwise(col("PRVCY_AUTH_RECPNT_TYP_CD"))
).withColumn(
    "PRVCY_AUTH_RECPNT_TYP_NM",
    when(col("PRVCY_AUTH_RECPNT_TYP_NM").isNull() | (length(trim(col("PRVCY_AUTH_RECPNT_TYP_NM"))) == 0), lit("NA")).otherwise(col("PRVCY_AUTH_RECPNT_TYP_NM"))
).withColumn(
    "PRVCY_AUTH_RQSTR_TYP_CD",
    when(col("PRVCY_AUTH_RQSTR_TYP_CD").isNull() | (length(trim(col("PRVCY_AUTH_RQSTR_TYP_CD"))) == 0), lit("NA")).otherwise(col("PRVCY_AUTH_RQSTR_TYP_CD"))
).withColumn(
    "PRVCY_AUTH_RQSTR_TYP_NM",
    when(col("PRVCY_AUTH_RQSTR_TYP_NM").isNull() | (length(trim(col("PRVCY_AUTH_RQSTR_TYP_NM"))) == 0), lit("NA")).otherwise(col("PRVCY_AUTH_RQSTR_TYP_NM"))
).withColumn(
    "PRVCY_AUTH_RVKD_IN", col("AUTH_RVKD_IN")
).withColumn(
    "PRVCY_AUTH_TERM_RSN_CD",
    when(col("PRVCY_AUTH_TERM_RSN_CD").isNull() | (length(trim(col("PRVCY_AUTH_TERM_RSN_CD"))) == 0), lit("NA")).otherwise(col("PRVCY_AUTH_TERM_RSN_CD"))
).withColumn(
    "PRVCY_AUTH_TERM_RSN_NM",
    when(col("PRVCY_AUTH_TERM_RSN_NM").isNull() | (length(trim(col("PRVCY_AUTH_TERM_RSN_NM"))) == 0), lit("NA")).otherwise(col("PRVCY_AUTH_TERM_RSN_NM"))
).withColumn(
    "PRVCY_AUTH_TERM_DT_SK", col("TERM_DT_SK")
).withColumn(
    "PRVCY_MBR_ID",
    when(
        col("PRVCY_MBR_SRC_CD") == "FACETS",
        when(col("MBR_ID").isNotNull(), col("MBR_ID")).otherwise(lit("NA"))
    ).otherwise(
        when(col("PRVCY_EXTRNL_MBR_ID").isNotNull(), col("PRVCY_EXTRNL_MBR_ID")).otherwise(lit("NA"))
    )
).withColumn(
    "PRVCY_MBR_SRC_NM",
    when(col("PRVCY_MBR_SRC_NM").isNull() | (length(trim(col("PRVCY_MBR_SRC_NM"))) == 0), lit("NA")).otherwise(col("PRVCY_MBR_SRC_NM"))
).withColumn(
    "SRC_SYS_LAST_UPDT_DT_SK", col("SRC_SYS_LAST_UPDT_DT_SK")
).withColumn(
    "SRC_SYS_LAST_UPDT_USER_SK", col("SRC_SYS_LAST_UPDT_USER_SK")
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle)
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle)
).withColumn(
    "PRVCY_AUTH_RQSTR_TYP_CD_SK", col("PRVCY_AUTH_RQSTR_TYP_CD_SK")
).withColumn(
    "PRVCY_AUTH_RECPNT_TYP_CD_SK", col("PRVCY_AUTH_RECPNT_TYP_CD_SK")
).withColumn(
    "PRVCY_AUTH_TERM_RSN_CD_SK", col("PRVCY_AUTH_TERM_RSN_CD_SK")
).withColumn(
    "PRVCY_MBR_SRC_CD_SK", col("PRVCY_MBR_SRC_CD_SK")
)

df_lnk_Main = df_lnk_Main.select(
    col("PRVCY_AUTH_SK"),
    col("SRC_SYS_CD"),
    col("PRVCY_MBR_UNIQ_KEY"),
    col("PRVCY_AUTH_SEQ_NO"),
    col("PRVCY_MBR_SRC_CD"),
    col("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PRVCY_EXTRNL_MBR_SK"),
    col("PRVCY_AUTH_METH_SK"),
    col("MBR_SK"),
    col("PRVCY_AUTH_CRT_DT_SK"),
    col("PRVCY_AUTH_DESC"),
    col("PRVCY_AUTH_EFF_DT_SK"),
    col("PRVCY_AUTH_ORIG_END_DT_SK"),
    col("PRVCY_AUTH_RECPNT_ID"),
    col("PRVCY_AUTH_RECPNT_TYP_CD"),
    col("PRVCY_AUTH_RECPNT_TYP_NM"),
    col("PRVCY_AUTH_RQSTR_TYP_CD"),
    col("PRVCY_AUTH_RQSTR_TYP_NM"),
    col("PRVCY_AUTH_RVKD_IN"),
    col("PRVCY_AUTH_TERM_RSN_CD"),
    col("PRVCY_AUTH_TERM_RSN_NM"),
    col("PRVCY_AUTH_TERM_DT_SK"),
    col("PRVCY_MBR_ID"),
    col("PRVCY_MBR_SRC_NM"),
    col("SRC_SYS_LAST_UPDT_DT_SK"),
    col("SRC_SYS_LAST_UPDT_USER_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PRVCY_AUTH_RQSTR_TYP_CD_SK"),
    col("PRVCY_AUTH_RECPNT_TYP_CD_SK"),
    col("PRVCY_AUTH_TERM_RSN_CD_SK"),
    col("PRVCY_MBR_SRC_CD_SK")
)

# -- NA link: single row with fixed values
data_na = [(
    "1","NA","1","0","NA","1753-01-01","1753-01-01","1","1","1","1753-01-01","",
    "1753-01-01","1753-01-01","NA","NA","NA","NA","NA","N","NA","NA","1753-01-01",
    "NA","NA","1753-01-01","1","100","100","1","1","1","1"
)]
df_NA = spark.createDataFrame(data_na, schema=df_lnk_Main.schema)

# -- UNK link: single row with fixed values
data_unk = [(
    "0","UNK","0","0","UNK","1753-01-01","1753-01-01","0","0","0","1753-01-01","",
    "1753-01-01","1753-01-01","UNK","UNK","UNK","UNK","UNK","N","UNK","UNK","1753-01-01",
    "UNK","UNK","1753-01-01","0","100","100","0","0","0","0"
)]
df_UNK = spark.createDataFrame(data_unk, schema=df_lnk_Main.schema)

# --------------------------------------------------------------------------------
# PxFunnel: fnl_dataLinks
df_fnl_dataLinks = df_NA.unionByName(df_lnk_Main).unionByName(df_UNK)

# --------------------------------------------------------------------------------
# Before writing, apply rpad for char columns in final
df_fnl_dataLinks = (
    df_fnl_dataLinks
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("PRVCY_AUTH_CRT_DT_SK", rpad(col("PRVCY_AUTH_CRT_DT_SK"), 10, " "))
    .withColumn("PRVCY_AUTH_EFF_DT_SK", rpad(col("PRVCY_AUTH_EFF_DT_SK"), 10, " "))
    .withColumn("PRVCY_AUTH_ORIG_END_DT_SK", rpad(col("PRVCY_AUTH_ORIG_END_DT_SK"), 10, " "))
    .withColumn("PRVCY_AUTH_RVKD_IN", rpad(col("PRVCY_AUTH_RVKD_IN"), 1, " "))
    .withColumn("PRVCY_AUTH_TERM_DT_SK", rpad(col("PRVCY_AUTH_TERM_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", rpad(col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " "))
)

# Maintain the same column order as funnel's output
final_columns = [
    "PRVCY_AUTH_SK",
    "SRC_SYS_CD",
    "PRVCY_MBR_UNIQ_KEY",
    "PRVCY_AUTH_SEQ_NO",
    "PRVCY_MBR_SRC_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "PRVCY_EXTRNL_MBR_SK",
    "PRVCY_AUTH_METH_SK",
    "MBR_SK",
    "PRVCY_AUTH_CRT_DT_SK",
    "PRVCY_AUTH_DESC",
    "PRVCY_AUTH_EFF_DT_SK",
    "PRVCY_AUTH_ORIG_END_DT_SK",
    "PRVCY_AUTH_RECPNT_ID",
    "PRVCY_AUTH_RECPNT_TYP_CD",
    "PRVCY_AUTH_RECPNT_TYP_NM",
    "PRVCY_AUTH_RQSTR_TYP_CD",
    "PRVCY_AUTH_RQSTR_TYP_NM",
    "PRVCY_AUTH_RVKD_IN",
    "PRVCY_AUTH_TERM_RSN_CD",
    "PRVCY_AUTH_TERM_RSN_NM",
    "PRVCY_AUTH_TERM_DT_SK",
    "PRVCY_MBR_ID",
    "PRVCY_MBR_SRC_NM",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRVCY_AUTH_RQSTR_TYP_CD_SK",
    "PRVCY_AUTH_RECPNT_TYP_CD_SK",
    "PRVCY_AUTH_TERM_RSN_CD_SK",
    "PRVCY_MBR_SRC_CD_SK"
]
df_final = df_fnl_dataLinks.select(*final_columns)

# --------------------------------------------------------------------------------
# PxSequentialFile: seq_PRVCY_AUTH_F_Load
write_files(
    df_final,
    f"{adls_path}/load/PRVCY_AUTH_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)