# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                       Project/Altiris #               Change Description                                                                                 Development Project       Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------        -----------------------------------    -------------------------------------------------------------------------------------------------------------         ----------------------------------     ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               1/26/2007              3028                          Originally Programmed                                                                                     devlEDW10                     Steph Goddard             02/23/2007
# MAGIC Ralph Tucker                   1/17/2008         15                                   Took out Drvr Table to allow all IDS rows to be processed on a                      devlEDW                         Steph Goddard             01/30/2008
# MAGIC                                                                                                           Load/Replace                                                                                          
# MAGIC Bhupinder kaur                12/03/2013        5114                             Rewrite in Parallel                                                                                       EnterpriseWhrsDevl               Jag Yelavarthi              2014-01-29

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwCustSvcTaskLinkDExtr
# MAGIC Add Defaults and Null Handling.
# MAGIC Read the  IDS table : CUST_SVC_TASK_LINK
# MAGIC Write CUST_SVC_TASK_LINK_D Data into a Sequential file for Load Job IdsEdwCustSvcTaskDocDLoad.
# MAGIC Code SK lookups for Denormalization
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_db2_APP_USER_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT USER_SK, USER_ID FROM {IDSOwner}.APP_USER")
    .load()
)

df_db2_CUST_SVC_TASK_LINK_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""SELECT
 LINK.CUST_SVC_TASK_LINK_SK,
 LINK.SRC_SYS_CD_SK,
 LINK.CUST_SVC_ID,
 LINK.TASK_SEQ_NO,
 LINK.CUST_SVC_TASK_LINK_TYP_CD_SK,
 LINK.LINK_RCRD_ID,
 LINK.CRT_RUN_CYC_EXCTN_SK,
 LINK.LAST_UPDT_RUN_CYC_EXCTN_SK,
 LINK.APL_SK,
 LINK.CLM_SK,
 LINK.CUST_SVC_TASK_SK,
 LINK.LAST_UPDT_USER_SK,
 LINK.REL_CUST_SVC_SK,
 LINK.UM_SK,
 LINK.CUST_SVC_TASK_LINK_RSN_CD_SK,
 LINK.LAST_UPDT_DTM,
 LINK.LINK_DESC
 FROM {IDSOwner}.CUST_SVC_TASK_LINK LINK"""
    )
    .load()
)

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""SELECT
 CD_MPPNG_SK,
 COALESCE(TRGT_CD,'UNK') TRGT_CD,
 COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
 FROM {IDSOwner}.CD_MPPNG"""
    )
    .load()
)

df_cpy_Cd_Mppng = df_db2_CD_MPPNG_in

df_ref_SrcSysCd = df_cpy_Cd_Mppng.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_ref_CustSvcTskLinkTypCdLkup = df_cpy_Cd_Mppng.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_ref_CustSvcTskLinkRsnCdLkup = df_cpy_Cd_Mppng.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lnk_IdsEdwCustSvcTaskLinkDExtr_InABC = df_db2_CUST_SVC_TASK_LINK_D_in

df_lkp_Codes_joined = (
    df_lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.alias("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC")
    .join(
        df_ref_SrcSysCd.alias("ref_SrcSysCd"),
        col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.SRC_SYS_CD_SK") == col("ref_SrcSysCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_ref_CustSvcTskLinkTypCdLkup.alias("ref_CustSvcTskLinkTypCdLkup"),
        col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.CUST_SVC_TASK_LINK_TYP_CD_SK")
        == col("ref_CustSvcTskLinkTypCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_ref_CustSvcTskLinkRsnCdLkup.alias("ref_CustSvcTskLinkRsnCdLkup"),
        col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.CUST_SVC_TASK_LINK_RSN_CD_SK")
        == col("ref_CustSvcTskLinkRsnCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_APP_USER_in.alias("ref_LastupdtUser"),
        col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.LAST_UPDT_USER_SK") == col("ref_LastupdtUser.USER_SK"),
        "left"
    )
)

df_lkp_Codes = df_lkp_Codes_joined.select(
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.CUST_SVC_TASK_LINK_SK").alias("CUST_SVC_TASK_LINK_SK"),
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.CUST_SVC_ID").alias("CUST_SVC_ID"),
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.CUST_SVC_TASK_LINK_TYP_CD_SK").alias("CUST_SVC_TASK_LINK_TYP_CD_SK"),
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.LINK_RCRD_ID").alias("LINK_RCRD_ID"),
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.APL_SK").alias("APL_SK"),
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.CLM_SK").alias("CLM_SK"),
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.REL_CUST_SVC_SK").alias("REL_CUST_SVC_SK"),
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.UM_SK").alias("UM_SK"),
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.CUST_SVC_TASK_LINK_RSN_CD_SK").alias("CUST_SVC_TASK_LINK_RSN_CD_SK"),
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    col("lnk_IdsEdwCustSvcTaskLinkDExtr_InABC.LINK_DESC").alias("LINK_DESC"),
    col("ref_SrcSysCd.TRGT_CD").alias("SRC_SYS_CD"),
    col("ref_CustSvcTskLinkTypCdLkup.TRGT_CD").alias("LINK_TYP_CD"),
    col("ref_CustSvcTskLinkTypCdLkup.TRGT_CD_NM").alias("LINK_TYP_NM"),
    col("ref_CustSvcTskLinkRsnCdLkup.TRGT_CD").alias("LINK_RSN_CD"),
    col("ref_CustSvcTskLinkRsnCdLkup.TRGT_CD_NM").alias("LINK_RSN_NM"),
    col("ref_LastupdtUser.USER_ID").alias("USER_ID")
)

windowSpec = Window.orderBy(F.lit(1))
df_lkp_Codes_num = df_lkp_Codes.withColumn("_row_num", row_number().over(windowSpec))

df_lnk_FullData_filtered = df_lkp_Codes_num.filter(
    (col("CUST_SVC_TASK_LINK_SK") != 0) & (col("CUST_SVC_TASK_LINK_SK") != 1)
)

df_lnk_FullData = df_lnk_FullData_filtered.select(
    col("CUST_SVC_TASK_LINK_SK").alias("CUST_SVC_TASK_LINK_SK"),
    when(
        col("SRC_SYS_CD").isNull() | (trim(col("SRC_SYS_CD")) == ""), lit("NA")
    ).otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    col("CUST_SVC_ID").alias("CUST_SVC_ID"),
    col("TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
    when(
        col("LINK_TYP_CD").isNull() | (F.length(trim(col("LINK_TYP_CD"))) == 0),
        lit("NA")
    ).otherwise(col("LINK_TYP_CD")).alias("CUST_SVC_TASK_LINK_TYP_CD"),
    col("LINK_RCRD_ID").alias("CUST_SVC_TASK_LINK_RCRD_ID"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("APL_SK").alias("APL_SK"),
    col("CLM_SK").alias("CLM_SK"),
    col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    col("REL_CUST_SVC_SK").alias("REL_CUST_SVC_SK"),
    col("UM_SK").alias("UM_SK"),
    col("LINK_DESC").alias("CUST_SVC_TASK_LINK_DESC"),
    col("LAST_UPDT_DTM").alias("CS_TASK_LINK_LAST_UPDT_DTM"),
    when(
        col("LINK_RSN_CD").isNull() | (F.length(trim(col("LINK_RSN_CD"))) == 0),
        lit("NA")
    ).otherwise(col("LINK_RSN_CD")).alias("CUST_SVC_TASK_LINK_RSN_CD"),
    when(
        col("LINK_RSN_NM").isNull() | (F.length(trim(col("LINK_RSN_NM"))) == 0),
        lit("NA")
    ).otherwise(col("LINK_RSN_NM")).alias("CUST_SVC_TASK_LINK_RSN_NM"),
    when(
        col("LINK_TYP_NM").isNull() | (F.length(trim(col("LINK_TYP_NM"))) == 0),
        lit("NA")
    ).otherwise(col("LINK_TYP_NM")).alias("CUST_SVC_TASK_LINK_TYP_NM"),
    when(
        col("USER_ID").isNull() | (F.length(trim(col("USER_ID"))) == 0),
        lit("NA")
    ).otherwise(col("USER_ID")).alias("LAST_UPDT_USER_ID"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CUST_SVC_TASK_LINK_RSN_CD_SK").alias("CUST_SVC_TASK_LINK_RSN_CD_SK"),
    col("CUST_SVC_TASK_LINK_TYP_CD_SK").alias("CUST_SVC_TASK_LINK_TYP_CD_SK")
)

df_lnk_NA_filtered = df_lkp_Codes_num.filter(col("_row_num") == 1)
df_lnk_NA = df_lnk_NA_filtered.select(
    F.lit(1).alias("CUST_SVC_TASK_LINK_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("CUST_SVC_ID"),
    F.lit(1).alias("CUST_SVC_TASK_SEQ_NO"),
    F.lit("NA").alias("CUST_SVC_TASK_LINK_TYP_CD"),
    F.lit("NA").alias("CUST_SVC_TASK_LINK_RCRD_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("APL_SK"),
    F.lit(1).alias("CLM_SK"),
    F.lit(1).alias("CUST_SVC_TASK_SK"),
    F.lit(1).alias("LAST_UPDT_USER_SK"),
    F.lit(1).alias("REL_CUST_SVC_SK"),
    F.lit(1).alias("UM_SK"),
    F.lit(None).cast(StringType()).alias("CUST_SVC_TASK_LINK_DESC"),
    F.lit("1753-01-01 00:00:00").alias("CS_TASK_LINK_LAST_UPDT_DTM"),
    F.lit("NA").alias("CUST_SVC_TASK_LINK_RSN_CD"),
    F.lit("NA").alias("CUST_SVC_TASK_LINK_RSN_NM"),
    F.lit("NA").alias("CUST_SVC_TASK_LINK_TYP_NM"),
    F.lit("NA").alias("LAST_UPDT_USER_ID"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CUST_SVC_TASK_LINK_RSN_CD_SK"),
    F.lit(1).alias("CUST_SVC_TASK_LINK_TYP_CD_SK")
)

df_lnk_UNK_filtered = df_lkp_Codes_num.filter(col("_row_num") == 1)
df_lnk_UNK = df_lnk_UNK_filtered.select(
    F.lit(0).alias("CUST_SVC_TASK_LINK_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("CUST_SVC_ID"),
    F.lit(0).alias("CUST_SVC_TASK_SEQ_NO"),
    F.lit("UNK").alias("CUST_SVC_TASK_LINK_TYP_CD"),
    F.lit("UNK").alias("CUST_SVC_TASK_LINK_RCRD_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("APL_SK"),
    F.lit(0).alias("CLM_SK"),
    F.lit(0).alias("CUST_SVC_TASK_SK"),
    F.lit(0).alias("LAST_UPDT_USER_SK"),
    F.lit(0).alias("REL_CUST_SVC_SK"),
    F.lit(0).alias("UM_SK"),
    F.lit(None).cast(StringType()).alias("CUST_SVC_TASK_LINK_DESC"),
    F.lit("1753-01-01 00:00:00").alias("CS_TASK_LINK_LAST_UPDT_DTM"),
    F.lit("UNK").alias("CUST_SVC_TASK_LINK_RSN_CD"),
    F.lit("UNK").alias("CUST_SVC_TASK_LINK_RSN_NM"),
    F.lit("UNK").alias("CUST_SVC_TASK_LINK_TYP_NM"),
    F.lit("UNK").alias("LAST_UPDT_USER_ID"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CUST_SVC_TASK_LINK_RSN_CD_SK"),
    F.lit(0).alias("CUST_SVC_TASK_LINK_TYP_CD_SK")
)

df_Fnl_CustSvcTaskLinkD = df_lnk_FullData.select(
    "CUST_SVC_TASK_LINK_SK",
    "SRC_SYS_CD",
    "CUST_SVC_ID",
    "CUST_SVC_TASK_SEQ_NO",
    "CUST_SVC_TASK_LINK_TYP_CD",
    "CUST_SVC_TASK_LINK_RCRD_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "APL_SK",
    "CLM_SK",
    "CUST_SVC_TASK_SK",
    "LAST_UPDT_USER_SK",
    "REL_CUST_SVC_SK",
    "UM_SK",
    "CUST_SVC_TASK_LINK_DESC",
    "CS_TASK_LINK_LAST_UPDT_DTM",
    "CUST_SVC_TASK_LINK_RSN_CD",
    "CUST_SVC_TASK_LINK_RSN_NM",
    "CUST_SVC_TASK_LINK_TYP_NM",
    "LAST_UPDT_USER_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CUST_SVC_TASK_LINK_RSN_CD_SK",
    "CUST_SVC_TASK_LINK_TYP_CD_SK"
).union(
    df_lnk_NA.select(
        "CUST_SVC_TASK_LINK_SK",
        "SRC_SYS_CD",
        "CUST_SVC_ID",
        "CUST_SVC_TASK_SEQ_NO",
        "CUST_SVC_TASK_LINK_TYP_CD",
        "CUST_SVC_TASK_LINK_RCRD_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "APL_SK",
        "CLM_SK",
        "CUST_SVC_TASK_SK",
        "LAST_UPDT_USER_SK",
        "REL_CUST_SVC_SK",
        "UM_SK",
        "CUST_SVC_TASK_LINK_DESC",
        "CS_TASK_LINK_LAST_UPDT_DTM",
        "CUST_SVC_TASK_LINK_RSN_CD",
        "CUST_SVC_TASK_LINK_RSN_NM",
        "CUST_SVC_TASK_LINK_TYP_NM",
        "LAST_UPDT_USER_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CUST_SVC_TASK_LINK_RSN_CD_SK",
        "CUST_SVC_TASK_LINK_TYP_CD_SK"
    )
).union(
    df_lnk_UNK.select(
        "CUST_SVC_TASK_LINK_SK",
        "SRC_SYS_CD",
        "CUST_SVC_ID",
        "CUST_SVC_TASK_SEQ_NO",
        "CUST_SVC_TASK_LINK_TYP_CD",
        "CUST_SVC_TASK_LINK_RCRD_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "APL_SK",
        "CLM_SK",
        "CUST_SVC_TASK_SK",
        "LAST_UPDT_USER_SK",
        "REL_CUST_SVC_SK",
        "UM_SK",
        "CUST_SVC_TASK_LINK_DESC",
        "CS_TASK_LINK_LAST_UPDT_DTM",
        "CUST_SVC_TASK_LINK_RSN_CD",
        "CUST_SVC_TASK_LINK_RSN_NM",
        "CUST_SVC_TASK_LINK_TYP_NM",
        "LAST_UPDT_USER_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CUST_SVC_TASK_LINK_RSN_CD_SK",
        "CUST_SVC_TASK_LINK_TYP_CD_SK"
    )
)

df_Fnl_CustSvcTaskLinkD_final = df_Fnl_CustSvcTaskLinkD.select(
    col("CUST_SVC_TASK_LINK_SK"),
    col("SRC_SYS_CD"),
    col("CUST_SVC_ID"),
    col("CUST_SVC_TASK_SEQ_NO"),
    col("CUST_SVC_TASK_LINK_TYP_CD"),
    col("CUST_SVC_TASK_LINK_RCRD_ID"),
    F.rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("APL_SK"),
    col("CLM_SK"),
    col("CUST_SVC_TASK_SK"),
    col("LAST_UPDT_USER_SK"),
    col("REL_CUST_SVC_SK"),
    col("UM_SK"),
    col("CUST_SVC_TASK_LINK_DESC"),
    col("CS_TASK_LINK_LAST_UPDT_DTM"),
    col("CUST_SVC_TASK_LINK_RSN_CD"),
    col("CUST_SVC_TASK_LINK_RSN_NM"),
    col("CUST_SVC_TASK_LINK_TYP_NM"),
    F.rpad(col("LAST_UPDT_USER_ID"), 20, " ").alias("LAST_UPDT_USER_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CUST_SVC_TASK_LINK_RSN_CD_SK"),
    col("CUST_SVC_TASK_LINK_TYP_CD_SK")
)

write_files(
    df_Fnl_CustSvcTaskLinkD_final,
    f"{adls_path}/load/CUST_SVC_TASK_LINK_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)