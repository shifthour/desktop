# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE                    CODE                            DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT               REVIEWER                   REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------            ------------------------------       --------------------
# MAGIC Archana Palivela     06/07/2013        5114                              Originally Programmed  (In Parallel)                                                     EnterpriseWhseDevl                     Bhoomi Dasari           8/14/2013
# MAGIC 
# MAGIC Nagesh Bandi         04/27/2018        5832                             Added below fields for Spira Care project                                                EnterpriseDev2                   Kalyan Neelam             2018-05-08
# MAGIC 
# MAGIC                                                                                                EXPRNC_CAT_CD ,EXPRNC_CAT_DESC ,FNCL_LOB_CD ,FNCL_LOB_DESC ,PROD_ABBR             
# MAGIC                                                                                               ,PROD_DNTL_BILL_PFX_CD ,PROD_DNTL_BILL_PFX_NM ,PROD_DESC ,PROD_EFF_DT_SK
# MAGIC                                                                                               ,PROD_FCTS_CONV_RATE_PFX ,PROD_LOB_NO ,PROD_LOB_CD ,PROD_LOB_NM 
# MAGIC                                                                                               ,PROD_PCKG_CD ,PROD_PCKG_DESC ,PROD_RATE_TYP_CD ,PROD_RATE_TYP_NM 
# MAGIC                                                                                             ,PROD_ST_CD ,PROD_ST_NM ,PROD_SUBPROD_CD,PROD_SUBPROD_NM,PROD_TERM_DT_SK
# MAGIC                                                                                             ,PROD_SH_NM ,PROD_SH_NM_CAT_CD ,PROD_SH_NM_CAT_NM ,PROD_SH_NM_DLVRY_METH_CD
# MAGIC                                                                                              ,PROD_SH_NM_DLVRY_METH_NM ,PROD_SH_NM_DESC ,SPIRA_BNF_ID 
# MAGIC 
# MAGIC Deepika C                  2023-07-19        US 588189                 Updated FNCL_LOB_DESC column length to VARCHAR(255)                EnterpriseDevB                 Goutham Kalidindi          2023-08-08

# MAGIC Records are created in PROD_HIST_D table whenever a row changes in PROD_D table
# MAGIC 
# MAGIC Job Name: EdwEdwProdHistDExtr
# MAGIC Captures any changes from Prod_hist_d and Prod_d tables for the following columns:
# MAGIC PROD_DNTL_LATE_WAIT_IN
# MAGIC PROD_DRUG_COV_IN
# MAGIC PROD_HLTH_COV_IN
# MAGIC PROD_MNL_PRCS_IN
# MAGIC PROD_MNTL_HLTH_COV_IN
# MAGIC PROD_SHNM_MCARE_SUPLMT_COV_IN
# MAGIC PROD_VSN_COV_IN
# MAGIC Applys transformation rules for the columns:
# MAGIC EDW_RCRD_STRT_DT_SK, EDW_CUR_RCRD_IN and
# MAGIC EDW_RCRD_END_DT_SK acc to the cdma document.
# MAGIC Inner Join for the update columns from source table PROD_HIST_D and Transformer
# MAGIC Funnel coulmns for Update and Insert
# MAGIC Create a load seq file for load jon.
# MAGIC Reads After  data from source table PROD_D
# MAGIC Read Before data from  table PROD_HIST_D
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    lit,
    when,
    coalesce,
    concat_ws,
    row_number,
    to_date,
    date_sub,
    date_format,
    monotonically_increasing_id,
    rpad
)
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url_EDW, jdbc_props_EDW = get_db_config(edw_secret_name)

query_DB2_PROD_D_Extrr = """
SELECT PROD_SK,
SRC_SYS_CD,
PROD_ID,
PROD_DNTL_LATE_WAIT_IN,
PROD_DRUG_COV_IN,
PROD_HLTH_COV_IN,
PROD_MNL_PRCS_IN,
PROD_MNTL_HLTH_COV_IN,
PROD_SHNM_MCARE_SUPLMT_COV_IN,
PROD_VSN_COV_IN,
EXPRNC_CAT_CD,
EXPRNC_CAT_DESC,
FNCL_LOB_CD,
FNCL_LOB_DESC,
PROD_ABBR,
PROD_DNTL_BILL_PFX_CD,
PROD_DNTL_BILL_PFX_NM,
PROD_DESC,
PROD_EFF_DT_SK,
PROD_FCTS_CONV_RATE_PFX,
PROD_LOB_NO,
PROD_LOB_CD,
PROD_LOB_NM,
PROD_PCKG_CD,
PROD_PCKG_DESC,
PROD_RATE_TYP_CD,
PROD_RATE_TYP_NM,
PROD_ST_CD,
PROD_ST_NM,
PROD_SUBPROD_CD,
PROD_SUBPROD_NM,
PROD_TERM_DT_SK,
PROD_SH_NM,
PROD_SH_NM_CAT_CD,
PROD_SH_NM_CAT_NM,
PROD_SH_NM_DLVRY_METH_CD,
PROD_SH_NM_DLVRY_METH_NM,
PROD_SH_NM_DESC,
SPIRA_BNF_ID
FROM {edwOwner}.PROD_D
WHERE
LAST_UPDT_RUN_CYC_EXCTN_SK >= {edwRunCycle}
""".format(edwOwner=EDWOwner, edwRunCycle=EDWRunCycle)

df_DB2_PROD_D_Extrr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("query", query_DB2_PROD_D_Extrr)
    .load()
)

query_DB2_PROD_HIST_D = """
SELECT PROD_SK,
EDW_RCRD_STRT_DT_SK,
PROD_ID,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_DT_SK,
LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
EDW_CUR_RCRD_IN,
PROD_DNTL_LATE_WAIT_IN,
PROD_DRUG_COV_IN,
PROD_HLTH_COV_IN,
PROD_MNL_PRCS_IN,
PROD_MNTL_HLTH_COV_IN,
PROD_SHNM_MCARE_SUPLMT_COV_IN,
PROD_VSN_COV_IN,
EDW_RCRD_END_DT_SK,
CRT_RUN_CYC_EXCTN_SK,
LAST_UPDT_RUN_CYC_EXCTN_SK,
PROD_SK as PROD_SK_HIST,
EXPRNC_CAT_CD,
EXPRNC_CAT_DESC,
FNCL_LOB_CD,
FNCL_LOB_DESC,
PROD_ABBR,
PROD_DNTL_BILL_PFX_CD,
PROD_DNTL_BILL_PFX_NM,
PROD_DESC,
PROD_EFF_DT_SK,
PROD_FCTS_CONV_RATE_PFX,
PROD_LOB_NO,
PROD_LOB_CD,
PROD_LOB_NM,
PROD_PCKG_CD,
PROD_PCKG_DESC,
PROD_RATE_TYP_CD,
PROD_RATE_TYP_NM,
PROD_ST_CD,
PROD_ST_NM,
PROD_SUBPROD_CD,
PROD_SUBPROD_NM,
PROD_TERM_DT_SK,
PROD_SH_NM,
PROD_SH_NM_CAT_CD,
PROD_SH_NM_CAT_NM,
PROD_SH_NM_DLVRY_METH_CD,
PROD_SH_NM_DLVRY_METH_NM,
PROD_SH_NM_DESC,
SPIRA_BNF_ID
FROM {edwOwner}.PROD_HIST_D
WHERE EDW_CUR_RCRD_IN = 'Y'
""".format(edwOwner=EDWOwner)

df_DB2_PROD_HIST_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("query", query_DB2_PROD_HIST_D)
    .load()
)

df_cpy_ProdHistD = df_DB2_PROD_HIST_D

df_Lnk_ProdHistD_Before_In = df_cpy_ProdHistD.select(
    col("PROD_SK").alias("PROD_SK"),
    col("PROD_DNTL_LATE_WAIT_IN").alias("PROD_DNTL_LATE_WAIT_IN_H"),
    col("PROD_DRUG_COV_IN").alias("PROD_DRUG_COV_IN_H"),
    col("PROD_HLTH_COV_IN").alias("PROD_HLTH_COV_IN_H"),
    col("PROD_MNL_PRCS_IN").alias("PROD_MNL_PRCS_IN_H"),
    col("PROD_MNTL_HLTH_COV_IN").alias("PROD_MNTL_HLTH_COV_IN_H"),
    col("PROD_SHNM_MCARE_SUPLMT_COV_IN").alias("PROD_SHNM_MCARE_SUPLMT_COV_IN_H"),
    col("PROD_VSN_COV_IN").alias("PROD_VSN_COV_IN_H"),
    col("PROD_SK_HIST").alias("PROD_SK_HIST"),
    col("EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD_H"),
    col("EXPRNC_CAT_DESC").alias("EXPRNC_CAT_DESC_H"),
    col("FNCL_LOB_CD").alias("FNCL_LOB_CD_H"),
    col("FNCL_LOB_DESC").alias("FNCL_LOB_DESC_H"),
    col("PROD_ABBR").alias("PROD_ABBR_H"),
    col("PROD_DNTL_BILL_PFX_CD").alias("PROD_DNTL_BILL_PFX_CD_H"),
    col("PROD_DNTL_BILL_PFX_NM").alias("PROD_DNTL_BILL_PFX_NM_H"),
    col("PROD_DESC").alias("PROD_DESC_H"),
    col("PROD_EFF_DT_SK").alias("PROD_EFF_DT_SK_H"),
    col("PROD_FCTS_CONV_RATE_PFX").alias("PROD_FCTS_CONV_RATE_PFX_H"),
    col("PROD_LOB_NO").alias("PROD_LOB_NO_H"),
    col("PROD_LOB_CD").alias("PROD_LOB_CD_H"),
    col("PROD_LOB_NM").alias("PROD_LOB_NM_H"),
    col("PROD_PCKG_CD").alias("PROD_PCKG_CD_H"),
    col("PROD_PCKG_DESC").alias("PROD_PCKG_DESC_H"),
    col("PROD_RATE_TYP_CD").alias("PROD_RATE_TYP_CD_H"),
    col("PROD_RATE_TYP_NM").alias("PROD_RATE_TYP_NM_H"),
    col("PROD_ST_CD").alias("PROD_ST_CD_H"),
    col("PROD_ST_NM").alias("PROD_ST_NM_H"),
    col("PROD_SUBPROD_CD").alias("PROD_SUBPROD_CD_H"),
    col("PROD_SUBPROD_NM").alias("PROD_SUBPROD_NM_H"),
    col("PROD_TERM_DT_SK").alias("PROD_TERM_DT_SK_H"),
    col("PROD_SH_NM").alias("PROD_SH_NM_H"),
    col("PROD_SH_NM_CAT_CD").alias("PROD_SH_NM_CAT_CD_H"),
    col("PROD_SH_NM_CAT_NM").alias("PROD_SH_NM_CAT_NM_H"),
    col("PROD_SH_NM_DLVRY_METH_CD").alias("PROD_SH_NM_DLVRY_METH_CD_H"),
    col("PROD_SH_NM_DLVRY_METH_NM").alias("PROD_SH_NM_DLVRY_METH_NM_H"),
    col("PROD_SH_NM_DESC").alias("PROD_SH_NM_DESC_H"),
    col("SPIRA_BNF_ID").alias("SPIRA_BNF_ID_H")
)

df_Lnk_L_ProdHistD_In = df_cpy_ProdHistD.select(
    col("PROD_SK").alias("PROD_SK"),
    col("EDW_RCRD_STRT_DT_SK").alias("EDW_RCRD_STRT_DT_SK"),
    col("PROD_ID").alias("PROD_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("PROD_DNTL_LATE_WAIT_IN").alias("PROD_DNTL_LATE_WAIT_IN"),
    col("PROD_DRUG_COV_IN").alias("PROD_DRUG_COV_IN"),
    col("PROD_HLTH_COV_IN").alias("PROD_HLTH_COV_IN"),
    col("PROD_MNL_PRCS_IN").alias("PROD_MNL_PRCS_IN"),
    col("PROD_MNTL_HLTH_COV_IN").alias("PROD_MNTL_HLTH_COV_IN"),
    col("PROD_SHNM_MCARE_SUPLMT_COV_IN").alias("PROD_SHNM_MCARE_SUPLMT_COV_IN"),
    col("PROD_VSN_COV_IN").alias("PROD_VSN_COV_IN"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
    col("EXPRNC_CAT_DESC").alias("EXPRNC_CAT_DESC"),
    col("FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    col("FNCL_LOB_DESC").alias("FNCL_LOB_DESC"),
    col("PROD_ABBR").alias("PROD_ABBR"),
    col("PROD_DNTL_BILL_PFX_CD").alias("PROD_DNTL_BILL_PFX_CD"),
    col("PROD_DNTL_BILL_PFX_NM").alias("PROD_DNTL_BILL_PFX_NM"),
    col("PROD_DESC").alias("PROD_DESC"),
    col("PROD_EFF_DT_SK").alias("PROD_EFF_DT_SK"),
    col("PROD_FCTS_CONV_RATE_PFX").alias("PROD_FCTS_CONV_RATE_PFX"),
    col("PROD_LOB_NO").alias("PROD_LOB_NO"),
    col("PROD_LOB_CD").alias("PROD_LOB_CD"),
    col("PROD_LOB_NM").alias("PROD_LOB_NM"),
    col("PROD_PCKG_CD").alias("PROD_Pckg_Cd"),
    col("PROD_PCKG_DESC").alias("PROD_PCKG_DESC"),
    col("PROD_RATE_TYP_CD").alias("PROD_RATE_TYP_CD"),
    col("PROD_RATE_TYP_NM").alias("PROD_RATE_TYP_NM"),
    col("PROD_ST_CD").alias("PROD_ST_CD"),
    col("PROD_ST_NM").alias("PROD_ST_NM"),
    col("PROD_SUBPROD_CD").alias("PROD_SUBPROD_CD"),
    col("PROD_SUBPROD_NM").alias("PROD_SUBPROD_NM"),
    col("PROD_TERM_DT_SK").alias("PROD_TERM_DT_SK"),
    col("PROD_SH_NM").alias("PROD_SH_NM"),
    col("PROD_SH_NM_CAT_CD").alias("PROD_SH_NM_CAT_CD"),
    col("PROD_SH_NM_CAT_NM").alias("PROD_SH_NM_CAT_NM"),
    col("PROD_SH_NM_DLVRY_METH_CD").alias("PROD_SH_NM_DLVRY_METH_CD"),
    col("PROD_SH_NM_DLVRY_METH_NM").alias("PROD_SH_NM_DLVRY_METH_NM"),
    col("PROD_SH_NM_DESC").alias("PROD_SH_NM_DESC"),
    col("SPIRA_BNF_ID").alias("SPIRA_BNF_ID")
)

df_Lnk_EdwEdwProdD_After_In = df_DB2_PROD_D_Extrr

df_Jn_ProdSkCompare = df_Lnk_EdwEdwProdD_After_In.alias("Lnk_EdwEdwProdD_After_In").join(
    df_Lnk_ProdHistD_Before_In.alias("Lnk_ProdHistD_Before_In"),
    on=[col("Lnk_EdwEdwProdD_After_In.PROD_SK") == col("Lnk_ProdHistD_Before_In.PROD_SK")],
    how="left"
)

df_Lnk_cdc_InABC = df_Jn_ProdSkCompare.select(
    col("Lnk_EdwEdwProdD_After_In.PROD_SK").alias("PROD_SK"),
    col("Lnk_EdwEdwProdD_After_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Lnk_EdwEdwProdD_After_In.PROD_ID").alias("PROD_ID"),
    col("Lnk_EdwEdwProdD_After_In.PROD_DNTL_LATE_WAIT_IN").alias("PROD_DNTL_LATE_WAIT_IN"),
    col("Lnk_EdwEdwProdD_After_In.PROD_DRUG_COV_IN").alias("PROD_DRUG_COV_IN"),
    col("Lnk_EdwEdwProdD_After_In.PROD_HLTH_COV_IN").alias("PROD_HLTH_COV_IN"),
    col("Lnk_EdwEdwProdD_After_In.PROD_MNL_PRCS_IN").alias("PROD_MNL_PRCS_IN"),
    col("Lnk_EdwEdwProdD_After_In.PROD_MNTL_HLTH_COV_IN").alias("PROD_MNTL_HLTH_COV_IN"),
    col("Lnk_EdwEdwProdD_After_In.PROD_SHNM_MCARE_SUPLMT_COV_IN").alias("PROD_SHNM_MCARE_SUPLMT_COV_IN"),
    col("Lnk_EdwEdwProdD_After_In.PROD_VSN_COV_IN").alias("PROD_VSN_COV_IN"),
    col("Lnk_ProdHistD_Before_In.PROD_DNTL_LATE_WAIT_IN_H").alias("PROD_DNTL_LATE_WAIT_IN_H"),
    col("Lnk_ProdHistD_Before_In.PROD_DRUG_COV_IN_H").alias("PROD_DRUG_COV_IN_H"),
    col("Lnk_ProdHistD_Before_In.PROD_HLTH_COV_IN_H").alias("PROD_HLTH_COV_IN_H"),
    col("Lnk_ProdHistD_Before_In.PROD_MNL_PRCS_IN_H").alias("PROD_MNL_PRCS_IN_H"),
    col("Lnk_ProdHistD_Before_In.PROD_MNTL_HLTH_COV_IN_H").alias("PROD_MNTL_HLTH_COV_IN_H"),
    col("Lnk_ProdHistD_Before_In.PROD_SHNM_MCARE_SUPLMT_COV_IN_H").alias("PROD_SHNM_MCARE_SUPLMT_COV_IN_H"),
    col("Lnk_ProdHistD_Before_In.PROD_VSN_COV_IN_H").alias("PROD_VSN_COV_IN_H"),
    col("Lnk_ProdHistD_Before_In.PROD_SK_HIST").alias("PROD_SK_HIST"),
    col("Lnk_EdwEdwProdD_After_In.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
    col("Lnk_EdwEdwProdD_After_In.EXPRNC_CAT_DESC").alias("EXPRNC_CAT_DESC"),
    col("Lnk_EdwEdwProdD_After_In.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    col("Lnk_EdwEdwProdD_After_In.FNCL_LOB_DESC").alias("FNCL_LOB_DESC"),
    col("Lnk_EdwEdwProdD_After_In.PROD_ABBR").alias("PROD_ABBR"),
    col("Lnk_EdwEdwProdD_After_In.PROD_DNTL_BILL_PFX_CD").alias("PROD_DNTL_BILL_PFX_CD"),
    col("Lnk_EdwEdwProdD_After_In.PROD_DNTL_BILL_PFX_NM").alias("PROD_DNTL_BILL_PFX_NM"),
    col("Lnk_EdwEdwProdD_After_In.PROD_DESC").alias("PROD_DESC"),
    col("Lnk_EdwEdwProdD_After_In.PROD_EFF_DT_SK").alias("PROD_EFF_DT_SK"),
    col("Lnk_EdwEdwProdD_After_In.PROD_FCTS_CONV_RATE_PFX").alias("PROD_FCTS_CONV_RATE_PFX"),
    col("Lnk_EdwEdwProdD_After_In.PROD_LOB_NO").alias("PROD_LOB_NO"),
    col("Lnk_EdwEdwProdD_After_In.PROD_LOB_CD").alias("PROD_LOB_CD"),
    col("Lnk_EdwEdwProdD_After_In.PROD_LOB_NM").alias("PROD_LOB_NM"),
    col("Lnk_EdwEdwProdD_After_In.PROD_PCKG_CD").alias("PROD_PCKG_CD"),
    col("Lnk_EdwEdwProdD_After_In.PROD_PCKG_DESC").alias("PROD_PCKG_DESC"),
    col("Lnk_EdwEdwProdD_After_In.PROD_RATE_TYP_CD").alias("PROD_RATE_TYP_CD"),
    col("Lnk_EdwEdwProdD_After_In.PROD_RATE_TYP_NM").alias("PROD_RATE_TYP_NM"),
    col("Lnk_EdwEdwProdD_After_In.PROD_ST_CD").alias("PROD_ST_CD"),
    col("Lnk_EdwEdwProdD_After_In.PROD_ST_NM").alias("PROD_ST_NM"),
    col("Lnk_EdwEdwProdD_After_In.PROD_SUBPROD_CD").alias("PROD_SUBPROD_CD"),
    col("Lnk_EdwEdwProdD_After_In.PROD_SUBPROD_NM").alias("PROD_SUBPROD_NM"),
    col("Lnk_EdwEdwProdD_After_In.PROD_TERM_DT_SK").alias("PROD_TERM_DT_SK"),
    col("Lnk_EdwEdwProdD_After_In.PROD_SH_NM").alias("PROD_SH_NM"),
    col("Lnk_EdwEdwProdD_After_In.PROD_SH_NM_CAT_CD").alias("PROD_SH_NM_CAT_CD"),
    col("Lnk_EdwEdwProdD_After_In.PROD_SH_NM_CAT_NM").alias("PROD_SH_NM_CAT_NM"),
    col("Lnk_EdwEdwProdD_After_In.PROD_SH_NM_DLVRY_METH_CD").alias("PROD_SH_NM_DLVRY_METH_CD"),
    col("Lnk_EdwEdwProdD_After_In.PROD_SH_NM_DLVRY_METH_NM").alias("PROD_SH_NM_DLVRY_METH_NM"),
    col("Lnk_EdwEdwProdD_After_In.PROD_SH_NM_DESC").alias("PROD_SH_NM_DESC"),
    col("Lnk_EdwEdwProdD_After_In.SPIRA_BNF_ID").alias("SPIRA_BNF_ID"),
    col("Lnk_ProdHistD_Before_In.EXPRNC_CAT_CD_H").alias("EXPRNC_CAT_CD_H"),
    col("Lnk_ProdHistD_Before_In.EXPRNC_CAT_DESC_H").alias("EXPRNC_CAT_DESC_H"),
    col("Lnk_ProdHistD_Before_In.FNCL_LOB_CD_H").alias("FNCL_LOB_CD_H"),
    col("Lnk_ProdHistD_Before_In.FNCL_LOB_DESC_H").alias("FNCL_LOB_DESC_H"),
    col("Lnk_ProdHistD_Before_In.PROD_ABBR_H").alias("PROD_ABBR_H"),
    col("Lnk_ProdHistD_Before_In.PROD_DNTL_BILL_PFX_CD_H").alias("PROD_DNTL_BILL_PFX_CD_H"),
    col("Lnk_ProdHistD_Before_In.PROD_DNTL_BILL_PFX_NM_H").alias("PROD_DNTL_BILL_PFX_NM_H"),
    col("Lnk_ProdHistD_Before_In.PROD_DESC_H").alias("PROD_DESC_H"),
    col("Lnk_ProdHistD_Before_In.PROD_EFF_DT_SK_H").alias("PROD_EFF_DT_SK_H"),
    col("Lnk_ProdHistD_Before_In.PROD_FCTS_CONV_RATE_PFX_H").alias("PROD_FCTS_CONV_RATE_PFX_H"),
    col("Lnk_ProdHistD_Before_In.PROD_LOB_NO_H").alias("PROD_LOB_NO_H"),
    col("Lnk_ProdHistD_Before_In.PROD_LOB_CD_H").alias("PROD_LOB_CD_H"),
    col("Lnk_ProdHistD_Before_In.PROD_LOB_NM_H").alias("PROD_LOB_NM_H"),
    col("Lnk_ProdHistD_Before_In.PROD_PCKG_CD_H").alias("PROD_PCKG_CD_H"),
    col("Lnk_ProdHistD_Before_In.PROD_PCKG_DESC_H").alias("PROD_PCKG_DESC_H"),
    col("Lnk_ProdHistD_Before_In.PROD_RATE_TYP_CD_H").alias("PROD_RATE_TYP_CD_H"),
    col("Lnk_ProdHistD_Before_In.PROD_RATE_TYP_NM_H").alias("PROD_RATE_TYP_NM_H"),
    col("Lnk_ProdHistD_Before_In.PROD_ST_CD_H").alias("PROD_ST_CD_H"),
    col("Lnk_ProdHistD_Before_In.PROD_ST_NM_H").alias("PROD_ST_NM_H"),
    col("Lnk_ProdHistD_Before_In.PROD_SUBPROD_CD_H").alias("PROD_SUBPROD_CD_H"),
    col("Lnk_ProdHistD_Before_In.PROD_SUBPROD_NM_H").alias("PROD_SUBPROD_NM_H"),
    col("Lnk_ProdHistD_Before_In.PROD_TERM_DT_SK_H").alias("PROD_TERM_DT_SK_H"),
    col("Lnk_ProdHistD_Before_In.PROD_SH_NM_H").alias("PROD_SH_NM_H"),
    col("Lnk_ProdHistD_Before_In.PROD_SH_NM_CAT_CD_H").alias("PROD_SH_NM_CAT_CD_H"),
    col("Lnk_ProdHistD_Before_In.PROD_SH_NM_CAT_NM_H").alias("PROD_SH_NM_CAT_NM_H"),
    col("Lnk_ProdHistD_Before_In.PROD_SH_NM_DLVRY_METH_CD_H").alias("PROD_SH_NM_DLVRY_METH_CD_H"),
    col("Lnk_ProdHistD_Before_In.PROD_SH_NM_DLVRY_METH_NM_H").alias("PROD_SH_NM_DLVRY_METH_NM_H"),
    col("Lnk_ProdHistD_Before_In.PROD_SH_NM_DESC_H").alias("PROD_SH_NM_DESC_H"),
    col("Lnk_ProdHistD_Before_In.SPIRA_BNF_ID_H").alias("SPIRA_BNF_ID_H")
)

df_xfrm_businesslogic_temp = (
    df_Lnk_cdc_InABC
    .withColumn(
        "svPrev",
        concat_ws(
            ":",
            col("PROD_DNTL_LATE_WAIT_IN_H"),
            col("PROD_DRUG_COV_IN_H"),
            col("PROD_HLTH_COV_IN_H"),
            col("PROD_MNL_PRCS_IN_H"),
            col("PROD_MNTL_HLTH_COV_IN_H"),
            col("PROD_SHNM_MCARE_SUPLMT_COV_IN_H"),
            col("PROD_VSN_COV_IN_H"),
            coalesce(col("EXPRNC_CAT_CD_H"), lit(" ")),
            coalesce(col("EXPRNC_CAT_DESC_H"), lit(" ")),
            coalesce(col("FNCL_LOB_CD_H"), lit(" ")),
            coalesce(col("FNCL_LOB_DESC_H"), lit(" ")),
            coalesce(col("PROD_ABBR_H"), lit(" ")),
            coalesce(col("PROD_DNTL_BILL_PFX_CD_H"), lit(" ")),
            coalesce(col("PROD_DNTL_BILL_PFX_NM_H"), lit(" ")),
            coalesce(col("PROD_DESC_H"), lit(" ")),
            coalesce(col("PROD_EFF_DT_SK_H"), lit(" ")),
            coalesce(col("PROD_FCTS_CONV_RATE_PFX_H"), lit(" ")),
            coalesce(col("PROD_LOB_NO_H"), lit(" ")),
            coalesce(col("PROD_LOB_CD_H"), lit(" ")),
            coalesce(col("PROD_LOB_NM_H"), lit(" ")),
            coalesce(col("PROD_PCKG_CD_H"), lit(" ")),
            coalesce(col("PROD_PCKG_DESC_H"), lit(" ")),
            coalesce(col("PROD_RATE_TYP_CD_H"), lit(" ")),
            coalesce(col("PROD_RATE_TYP_NM_H"), lit(" ")),
            coalesce(col("PROD_ST_CD_H"), lit(" ")),
            coalesce(col("PROD_ST_NM_H"), lit(" ")),
            coalesce(col("PROD_SUBPROD_CD_H"), lit(" ")),
            coalesce(col("PROD_SUBPROD_NM_H"), lit(" ")),
            coalesce(col("PROD_TERM_DT_SK_H"), lit(" ")),
            coalesce(col("PROD_SH_NM_H"), lit(" ")),
            coalesce(col("PROD_SH_NM_CAT_CD_H"), lit(" ")),
            coalesce(col("PROD_SH_NM_CAT_NM_H"), lit(" ")),
            coalesce(col("PROD_SH_NM_DLVRY_METH_CD_H"), lit(" ")),
            coalesce(col("PROD_SH_NM_DLVRY_METH_NM_H"), lit(" ")),
            coalesce(col("PROD_SH_NM_DESC_H"), lit(" ")),
            coalesce(col("SPIRA_BNF_ID_H"), lit(" "))
        )
    )
    .withColumn(
        "svCurr",
        concat_ws(
            ":",
            col("PROD_DNTL_LATE_WAIT_IN"),
            col("PROD_DRUG_COV_IN"),
            col("PROD_HLTH_COV_IN"),
            col("PROD_MNL_PRCS_IN"),
            col("PROD_MNTL_HLTH_COV_IN"),
            col("PROD_SHNM_MCARE_SUPLMT_COV_IN"),
            col("PROD_VSN_COV_IN"),
            col("EXPRNC_CAT_CD"),
            coalesce(col("EXPRNC_CAT_DESC"), lit(" ")),
            col("FNCL_LOB_CD"),
            coalesce(col("FNCL_LOB_DESC"), lit(" ")),
            coalesce(col("PROD_ABBR"), lit(" ")),
            col("PROD_DNTL_BILL_PFX_CD"),
            coalesce(col("PROD_DNTL_BILL_PFX_NM"), lit(" ")),
            coalesce(col("PROD_DESC"), lit(" ")),
            col("PROD_EFF_DT_SK"),
            coalesce(col("PROD_FCTS_CONV_RATE_PFX"), lit(" ")),
            coalesce(col("PROD_LOB_NO"), lit(" ")),
            col("PROD_LOB_CD"),
            coalesce(col("PROD_LOB_NM"), lit(" ")),
            coalesce(col("PROD_PCKG_CD"), lit(" ")),
            col("PROD_PCKG_DESC"),
            col("PROD_RATE_TYP_CD"),
            coalesce(col("PROD_RATE_TYP_NM"), lit(" ")),
            coalesce(col("PROD_ST_CD"), lit(" ")),
            coalesce(col("PROD_ST_NM"), lit(" ")),
            col("PROD_SUBPROD_CD"),
            coalesce(col("PROD_SUBPROD_NM"), lit(" ")),
            col("PROD_TERM_DT_SK"),
            col("PROD_SH_NM"),
            col("PROD_SH_NM_CAT_CD"),
            coalesce(col("PROD_SH_NM_CAT_NM"), lit(" ")),
            col("PROD_SH_NM_DLVRY_METH_CD"),
            coalesce(col("PROD_SH_NM_DLVRY_METH_NM"), lit(" ")),
            coalesce(col("PROD_SH_NM_DESC"), lit(" ")),
            col("SPIRA_BNF_ID")
        )
    )
    .withColumn("svChanged", when(col("svPrev") != col("svCurr"), lit("Y")).otherwise(lit("N")))
    .withColumn("svProdskHistNull", when(col("PROD_SK_HIST").isNull(), lit("Y")).otherwise(lit("N")))
)

df_xfrm_businesslogic_temp_2 = df_xfrm_businesslogic_temp.withColumn(
    "row_num_for_unk_na",
    row_number().over(Window.orderBy(lit(1)))
)

df_Lnk_Inst_Out = (
    df_xfrm_businesslogic_temp_2
    .filter(
        "((svChanged = 'Y' or svProdskHistNull = 'Y') and (PROD_SK <> 0 and PROD_SK <> 1))"
    )
    .select(
        col("PROD_SK").alias("PROD_SK"),
        lit(EDWRunCycleDate).alias("EDW_RCRD_STRT_DT_SK"),
        col("PROD_ID").alias("PROD_ID"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        lit("Y").alias("EDW_CUR_RCRD_IN"),
        col("PROD_DNTL_LATE_WAIT_IN").alias("PROD_DNTL_LATE_WAIT_IN"),
        col("PROD_DRUG_COV_IN").alias("PROD_DRUG_COV_IN"),
        col("PROD_HLTH_COV_IN").alias("PROD_HLTH_COV_IN"),
        col("PROD_MNL_PRCS_IN").alias("PROD_MNL_PRCS_IN"),
        col("PROD_MNTL_HLTH_COV_IN").alias("PROD_MNTL_HLTH_COV_IN"),
        col("PROD_SHNM_MCARE_SUPLMT_COV_IN").alias("PROD_SHNM_MCARE_SUPLMT_COV_IN"),
        col("PROD_VSN_COV_IN").alias("PROD_VSN_COV_IN"),
        lit("2199-12-31").alias("EDW_RCRD_END_DT_SK"),
        lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
        col("EXPRNC_CAT_DESC").alias("EXPRNC_CAT_DESC"),
        col("FNCL_LOB_CD").alias("FNCL_LOB_CD"),
        col("FNCL_LOB_DESC").alias("FNCL_LOB_DESC"),
        col("PROD_ABBR").alias("PROD_ABBR"),
        col("PROD_DNTL_BILL_PFX_CD").alias("PROD_DNTL_BILL_PFX_CD"),
        col("PROD_DNTL_BILL_PFX_NM").alias("PROD_DNTL_BILL_PFX_NM"),
        col("PROD_DESC").alias("PROD_DESC"),
        col("PROD_EFF_DT_SK").alias("PROD_EFF_DT_SK"),
        col("PROD_FCTS_CONV_RATE_PFX").alias("PROD_FCTS_CONV_RATE_PFX"),
        col("PROD_LOB_NO").alias("PROD_LOB_NO"),
        col("PROD_LOB_CD").alias("PROD_LOB_CD"),
        col("PROD_LOB_NM").alias("PROD_LOB_NM"),
        col("PROD_PCKG_CD").alias("PROD_PCKG_CD"),
        col("PROD_PCKG_DESC").alias("PROD_PCKG_DESC"),
        col("PROD_RATE_TYP_CD").alias("PROD_RATE_TYP_CD"),
        col("PROD_RATE_TYP_NM").alias("PROD_RATE_TYP_NM"),
        col("PROD_ST_CD").alias("PROD_ST_CD"),
        col("PROD_ST_NM").alias("PROD_ST_NM"),
        col("PROD_SUBPROD_CD").alias("PROD_SUBPROD_CD"),
        col("PROD_SUBPROD_NM").alias("PROD_SUBPROD_NM"),
        col("PROD_TERM_DT_SK").alias("PROD_TERM_DT_SK"),
        col("PROD_SH_NM").alias("PROD_SH_NM"),
        col("PROD_SH_NM_CAT_CD").alias("PROD_SH_NM_CAT_CD"),
        col("PROD_SH_NM_CAT_NM").alias("PROD_SH_NM_CAT_NM"),
        col("PROD_SH_NM_DLVRY_METH_CD").alias("PROD_SH_NM_DLVRY_METH_CD"),
        col("PROD_SH_NM_DLVRY_METH_NM").alias("PROD_SH_NM_DLVRY_METH_NM"),
        col("PROD_SH_NM_DESC").alias("PROD_SH_NM_DESC"),
        col("SPIRA_BNF_ID").alias("SPIRA_BNF_ID")
    )
)

df_Lnk_R_TxmfOut = (
    df_xfrm_businesslogic_temp_2
    .filter(
        "((svChanged = 'Y' and svProdskHistNull = 'N') and (PROD_SK <> 0 and PROD_SK <> 1))"
    )
    .select(
        col("PROD_SK").alias("PROD_SK"),
        date_format(
            date_sub(to_date(lit(EDWRunCycleDate), "yyyy-MM-dd"), 1),
            "yyyy-MM-dd"
        ).alias("EDW_RCRD_END_DT_SK"),
        lit("N").alias("EDW_CUR_RCRD_IN"),
        lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK")
    )
)

df_UNK_Row_temp = df_xfrm_businesslogic_temp_2.filter("row_num_for_unk_na = 1")
df_NA_Row_temp = df_xfrm_businesslogic_temp_2.filter("row_num_for_unk_na = 1")

df_UNK_Row = df_UNK_Row_temp.filter("(((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1)") .select(
    lit(0).alias("PROD_SK"),
    lit("UNK").alias("SRC_SYS_CD"),
    lit("UNK").alias("PROD_ID"),
    lit("N").alias("PROD_DNTL_LATE_WAIT_IN"),
    lit("N").alias("PROD_DRUG_COV_IN"),
    lit("N").alias("PROD_HLTH_COV_IN"),
    lit("N").alias("PROD_MNL_PRCS_IN"),
    lit("N").alias("PROD_MNTL_HLTH_COV_IN"),
    lit("N").alias("PROD_SHNM_MCARE_SUPLMT_COV_IN"),
    lit("N").alias("PROD_VSN_COV_IN"),
    lit("1753-01-01").alias("EDW_RCRD_STRT_DT_SK"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("1753-01-01").alias("EDW_RCRD_END_DT_SK"),
    lit("N").alias("EDW_CUR_RCRD_IN"),
    lit(None).alias("EXPRNC_CAT_CD"),
    lit(None).alias("EXPRNC_CAT_DESC"),
    lit(None).alias("FNCL_LOB_CD"),
    lit(None).alias("FNCL_LOB_DESC"),
    lit(None).alias("PROD_ABBR"),
    lit(None).alias("PROD_DNTL_BILL_PFX_CD"),
    lit(None).alias("PROD_DNTL_BILL_PFX_NM"),
    lit(None).alias("PROD_DESC"),
    lit(None).alias("PROD_EFF_DT_SK"),
    lit(None).alias("PROD_FCTS_CONV_RATE_PFX"),
    lit(None).alias("PROD_LOB_NO"),
    lit(None).alias("PROD_LOB_CD"),
    lit(None).alias("PROD_LOB_NM"),
    lit(None).alias("PROD_PCKG_CD"),
    lit(None).alias("PROD_PCKG_DESC"),
    lit(None).alias("PROD_RATE_TYP_CD"),
    lit(None).alias("PROD_RATE_TYP_NM"),
    lit(None).alias("PROD_ST_CD"),
    lit(None).alias("PROD_ST_NM"),
    lit(None).alias("PROD_SUBPROD_CD"),
    lit(None).alias("PROD_SUBPROD_NM"),
    lit(None).alias("PROD_TERM_DT_SK"),
    lit(None).alias("PROD_SH_NM"),
    lit(None).alias("PROD_SH_NM_CAT_CD"),
    lit(None).alias("PROD_SH_NM_CAT_NM"),
    lit(None).alias("PROD_SH_NM_DLVRY_METH_CD"),
    lit(None).alias("PROD_SH_NM_DLVRY_METH_NM"),
    lit(None).alias("PROD_SH_NM_DESC"),
    lit(None).alias("SPIRA_BNF_ID")
)

df_NA_Row = df_NA_Row_temp.filter("(((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1)") .select(
    lit(1).alias("PROD_SK"),
    lit("NA").alias("SRC_SYS_CD"),
    lit("NA").alias("PROD_ID"),
    lit("N").alias("PROD_DNTL_LATE_WAIT_IN"),
    lit("N").alias("PROD_DRUG_COV_IN"),
    lit("N").alias("PROD_HLTH_COV_IN"),
    lit("N").alias("PROD_MNL_PRCS_IN"),
    lit("N").alias("PROD_MNTL_HLTH_COV_IN"),
    lit("N").alias("PROD_SHNM_MCARE_SUPLMT_COV_IN"),
    lit("N").alias("PROD_VSN_COV_IN"),
    lit("1753-01-01").alias("EDW_RCRD_STRT_DT_SK"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("EDW_RCRD_END_DT_SK"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("N").alias("EDW_CUR_RCRD_IN"),
    lit(None).alias("EXPRNC_CAT_CD"),
    lit(None).alias("EXPRNC_CAT_DESC"),
    lit(None).alias("FNCL_LOB_CD"),
    lit(None).alias("FNCL_LOB_DESC"),
    lit(None).alias("PROD_ABBR"),
    lit(None).alias("PROD_DNTL_BILL_PFX_CD"),
    lit(None).alias("PROD_DNTL_BILL_PFX_NM"),
    lit(None).alias("PROD_DESC"),
    lit(None).alias("PROD_EFF_DT_SK"),
    lit(None).alias("PROD_FCTS_CONV_RATE_PFX"),
    lit(None).alias("PROD_LOB_NO"),
    lit(None).alias("PROD_LOB_CD"),
    lit(None).alias("PROD_LOB_NM"),
    lit(None).alias("PROD_PCKG_CD"),
    lit(None).alias("PROD_PCKG_DESC"),
    lit(None).alias("PROD_RATE_TYP_CD"),
    lit(None).alias("PROD_RATE_TYP_NM"),
    lit(None).alias("PROD_ST_CD"),
    lit(None).alias("PROD_ST_NM"),
    lit(None).alias("PROD_SUBPROD_CD"),
    lit(None).alias("PROD_SUBPROD_NM"),
    lit(None).alias("PROD_TERM_DT_SK"),
    lit(None).alias("PROD_SH_NM"),
    lit(None).alias("PROD_SH_NM_CAT_CD"),
    lit(None).alias("PROD_SH_NM_CAT_NM"),
    lit(None).alias("PROD_SH_NM_DLVRY_METH_CD"),
    lit(None).alias("PROD_SH_NM_DLVRY_METH_NM"),
    lit(None).alias("PROD_SH_NM_DESC"),
    lit(None).alias("SPIRA_BNF_ID")
)

df_Jn_Updt = df_Lnk_R_TxmfOut.alias("Lnk_R_TxmfOut").join(
    df_Lnk_L_ProdHistD_In.alias("Lnk_L_ProdHistD_In"),
    on=[col("Lnk_R_TxmfOut.PROD_SK") == col("Lnk_L_ProdHistD_In.PROD_SK")],
    how="inner"
)

df_Lnk_Updt_Out = df_Jn_Updt.select(
    col("Lnk_R_TxmfOut.PROD_SK").alias("PROD_SK"),
    col("Lnk_R_TxmfOut.EDW_RCRD_END_DT_SK").alias("EDW_RCRD_END_DT_SK"),
    col("Lnk_R_TxmfOut.EDW_CUR_RCRD_IN").alias("EDW_CUR_RCRD_IN"),
    col("Lnk_R_TxmfOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Lnk_R_TxmfOut.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("Lnk_L_ProdHistD_In.PROD_ID").alias("PROD_ID"),
    col("Lnk_L_ProdHistD_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Lnk_L_ProdHistD_In.EDW_RCRD_STRT_DT_SK").alias("EDW_RCRD_STRT_DT_SK"),
    col("Lnk_L_ProdHistD_In.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("Lnk_L_ProdHistD_In.PROD_DNTL_LATE_WAIT_IN").alias("PROD_DNTL_LATE_WAIT_IN"),
    col("Lnk_L_ProdHistD_In.PROD_DRUG_COV_IN").alias("PROD_DRUG_COV_IN"),
    col("Lnk_L_ProdHistD_In.PROD_HLTH_COV_IN").alias("PROD_HLTH_COV_IN"),
    col("Lnk_L_ProdHistD_In.PROD_MNL_PRCS_IN").alias("PROD_MNL_PRCS_IN"),
    col("Lnk_L_ProdHistD_In.PROD_MNTL_HLTH_COV_IN").alias("PROD_MNTL_HLTH_COV_IN"),
    col("Lnk_L_ProdHistD_In.PROD_SHNM_MCARE_SUPLMT_COV_IN").alias("PROD_SHNM_MCARE_SUPLMT_COV_IN"),
    col("Lnk_L_ProdHistD_In.PROD_VSN_COV_IN").alias("PROD_VSN_COV_IN"),
    col("Lnk_L_ProdHistD_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Lnk_L_ProdHistD_In.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
    col("Lnk_L_ProdHistD_In.EXPRNC_CAT_DESC").alias("EXPRNC_CAT_DESC"),
    col("Lnk_L_ProdHistD_In.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    col("Lnk_L_ProdHistD_In.FNCL_LOB_DESC").alias("FNCL_LOB_DESC"),
    col("Lnk_L_ProdHistD_In.PROD_ABBR").alias("PROD_ABBR"),
    col("Lnk_L_ProdHistD_In.PROD_DNTL_BILL_PFX_CD").alias("PROD_DNTL_BILL_PFX_CD"),
    col("Lnk_L_ProdHistD_In.PROD_DNTL_BILL_PFX_NM").alias("PROD_DNTL_BILL_PFX_NM"),
    col("Lnk_L_ProdHistD_In.PROD_DESC").alias("PROD_DESC"),
    col("Lnk_L_ProdHistD_In.PROD_EFF_DT_SK").alias("PROD_EFF_DT_SK"),
    col("Lnk_L_ProdHistD_In.PROD_FCTS_CONV_RATE_PFX").alias("PROD_FCTS_CONV_RATE_PFX"),
    col("Lnk_L_ProdHistD_In.PROD_LOB_NO").alias("PROD_LOB_NO"),
    col("Lnk_L_ProdHistD_In.PROD_LOB_CD").alias("PROD_LOB_CD"),
    col("Lnk_L_ProdHistD_In.PROD_LOB_NM").alias("PROD_LOB_NM"),
    col("Lnk_L_ProdHistD_In.PROD_Pckg_Cd").alias("PROD_PCKG_CD"),
    col("Lnk_L_ProdHistD_In.PROD_PCKG_DESC").alias("PROD_PCKG_DESC"),
    col("Lnk_L_ProdHistD_In.PROD_RATE_TYP_CD").alias("PROD_RATE_TYP_CD"),
    col("Lnk_L_ProdHistD_In.PROD_RATE_TYP_NM").alias("PROD_RATE_TYP_NM"),
    col("Lnk_L_ProdHistD_In.PROD_ST_CD").alias("PROD_ST_CD"),
    col("Lnk_L_ProdHistD_In.PROD_ST_NM").alias("PROD_ST_NM"),
    col("Lnk_L_ProdHistD_In.PROD_SUBPROD_CD").alias("PROD_SUBPROD_CD"),
    col("Lnk_L_ProdHistD_In.PROD_SUBPROD_NM").alias("PROD_SUBPROD_NM"),
    col("Lnk_L_ProdHistD_In.PROD_TERM_DT_SK").alias("PROD_TERM_DT_SK"),
    col("Lnk_L_ProdHistD_In.PROD_SH_NM").alias("PROD_SH_NM"),
    col("Lnk_L_ProdHistD_In.PROD_SH_NM_CAT_CD").alias("PROD_SH_NM_CAT_CD"),
    col("Lnk_L_ProdHistD_In.PROD_SH_NM_CAT_NM").alias("PROD_SH_NM_CAT_NM"),
    col("Lnk_L_ProdHistD_In.PROD_SH_NM_DLVRY_METH_CD").alias("PROD_SH_NM_DLVRY_METH_CD"),
    col("Lnk_L_ProdHistD_In.PROD_SH_NM_DLVRY_METH_NM").alias("PROD_SH_NM_DLVRY_METH_NM"),
    col("Lnk_L_ProdHistD_In.PROD_SH_NM_DESC").alias("PROD_SH_NM_DESC"),
    col("Lnk_L_ProdHistD_In.SPIRA_BNF_ID").alias("SPIRA_BNF_ID")
)

df_Fnl_Update_Insert = (
    df_Lnk_Updt_Out.unionByName(df_Lnk_Inst_Out)
    .unionByName(df_UNK_Row)
    .unionByName(df_NA_Row)
)

df_Seq_PROD_HIST_D = df_Fnl_Update_Insert.select(
    col("PROD_SK"),
    col("EDW_RCRD_STRT_DT_SK"),
    col("PROD_ID"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("EDW_CUR_RCRD_IN"),
    col("PROD_DNTL_LATE_WAIT_IN"),
    col("PROD_DRUG_COV_IN"),
    col("PROD_HLTH_COV_IN"),
    col("PROD_MNL_PRCS_IN"),
    col("PROD_MNTL_HLTH_COV_IN"),
    col("PROD_SHNM_MCARE_SUPLMT_COV_IN"),
    col("PROD_VSN_COV_IN"),
    col("EDW_RCRD_END_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("EXPRNC_CAT_CD"),
    col("EXPRNC_CAT_DESC"),
    col("FNCL_LOB_CD"),
    col("FNCL_LOB_DESC"),
    col("PROD_ABBR"),
    col("PROD_DNTL_BILL_PFX_CD"),
    col("PROD_DNTL_BILL_PFX_NM"),
    col("PROD_DESC"),
    col("PROD_EFF_DT_SK"),
    col("PROD_FCTS_CONV_RATE_PFX"),
    col("PROD_LOB_NO"),
    col("PROD_LOB_CD"),
    col("PROD_LOB_NM"),
    col("PROD_PCKG_CD"),
    col("PROD_PCKG_DESC"),
    col("PROD_RATE_TYP_CD"),
    col("PROD_RATE_TYP_NM"),
    col("PROD_ST_CD"),
    col("PROD_ST_NM"),
    col("PROD_SUBPROD_CD"),
    col("PROD_SUBPROD_NM"),
    col("PROD_TERM_DT_SK"),
    col("PROD_SH_NM"),
    col("PROD_SH_NM_CAT_CD"),
    col("PROD_SH_NM_CAT_NM"),
    col("PROD_SH_NM_DLVRY_METH_CD"),
    col("PROD_SH_NM_DLVRY_METH_NM"),
    col("PROD_SH_NM_DESC"),
    col("SPIRA_BNF_ID")
)

df_Seq_PROD_HIST_D_rpad = (
    df_Seq_PROD_HIST_D
    .withColumn("EDW_RCRD_STRT_DT_SK", rpad(col("EDW_RCRD_STRT_DT_SK"), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("EDW_CUR_RCRD_IN", rpad(col("EDW_CUR_RCRD_IN"), 1, " "))
    .withColumn("PROD_DNTL_LATE_WAIT_IN", rpad(col("PROD_DNTL_LATE_WAIT_IN"), 1, " "))
    .withColumn("PROD_DRUG_COV_IN", rpad(col("PROD_DRUG_COV_IN"), 1, " "))
    .withColumn("PROD_HLTH_COV_IN", rpad(col("PROD_HLTH_COV_IN"), 1, " "))
    .withColumn("PROD_MNL_PRCS_IN", rpad(col("PROD_MNL_PRCS_IN"), 1, " "))
    .withColumn("PROD_MNTL_HLTH_COV_IN", rpad(col("PROD_MNTL_HLTH_COV_IN"), 1, " "))
    .withColumn("PROD_SHNM_MCARE_SUPLMT_COV_IN", rpad(col("PROD_SHNM_MCARE_SUPLMT_COV_IN"), 1, " "))
    .withColumn("PROD_VSN_COV_IN", rpad(col("PROD_VSN_COV_IN"), 1, " "))
    .withColumn("EDW_RCRD_END_DT_SK", rpad(col("EDW_RCRD_END_DT_SK"), 10, " "))
    .withColumn("EXPRNC_CAT_CD", col("EXPRNC_CAT_CD"))
    .withColumn("EXPRNC_CAT_DESC", col("EXPRNC_CAT_DESC"))
    .withColumn("FNCL_LOB_CD", col("FNCL_LOB_CD"))
    .withColumn("FNCL_LOB_DESC", col("FNCL_LOB_DESC"))
    .withColumn("PROD_ABBR", col("PROD_ABBR"))
    .withColumn("PROD_DNTL_BILL_PFX_CD", col("PROD_DNTL_BILL_PFX_CD"))
    .withColumn("PROD_DNTL_BILL_PFX_NM", col("PROD_DNTL_BILL_PFX_NM"))
    .withColumn("PROD_DESC", col("PROD_DESC"))
    .withColumn("PROD_EFF_DT_SK", rpad(col("PROD_EFF_DT_SK"), 10, " "))
    .withColumn("PROD_FCTS_CONV_RATE_PFX", col("PROD_FCTS_CONV_RATE_PFX"))
    .withColumn("PROD_LOB_NO", col("PROD_LOB_NO"))
    .withColumn("PROD_LOB_CD", col("PROD_LOB_CD"))
    .withColumn("PROD_LOB_NM", col("PROD_LOB_NM"))
    .withColumn("PROD_PCKG_CD", col("PROD_PCKG_CD"))
    .withColumn("PROD_PCKG_DESC", col("PROD_PCKG_DESC"))
    .withColumn("PROD_RATE_TYP_CD", col("PROD_RATE_TYP_CD"))
    .withColumn("PROD_RATE_TYP_NM", col("PROD_RATE_TYP_NM"))
    .withColumn("PROD_ST_CD", col("PROD_ST_CD"))
    .withColumn("PROD_ST_NM", col("PROD_ST_NM"))
    .withColumn("PROD_SUBPROD_CD", col("PROD_SUBPROD_CD"))
    .withColumn("PROD_SUBPROD_NM", col("PROD_SUBPROD_NM"))
    .withColumn("PROD_TERM_DT_SK", rpad(col("PROD_TERM_DT_SK"), 10, " "))
    .withColumn("PROD_SH_NM", col("PROD_SH_NM"))
    .withColumn("PROD_SH_NM_CAT_CD", col("PROD_SH_NM_CAT_CD"))
    .withColumn("PROD_SH_NM_CAT_NM", col("PROD_SH_NM_CAT_NM"))
    .withColumn("PROD_SH_NM_DLVRY_METH_CD", col("PROD_SH_NM_DLVRY_METH_CD"))
    .withColumn("PROD_SH_NM_DLVRY_METH_NM", col("PROD_SH_NM_DLVRY_METH_NM"))
    .withColumn("PROD_SH_NM_DESC", col("PROD_SH_NM_DESC"))
    .withColumn("SPIRA_BNF_ID", col("SPIRA_BNF_ID"))
)

write_files(
    df_Seq_PROD_HIST_D_rpad,
    f"{adls_path}/load/PROD_HIST_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)