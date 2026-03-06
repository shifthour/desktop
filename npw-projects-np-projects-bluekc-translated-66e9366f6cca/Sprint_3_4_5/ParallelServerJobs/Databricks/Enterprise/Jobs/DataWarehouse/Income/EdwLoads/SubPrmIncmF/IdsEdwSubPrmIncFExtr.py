# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Aditya   Raju              08/22/2013               5114                              Create Load File for EDW Table FEE_DSCNT_D                         EnterpriseWrhsDevl       Jag Yelavarthi         2013-12-11

# MAGIC Write SUM_PRM_INCM_F Data into a Sequential file for Load Job IdsEdwSumPrmIncmFLoad.
# MAGIC Read all the Data from IDS INVC_SUB Table and join Codes Mapping table to Bring SRC_SYS_CD; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsEdwSubPrmIncmFExtr
# MAGIC This Job will extract records from INVC_SUB from IDS and applies code Denormalization
# MAGIC Lookup Keys
# MAGIC 1) INVC_SUB_PRM_TYP_CD_SK
# MAGIC 2) INVC_SUB_BILL_DISP_CD_SK
# MAGIC 3 INVC_SUB_FMLY_CNTR_CD_SK
# MAGIC Write B_SUM_PRM_INCM_F Data into a Sequential file for Load Job IdsEdwBSumPrmIncmFLoad.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve job parameters
CurrRunycle = get_widget_value('CurrRunycle','')
BeginCycle = get_widget_value('BeginCycle','')
EdwRuncycleDate = get_widget_value('EdwRuncycleDate','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Stage: db2_SUB_PRM_INC_in (DB2ConnectorPX)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
sql_db2_SUB_PRM_INC_in = f"""
SELECT 
    INVC_SUB.INVC_SUB_SK,
    COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
    INVC_SUB.BILL_INVC_ID,
    INVC_SUB.SUB_UNIQ_KEY,
    INVC_SUB.CLS_PLN_ID,
    INVC_SUB.PROD_ID,
    INVC_SUB.PROD_BILL_CMPNT_ID,
    INVC_SUB.COV_DUE_DT_SK,
    INVC_SUB.COV_STRT_DT_SK,
    INVC_SUB.INVC_SUB_PRM_TYP_CD_SK,
    INVC_SUB.CRT_TS,
    INVC_SUB.INVC_SUB_BILL_DISP_CD_SK,
    INVC_SUB.CLS_SK,
    INVC_SUB.CLS_PLN_SK,
    INVC_SUB.GRP_SK,
    INVC_SUB.INVC_SK,
    INVC_SUB.PROD_SK,
    INVC_SUB.SUBGRP_SK,
    INVC_SUB.SUB_SK,
    INVC_SUB.INVC_SUB_FMLY_CNTR_CD_SK,
    INVC_SUB.DPNDT_PRM_AMT,
    INVC_SUB.SUB_PRM_AMT,
    INVC_SUB.DPNDT_CT,
    INVC_SUB.SUB_CT,
    INVC.INVC_TYP_CD_SK,
    INVC.BILL_DUE_DT_SK,
    INVC.BILL_END_DT_SK,
    INVC.CRT_DT_SK AS INVC_CRT_DT_SK,
    INVC_SUB.COV_END_DT_SK,
    INVC.BILL_ENTY_SK,
    INVC_SUB.VOL_COV_AMT,
    INVC.CUR_RCRD_IN,
    GRP.GRP_ID,
    CLS.CLS_ID,
    SUBGRP.SUBGRP_ID
FROM {IDSOwner}.INVC_SUB INVC_SUB
LEFT JOIN {IDSOwner}.CD_MPPNG CD
  ON INVC_SUB.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,
     {IDSOwner}.INVC INVC,
     {IDSOwner}.CLS CLS,
     {IDSOwner}.GRP GRP,
     {IDSOwner}.SUBGRP SUBGRP
WHERE
    INVC_SUB.INVC_SK = INVC.INVC_SK 
    AND INVC_SUB.CLS_SK = CLS.CLS_SK
    AND INVC_SUB.GRP_SK = GRP.GRP_SK
    AND INVC_SUB.SUBGRP_SK = SUBGRP.SUBGRP_SK
    AND INVC_SUB.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BeginCycle}
"""
df_db2_SUB_PRM_INC_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_db2_SUB_PRM_INC_in)
    .load()
)

# Stage: db2_CD_MPPNG_Extr (DB2ConnectorPX)
sql_db2_CD_MPPNG_Extr = f"""
SELECT
    CD_MPPNG_SK,
    COALESCE(TRGT_CD,'UNK') TRGT_CD,
    COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_db2_CD_MPPNG_Extr)
    .load()
)

# Stage: cpy_cd_mppng (PxCopy) - produce 4 output dataframes
df_cpy_cd_mppng_Ref_BillDispCd_Lkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_Ref_PrmTypCd_Lkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_Ref_InvcTypCd_Lkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_Ref_FmlyCntrCd_Lkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# Stage: lkp_Codes (PxLookup) - chain left joins
df_lkp_Codes = (
    df_db2_SUB_PRM_INC_in.alias("lnk_IdsEdwSubPrmIncFExtr_InABC")
    .join(
        df_cpy_cd_mppng_Ref_InvcTypCd_Lkup.alias("Ref_InvcTypCd_Lkup"),
        F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.INVC_TYP_CD_SK") == F.col("Ref_InvcTypCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng_Ref_FmlyCntrCd_Lkup.alias("Ref_FmlyCntrCd_Lkup"),
        F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.INVC_SUB_FMLY_CNTR_CD_SK") == F.col("Ref_FmlyCntrCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng_Ref_BillDispCd_Lkup.alias("Ref_BillDispCd_Lkup"),
        F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.INVC_SUB_BILL_DISP_CD_SK") == F.col("Ref_BillDispCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng_Ref_PrmTypCd_Lkup.alias("Ref_PrmTypCd_Lkup"),
        F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.INVC_SUB_PRM_TYP_CD_SK") == F.col("Ref_PrmTypCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
)

df_lkp_Codes = df_lkp_Codes.select(
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.INVC_SUB_SK").alias("INVC_SUB_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.PROD_ID").alias("PROD_ID"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.COV_DUE_DT_SK").alias("INVC_SUB_COV_DUE_DT_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.COV_STRT_DT_SK").alias("INVC_SUB_COV_STRT_DT_SK"),
    F.col("Ref_PrmTypCd_Lkup.TRGT_CD").alias("INVC_SUB_PRM_TYP_CD"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.CRT_TS").alias("INVC_SUB_CRT_DTM"),
    F.col("Ref_BillDispCd_Lkup.TRGT_CD").alias("INVC_SUB_BILL_DISP_CD"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.CLS_SK").alias("CLS_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.GRP_SK").alias("GRP_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.PROD_SK").alias("PROD_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.CUR_RCRD_IN").alias("INVC_CUR_RCRD_IN"),
    F.col("Ref_FmlyCntrCd_Lkup.TRGT_CD").alias("INVC_SUB_FMLY_CNTR_CD"),
    F.col("Ref_InvcTypCd_Lkup.TRGT_CD").alias("INVC_TYP_CD"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.BILL_DUE_DT_SK").alias("INVC_BILL_DUE_DT_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.BILL_END_DT_SK").alias("INVC_BILL_END_DT_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.INVC_CRT_DT_SK").alias("INVC_CRT_DT_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.COV_END_DT_SK").alias("INVC_SUB_COV_END_DT_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.DPNDT_PRM_AMT").alias("INVC_SUB_DPNDT_PRM_AMT"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.SUB_PRM_AMT").alias("INVC_SUB_SUB_PRM_AMT"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.VOL_COV_AMT").alias("INVC_SUB_VOL_COV_AMT"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.DPNDT_CT").alias("INVC_SUB_DPNDT_CT"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.SUB_CT").alias("INVC_SUB_SUB_CT"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.CLS_ID").alias("CLS_ID"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.GRP_ID").alias("GRP_ID"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.INVC_SK").alias("INVC_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.INVC_SUB_BILL_DISP_CD_SK").alias("INVC_SUB_BILL_DISP_CD_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.INVC_SUB_FMLY_CNTR_CD_SK").alias("INVC_SUB_FMLY_CNTR_CD_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.INVC_SUB_PRM_TYP_CD_SK").alias("INVC_SUB_PRM_TYP_CD_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.INVC_TYP_CD_SK").alias("INVC_TYP_CD_SK"),
    F.col("lnk_IdsEdwSubPrmIncFExtr_InABC.SUB_SK").alias("SUB_SK")
)

# Stage: xfrm_BusinessLogic (CTransformerStage)

# 1) Primary output link: lnk_IdsEdwSubPrmIncFMain_Out => filter => (INVC_SUB_SK <> 0 AND INVC_SUB_SK <> 1)
df_main_filtered = df_lkp_Codes.filter((F.col("INVC_SUB_SK") != 0) & (F.col("INVC_SUB_SK") != 1))
df_main = df_main_filtered.select(
    F.col("INVC_SUB_SK").alias("SUB_PRM_INCM_SK"),
    F.when(F.trim(F.col("SRC_SYS_CD")) == "", "UNK").otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("INVC_SUB_COV_DUE_DT_SK").alias("INVC_SUB_COV_DUE_DT_SK"),
    F.col("INVC_SUB_COV_STRT_DT_SK").alias("INVC_SUB_COV_STRT_DT_SK"),
    F.col("INVC_SUB_PRM_TYP_CD").alias("INVC_SUB_PRM_TYP_CD"),
    F.col("INVC_SUB_CRT_DTM").alias("INVC_SUB_CRT_DTM"),
    F.when(F.trim(F.col("INVC_SUB_BILL_DISP_CD")) == "", "UNK").otherwise(F.col("INVC_SUB_BILL_DISP_CD")).alias("INVC_SUB_BILL_DISP_CD"),
    F.lit(EdwRuncycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EdwRuncycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("CLS_SK").alias("CLS_SK"),
    F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("INVC_CUR_RCRD_IN").alias("INVC_CUR_RCRD_IN"),
    F.when(F.trim(F.col("INVC_SUB_FMLY_CNTR_CD")) == "", "UNK").otherwise(F.col("INVC_SUB_FMLY_CNTR_CD")).alias("INVC_SUB_FMLY_CNTR_CD"),
    F.when(F.trim(F.col("INVC_TYP_CD")) == "", "UNK").otherwise(F.col("INVC_TYP_CD")).alias("INVC_TYP_CD"),
    F.col("INVC_BILL_DUE_DT_SK").alias("INVC_BILL_DUE_DT_SK"),
    F.col("INVC_BILL_END_DT_SK").alias("INVC_BILL_END_DT_SK"),
    F.col("INVC_CRT_DT_SK").alias("INVC_CRT_DT_SK"),
    F.col("INVC_SUB_COV_END_DT_SK").alias("INVC_SUB_COV_END_DT_SK"),
    F.col("INVC_SUB_DPNDT_PRM_AMT").alias("INVC_SUB_DPNDT_PRM_AMT"),
    F.col("INVC_SUB_SUB_PRM_AMT").alias("INVC_SUB_SUB_PRM_AMT"),
    F.col("INVC_SUB_VOL_COV_AMT").alias("INVC_SUB_VOL_COV_AMT"),
    F.col("INVC_SUB_DPNDT_CT").alias("INVC_SUB_DPNDT_CT"),
    F.col("INVC_SUB_SUB_CT").alias("INVC_SUB_SUB_CT"),
    F.col("CLS_ID").alias("CLS_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.lit(CurrRunycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("INVC_SK").alias("INVC_SK"),
    F.col("INVC_SUB_BILL_DISP_CD_SK").alias("INVC_SUB_BILL_DISP_CD_SK"),
    F.col("INVC_SUB_FMLY_CNTR_CD_SK").alias("INVC_SUB_FMLY_CNTR_CD_SK"),
    F.col("INVC_SUB_PRM_TYP_CD_SK").alias("INVC_SUB_PRM_TYP_CD_SK"),
    F.col("INVC_TYP_CD_SK").alias("INVC_TYP_CD_SK"),
    F.concat(F.col("INVC_BILL_DUE_DT_SK").substr(F.lit(1), F.lit(4)),
             F.col("INVC_BILL_DUE_DT_SK").substr(F.lit(6), F.lit(2))).alias("INVC_BILL_DUE_YR_MO_SK"),
    F.concat(F.col("INVC_BILL_END_DT_SK").substr(F.lit(1), F.lit(4)),
             F.col("INVC_BILL_END_DT_SK").substr(F.lit(6), F.lit(2))).alias("INVC_BILL_END_YR_MO_SK"),
    F.col("SUB_SK").alias("SUB_SK")
)

# 2) lnk_UNK => single row with literal values
# Create a one-row DataFrame with those columns
df_unk = spark.createDataFrame(
    [(0,
      'UNK',
      'UNK',
      0,
      'UNK',
      'UNK',
      'UNK',
      '1753-01-01',
      '1753-01-01',
      'UNK',
      '1753-01-01 00:00:00.000000',
      'UNK',
      '1753-01-01',
      '1753-01-01',
      0,
      0,
      0,
      0,
      0,
      0,
      'N',
      'UNK',
      'UNK',
      '1753-01-01',
      '1753-01-01',
      '1753-01-01',
      '1753-01-01',
      0,
      0,
      0,
      0,
      0,
      'UNK',
      'UNK',
      'UNK',
      100,
      100,
      0,
      0,
      0,
      0,
      0,
      '175301',
      '175301',
      0)],
    [
      "SUB_PRM_INCM_SK",
      "SRC_SYS_CD",
      "BILL_INVC_ID",
      "SUB_UNIQ_KEY",
      "CLS_PLN_ID",
      "PROD_ID",
      "PROD_BILL_CMPNT_ID",
      "INVC_SUB_COV_DUE_DT_SK",
      "INVC_SUB_COV_STRT_DT_SK",
      "INVC_SUB_PRM_TYP_CD",
      "INVC_SUB_CRT_DTM",
      "INVC_SUB_BILL_DISP_CD",
      "CRT_RUN_CYC_EXCTN_DT_SK",
      "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
      "BILL_ENTY_SK",
      "CLS_SK",
      "CLS_PLN_SK",
      "GRP_SK",
      "PROD_SK",
      "SUBGRP_SK",
      "INVC_CUR_RCRD_IN",
      "INVC_SUB_FMLY_CNTR_CD",
      "INVC_TYP_CD",
      "INVC_BILL_DUE_DT_SK",
      "INVC_BILL_END_DT_SK",
      "INVC_CRT_DT_SK",
      "INVC_SUB_COV_END_DT_SK",
      "INVC_SUB_DPNDT_PRM_AMT",
      "INVC_SUB_SUB_PRM_AMT",
      "INVC_SUB_VOL_COV_AMT",
      "INVC_SUB_DPNDT_CT",
      "INVC_SUB_SUB_CT",
      "CLS_ID",
      "GRP_ID",
      "SUBGRP_ID",
      "CRT_RUN_CYC_EXCTN_SK",
      "LAST_UPDT_RUN_CYC_EXCTN_SK",
      "INVC_SK",
      "INVC_SUB_BILL_DISP_CD_SK",
      "INVC_SUB_FMLY_CNTR_CD_SK",
      "INVC_SUB_PRM_TYP_CD_SK",
      "INVC_TYP_CD_SK",
      "INVC_BILL_DUE_YR_MO_SK",
      "INVC_BILL_END_YR_MO_SK",
      "SUB_SK"
    ]
)

# 3) lnk_NA => single row with literal values
df_na = spark.createDataFrame(
    [(1,
      'NA',
      'NA',
      1,
      'NA',
      'NA',
      'NA',
      '1753-01-01',
      '1753-01-01',
      'NA',
      '1753-01-01 00:00:00.000000',
      'NA',
      '1753-01-01',
      '1753-01-01',
      1,
      1,
      1,
      1,
      1,
      1,
      'N',
      'NA',
      'NA',
      '1753-01-01',
      '1753-01-01',
      '1753-01-01',
      '1753-01-01',
      0,
      0,
      0,
      0,
      0,
      'NA',
      'NA',
      'NA',
      100,
      100,
      1,
      1,
      1,
      1,
      1,
      '175301',
      '175301',
      1)],
    [
      "SUB_PRM_INCM_SK",
      "SRC_SYS_CD",
      "BILL_INVC_ID",
      "SUB_UNIQ_KEY",
      "CLS_PLN_ID",
      "PROD_ID",
      "PROD_BILL_CMPNT_ID",
      "INVC_SUB_COV_DUE_DT_SK",
      "INVC_SUB_COV_STRT_DT_SK",
      "INVC_SUB_PRM_TYP_CD",
      "INVC_SUB_CRT_DTM",
      "INVC_SUB_BILL_DISP_CD",
      "CRT_RUN_CYC_EXCTN_DT_SK",
      "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
      "BILL_ENTY_SK",
      "CLS_SK",
      "CLS_PLN_SK",
      "GRP_SK",
      "PROD_SK",
      "SUBGRP_SK",
      "INVC_CUR_RCRD_IN",
      "INVC_SUB_FMLY_CNTR_CD",
      "INVC_TYP_CD",
      "INVC_BILL_DUE_DT_SK",
      "INVC_BILL_END_DT_SK",
      "INVC_CRT_DT_SK",
      "INVC_SUB_COV_END_DT_SK",
      "INVC_SUB_DPNDT_PRM_AMT",
      "INVC_SUB_SUB_PRM_AMT",
      "INVC_SUB_VOL_COV_AMT",
      "INVC_SUB_DPNDT_CT",
      "INVC_SUB_SUB_CT",
      "CLS_ID",
      "GRP_ID",
      "SUBGRP_ID",
      "CRT_RUN_CYC_EXCTN_SK",
      "LAST_UPDT_RUN_CYC_EXCTN_SK",
      "INVC_SK",
      "INVC_SUB_BILL_DISP_CD_SK",
      "INVC_SUB_FMLY_CNTR_CD_SK",
      "INVC_SUB_PRM_TYP_CD_SK",
      "INVC_TYP_CD_SK",
      "INVC_BILL_DUE_YR_MO_SK",
      "INVC_BILL_END_YR_MO_SK",
      "SUB_SK"
    ]
)

# Stage: fnl_UNK_NA (PxFunnel) => union the three dataframes
common_cols = df_main.columns
df_fnl_UNK_NA = df_main.select(common_cols).unionByName(df_unk.select(common_cols)).unionByName(df_na.select(common_cols))

# Stage: cpy_Main_Data (PxCopy) => produce two output flows

# 1) lnk_IdsEdwBSubPrmIncFExtr_OutABC => has these columns:
df_cpy_Main_Data_out_1 = df_fnl_UNK_NA.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.rpad(F.col("INVC_SUB_COV_DUE_DT_SK"), 10, " ").alias("INVC_SUB_COV_DUE_DT_SK"),
    F.rpad(F.col("INVC_SUB_COV_STRT_DT_SK"), 10, " ").alias("INVC_SUB_COV_STRT_DT_SK"),
    F.col("INVC_SUB_PRM_TYP_CD").alias("INVC_SUB_PRM_TYP_CD"),
    F.col("INVC_SUB_CRT_DTM").alias("INVC_SUB_CRTTM"),
    F.col("INVC_SUB_BILL_DISP_CD").alias("INVC_SUB_BILL_DISP_CD"),
    F.col("INVC_SUB_SUB_PRM_AMT").alias("INVC_SUB_SUB_PRM_AMT")
)

# 2) lnk_IdsEdwSubPrmIncFExtr_OutABC => has these columns:
df_cpy_Main_Data_out_2 = df_fnl_UNK_NA.select(
    F.col("SUB_PRM_INCM_SK").alias("SUB_PRM_INCM_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.rpad(F.col("INVC_SUB_COV_DUE_DT_SK"), 10, " ").alias("INVC_SUB_COV_DUE_DT_SK"),
    F.rpad(F.col("INVC_SUB_COV_STRT_DT_SK"), 10, " ").alias("INVC_SUB_COV_STRT_DT_SK"),
    F.col("INVC_SUB_PRM_TYP_CD").alias("INVC_SUB_PRM_TYP_CD"),
    F.col("INVC_SUB_CRT_DTM").alias("INVC_SUB_CRT_DTM"),
    F.col("INVC_SUB_BILL_DISP_CD").alias("INVC_SUB_BILL_DISP_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("CLS_SK").alias("CLS_SK"),
    F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.rpad(F.col("INVC_CUR_RCRD_IN"), 1, " ").alias("INVC_CUR_RCRD_IN"),
    F.col("INVC_SUB_FMLY_CNTR_CD").alias("INVC_SUB_FMLY_CNTR_CD"),
    F.col("INVC_TYP_CD").alias("INVC_TYP_CD"),
    F.rpad(F.col("INVC_BILL_DUE_DT_SK"), 10, " ").alias("INVC_BILL_DUE_DT_SK"),
    F.rpad(F.col("INVC_BILL_END_DT_SK"), 10, " ").alias("INVC_BILL_END_DT_SK"),
    F.rpad(F.col("INVC_CRT_DT_SK"), 10, " ").alias("INVC_CRT_DT_SK"),
    F.rpad(F.col("INVC_SUB_COV_END_DT_SK"), 10, " ").alias("INVC_SUB_COV_END_DT_SK"),
    F.col("INVC_SUB_DPNDT_PRM_AMT").alias("INVC_SUB_DPNDT_PRM_AMT"),
    F.col("INVC_SUB_SUB_PRM_AMT").alias("INVC_SUB_SUB_PRM_AMT"),
    F.col("INVC_SUB_VOL_COV_AMT").alias("INVC_SUB_VOL_COV_AMT"),
    F.col("INVC_SUB_DPNDT_CT").alias("INVC_SUB_DPNDT_CT"),
    F.col("INVC_SUB_SUB_CT").alias("INVC_SUB_SUB_CT"),
    F.col("CLS_ID").alias("CLS_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("INVC_SK").alias("INVC_SK"),
    F.col("INVC_SUB_BILL_DISP_CD_SK").alias("INVC_SUB_BILL_DISP_CD_SK"),
    F.col("INVC_SUB_FMLY_CNTR_CD_SK").alias("INVC_SUB_FMLY_CNTR_CD_SK"),
    F.col("INVC_SUB_PRM_TYP_CD_SK").alias("INVC_SUB_PRM_TYP_CD_SK"),
    F.col("INVC_TYP_CD_SK").alias("INVC_TYP_CD_SK"),
    F.rpad(F.col("INVC_BILL_DUE_YR_MO_SK"), 6, " ").alias("INVC_BILL_DUE_YR_MO_SK"),
    F.rpad(F.col("INVC_BILL_END_YR_MO_SK"), 6, " ").alias("INVC_BILL_END_YR_MO_SK"),
    F.col("SUB_SK").alias("SUB_SK")
)

# Stage: seq_SUM_PRM_INCM_F_csv_load (PxSequentialFile)
# Write df_cpy_Main_Data_out_2 to SUB_PRM_INCM_F.dat
write_files(
    df_cpy_Main_Data_out_2,
    f"{adls_path}/load/SUB_PRM_INCM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# Stage: seq_B_SUM_PRM_INCM_F_csv_load (PxSequentialFile)
# Write df_cpy_Main_Data_out_1 to B_SUB_PRM_INCM_F.dat
write_files(
    df_cpy_Main_Data_out_1,
    f"{adls_path}/load/B_SUB_PRM_INCM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)