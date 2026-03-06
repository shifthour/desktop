# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY: EdwIncomeExtractSeq
# MAGIC 
# MAGIC Processing: Extracts data from IDS INVC_SUB_SBSDY and creates a load file to load into EDW
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Kalyan Neelam          2014-06-20          5235                                Initial Programming                                                                             EnterpriseNewDevl    Bhoomi Dasari             6/22/2014

# MAGIC Write SUB_SUBSDY_INCM_F Data into a Sequential file for Load Job IdsEdwSumPrmIncmFLoad.
# MAGIC Read all the Data from IDS INVC_SUB Table and join Codes Mapping table to Bring SRC_SYS_CD; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsEdwSubSubsdyIncmFExtr
# MAGIC This Job will extract records from INVC_SUB from IDS and applies code Denormalization
# MAGIC Lookup Keys
# MAGIC 1) INVC_TYP_CD_SK
# MAGIC 2) INVC_SUB_FMLY_CNTR_CD_SK
# MAGIC Write B_SUB_SUBSDY_INCM_F Data into a Sequential file for Load Job IdsEdwBSumPrmIncmFLoad.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve all parameter values
CurrRunycle = get_widget_value("CurrRunycle", "227")
BeginCycle = get_widget_value("BeginCycle", "869")
EdwRuncycleDate = get_widget_value("EdwRuncycleDate", "2014-06-20")
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

# Obtain JDBC configuration for IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Read from db2_INC_SUB_SUBSDY_in (DB2ConnectorPX)
db2_INC_SUB_SUBSDY_in_query = f"""
SELECT
       ISS.INVC_SUB_SBSDY_SK,
       ISS.BILL_INVC_ID,
       ISS.SUB_UNIQ_KEY,
       ISS.CLS_PLN_ID,
       ISS.PROD_ID,
       ISS.PROD_BILL_CMPNT_ID,
       ISS.COV_DUE_DT_SK,
       ISS.COV_STRT_DT_SK,
       ISS.INVC_SUB_SBSDY_PRM_TYP_CD,
       ISS.CRT_TS,
       ISS.INVC_SUB_SBSDY_BILL_DISP_CD,
       COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
       ISS.CRT_RUN_CYC_EXCTN_SK,
       ISS.LAST_UPDT_RUN_CYC_EXCTN_SK,
       ISS.CLS_SK,
       ISS.CLS_PLN_SK,
       ISS.GRP_SK,
       ISS.INVC_SK,
       ISS.PROD_SK,
       ISS.SUBGRP_SK,
       ISS.SUB_SK,
       ISS.INVC_SUB_SBSDY_BILL_DISP_CD_SK,
       ISS.INVC_SUB_SBSDY_FMLY_CNTR_CD_SK,
       ISS.INVC_SUB_SBSDY_PRM_TYP_CD_SK,
       ISS.COV_END_DT_SK,
       ISS.SUB_SBSDY_AMT,
       CASE WHEN INVC.INVC_TYP_CD_SK IS NULL THEN 0 ELSE INVC.INVC_TYP_CD_SK END INVC_TYP_CD_SK,
       CASE WHEN INVC.CUR_RCRD_IN IS NULL THEN 'N' ELSE INVC.CUR_RCRD_IN END CUR_RCRD_IN,
       CASE WHEN INVC.BILL_DUE_DT_SK IS NULL THEN CAST('1753-01-01' AS CHAR(10)) ELSE INVC.BILL_DUE_DT_SK END BILL_DUE_DT_SK,
       CASE WHEN INVC.BILL_END_DT_SK IS NULL THEN CAST('2199-12-31' AS CHAR(10)) ELSE INVC.BILL_END_DT_SK END BILL_END_DT_SK,
       CASE WHEN INVC.CRT_DT_SK IS NULL THEN CAST('1753-01-01' AS CHAR(10)) ELSE INVC.CRT_DT_SK END CRT_DT_SK,
       CASE WHEN INVC.BILL_ENTY_SK IS NULL THEN 0 ELSE INVC.BILL_ENTY_SK END BILL_ENTY_SK,
       CASE WHEN GRP.GRP_ID IS NULL THEN 'UNK' ELSE GRP.GRP_ID END GRP_ID,
       CASE WHEN CLS.CLS_ID IS NULL THEN 'UNK' ELSE CLS.CLS_ID END CLS_ID,
       CASE WHEN SUBGRP.SUBGRP_ID IS NULL THEN 'UNK' ELSE SUBGRP.SUBGRP_ID END SUBGRP_ID
FROM {IDSOwner}.INVC_SUB_SBSDY ISS
LEFT JOIN {IDSOwner}.CD_MPPNG CD
       ON ISS.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
LEFT JOIN {IDSOwner}.INVC INVC
       ON ISS.INVC_SK = INVC.INVC_SK
LEFT JOIN {IDSOwner}.CLS CLS
       ON ISS.CLS_SK = CLS.CLS_SK
LEFT JOIN {IDSOwner}.GRP GRP
       ON ISS.GRP_SK = GRP.GRP_SK
LEFT JOIN {IDSOwner}.SUBGRP SUBGRP
       ON ISS.SUBGRP_SK = SUBGRP.SUBGRP_SK
WHERE ISS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BeginCycle}
"""

df_db2_INC_SUB_SUBSDY_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", db2_INC_SUB_SUBSDY_in_query)
    .load()
)

# Read from db2_CD_MPPNG_Extr (DB2ConnectorPX)
db2_CD_MPPNG_Extr_query = f"""
SELECT
    CD_MPPNG_SK,
    COALESCE(TRGT_CD,'UNK') AS TRGT_CD,
    COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", db2_CD_MPPNG_Extr_query)
    .load()
)

# cpy_cd_mppng (PxCopy)
df_cpy_cd_mppng = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM")
)

# lkp_Codes (PxLookup): primary df_db2_INC_SUB_SUBSDY_in, leftjoin df_cpy_cd_mppng as Ref_InvcTypCd_Lkup on (INVC_TYP_CD_SK=CD_MPPNG_SK), leftjoin df_cpy_cd_mppng as Ref_FmlyCntrCd_Lkup on (INVC_SUB_SBSDY_FMLY_CNTR_CD_SK=CD_MPPNG_SK)
df_lkp_Codes = (
    df_db2_INC_SUB_SUBSDY_in.alias("lnk_IdsEdwSubSubsdyIncFExtr_InABC")
    .join(
        df_cpy_cd_mppng.alias("Ref_InvcTypCd_Lkup"),
        F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.INVC_TYP_CD_SK") == F.col("Ref_InvcTypCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_FmlyCntrCd_Lkup"),
        F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.INVC_SUB_SBSDY_FMLY_CNTR_CD_SK") == F.col("Ref_FmlyCntrCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
)

df_lkp_Codes = df_lkp_Codes.select(
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.INVC_SUB_SBSDY_SK").alias("SUB_SBSDY_INCM_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.PROD_ID").alias("PROD_ID"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.COV_DUE_DT_SK").alias("INVC_SUB_SBSDY_COV_DUE_DT_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.COV_STRT_DT_SK").alias("INVC_SUB_SBSDY_COV_STRT_DT_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.INVC_SUB_SBSDY_PRM_TYP_CD").alias("INVC_SUB_SBSDY_PRM_TYP_CD"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.CRT_TS").alias("INVC_SUB_SBSDY_CRT_DTM"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.INVC_SUB_SBSDY_BILL_DISP_CD").alias("INVC_SUB_SBSDY_BILL_DISP_CD"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.CLS_SK").alias("CLS_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.GRP_SK").alias("GRP_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.INVC_SK").alias("INVC_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.PROD_SK").alias("PROD_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.SUB_SK").alias("SUB_SK"),
    F.col("Ref_FmlyCntrCd_Lkup.TRGT_CD").alias("INVC_SUB_SBSDY_FMLY_CNTR_CD"),
    F.col("Ref_FmlyCntrCd_Lkup.TRGT_CD_NM").alias("INVC_SUB_SBSDY_FMLY_CNTR_NM"),
    F.col("Ref_InvcTypCd_Lkup.TRGT_CD").alias("INVC_TYP_CD"),
    F.col("Ref_InvcTypCd_Lkup.TRGT_CD_NM").alias("INVC_TYP_NM"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.CUR_RCRD_IN").alias("INVC_CUR_RCRD_IN"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.BILL_DUE_DT_SK").alias("INVC_BILL_DUE_DT_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.BILL_END_DT_SK").alias("INVC_BILL_END_DT_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.CRT_DT_SK").alias("INVC_CRT_DT_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.COV_END_DT_SK").alias("INVC_SUB_SBSDY_COV_END_DT_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.SUB_SBSDY_AMT").alias("INVC_SUB_SUB_SBSDY_AMT"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.CLS_ID").alias("CLS_ID"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.GRP_ID").alias("GRP_ID"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.INVC_SUB_SBSDY_BILL_DISP_CD_SK").alias("INVC_SUB_SBSDY_BILL_DISP_CD_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.INVC_SUB_SBSDY_FMLY_CNTR_CD_SK").alias("INVC_SUB_SBSDY_FMLY_CNTR_CD_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.INVC_SUB_SBSDY_PRM_TYP_CD_SK").alias("INVC_SUB_SBSDY_PRM_TYP_CD_SK"),
    F.col("lnk_IdsEdwSubSubsdyIncFExtr_InABC.INVC_TYP_CD_SK").alias("INVC_TYP_CD_SK")
)

# xfrm_BusinessLogic (CTransformerStage)

# 1) df_lnk_IdsEdwSubPrmIncFMain_Out: filter SUB_SBSDY_INCM_SK not in (0,1), then apply transformations
df_lnk_IdsEdwSubPrmIncFMain_Out_pre = df_lkp_Codes.filter(
    (F.col("SUB_SBSDY_INCM_SK") != 0) & (F.col("SUB_SBSDY_INCM_SK") != 1)
)

df_lnk_IdsEdwSubPrmIncFMain_Out = df_lnk_IdsEdwSubPrmIncFMain_Out_pre.select(
    F.col("SUB_SBSDY_INCM_SK"),
    F.col("BILL_INVC_ID"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID"),
    F.col("INVC_SUB_SBSDY_COV_DUE_DT_SK"),
    F.col("INVC_SUB_SBSDY_COV_STRT_DT_SK"),
    F.col("INVC_SUB_SBSDY_PRM_TYP_CD"),
    F.col("INVC_SUB_SBSDY_CRT_DTM"),
    F.col("INVC_SUB_SBSDY_BILL_DISP_CD"),
    F.col("SRC_SYS_CD"),
    F.lit(EdwRuncycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EdwRuncycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(F.col("BILL_ENTY_SK").isNull(), F.lit(0)).otherwise(F.col("BILL_ENTY_SK")).alias("BILL_ENTY_SK"),
    F.when(F.trim(F.col("CLS_SK")) == "", F.lit(0)).otherwise(F.col("CLS_SK")).alias("CLS_SK"),
    F.when(F.trim(F.col("CLS_PLN_SK")) == "", F.lit(0)).otherwise(F.col("CLS_PLN_SK")).alias("CLS_PLN_SK"),
    F.when(F.trim(F.col("GRP_SK")) == "", F.lit(0)).otherwise(F.col("GRP_SK")).alias("GRP_SK"),
    F.col("INVC_SK"),
    F.col("PROD_SK"),
    F.col("SUBGRP_SK"),
    F.col("SUB_SK"),
    F.when(F.trim(F.col("INVC_SUB_SBSDY_FMLY_CNTR_CD")) == "", F.lit("UNK")).otherwise(F.col("INVC_SUB_SBSDY_FMLY_CNTR_CD")).alias("INVC_SUB_SBSDY_FMLY_CNTR_CD"),
    F.when(F.trim(F.col("INVC_SUB_SBSDY_FMLY_CNTR_NM")) == "", F.lit("UNK")).otherwise(F.col("INVC_SUB_SBSDY_FMLY_CNTR_NM")).alias("INVC_SUB_SBSDY_FMLY_CNTR_NM"),
    F.when(F.trim(F.col("INVC_TYP_CD")) == "", F.lit("UNK")).otherwise(F.col("INVC_TYP_CD")).alias("INVC_TYP_CD"),
    F.when(F.trim(F.col("INVC_TYP_NM")) == "", F.lit("UNK")).otherwise(F.col("INVC_TYP_NM")).alias("INVC_TYP_NM"),
    F.when(F.trim(F.col("INVC_CUR_RCRD_IN")) == "", F.lit("N")).otherwise(F.col("INVC_CUR_RCRD_IN")).alias("INVC_CUR_RCRD_IN"),
    # Stage variable usage for BillDueDt
    F.when(F.trim(F.col("INVC_BILL_DUE_DT_SK")) == "", F.lit("1753-01-01")).otherwise(F.col("INVC_BILL_DUE_DT_SK")).alias("INVC_BILL_DUE_DT_SK"),
    (F.substring(
        F.when(F.trim(F.col("INVC_BILL_DUE_DT_SK")) == "", F.lit("1753-01-01")).otherwise(F.col("INVC_BILL_DUE_DT_SK")), 1, 4
     )
     + F.substring(
         F.when(F.trim(F.col("INVC_BILL_DUE_DT_SK")) == "", F.lit("1753-01-01")).otherwise(F.col("INVC_BILL_DUE_DT_SK")), 6, 2
     )).alias("INVC_BILL_DUE_YR_MO_SK"),
    # Stage variable usage for BillEndDt
    F.when(F.trim(F.col("INVC_BILL_END_DT_SK")) == "", F.lit("2199-12-31")).otherwise(F.col("INVC_BILL_END_DT_SK")).alias("INVC_BILL_END_DT_SK"),
    (F.substring(
        F.when(F.trim(F.col("INVC_BILL_END_DT_SK")) == "", F.lit("2199-12-31")).otherwise(F.col("INVC_BILL_END_DT_SK")), 1, 4
     )
     + F.substring(
         F.when(F.trim(F.col("INVC_BILL_END_DT_SK")) == "", F.lit("2199-12-31")).otherwise(F.col("INVC_BILL_END_DT_SK")), 6, 2
     )).alias("INVC_BILL_END_YR_MO_SK"),
    F.when(F.trim(F.col("INVC_CRT_DT_SK")) == "", F.lit("1753-01-01")).otherwise(F.col("INVC_CRT_DT_SK")).alias("INVC_CRT_DT_SK"),
    F.col("INVC_SUB_SBSDY_COV_END_DT_SK"),
    F.col("INVC_SUB_SUB_SBSDY_AMT").alias("INVC_SUB_SUB_SBSDY_AMT"),
    F.when(F.trim(F.col("CLS_ID")) == "", F.lit("NA")).otherwise(F.col("CLS_ID")).alias("CLS_ID"),
    F.when(F.trim(F.col("GRP_ID")) == "", F.lit("NA")).otherwise(F.col("GRP_ID")).alias("GRP_ID"),
    F.when(F.trim(F.col("SUBGRP_ID")) == "", F.lit("NA")).otherwise(F.col("SUBGRP_ID")).alias("SUBGRP_ID"),
    F.lit(CurrRunycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("INVC_SUB_SBSDY_BILL_DISP_CD_SK"),
    F.col("INVC_SUB_SBSDY_FMLY_CNTR_CD_SK"),
    F.col("INVC_SUB_SBSDY_PRM_TYP_CD_SK"),
    F.when(F.trim(F.col("INVC_TYP_CD_SK")) == "", F.lit(0)).otherwise(F.col("INVC_TYP_CD_SK")).alias("INVC_TYP_CD_SK")
)

# 2) df_lnk_UNK: single-row DataFrame with WhereExpression for each column
df_lnk_UNK = spark.createDataFrame(
    [(
        0,                # SUB_SBSDY_INCM_SK
        "UNK",            # BILL_INVC_ID
        0,                # SUB_UNIQ_KEY
        "UNK",            # CLS_PLN_ID
        "UNK",            # PROD_ID
        "UNK",            # PROD_BILL_CMPNT_ID
        "1753-01-01",     # INVC_SUB_SBSDY_COV_DUE_DT_SK
        "1753-01-01",     # INVC_SUB_SBSDY_COV_STRT_DT_SK
        "UNK",            # INVC_SUB_SBSDY_PRM_TYP_CD
        "1753-01-01 00:00:00.000000",  # INVC_SUB_SBSDY_CRT_DTM
        "UNK",            # INVC_SUB_SBSDY_BILL_DISP_CD
        "UNK",            # SRC_SYS_CD
        "1753-01-01",     # CRT_RUN_CYC_EXCTN_DT_SK
        "1753-01-01",     # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
        0,                # BILL_ENTY_SK
        0,                # CLS_SK
        0,                # CLS_PLN_SK
        0,                # GRP_SK
        0,                # INVC_SK
        0,                # PROD_SK
        0,                # SUBGRP_SK
        0,                # SUB_SK
        "UNK",            # INVC_SUB_SBSDY_FMLY_CNTR_CD
        "UNK",            # INVC_SUB_SBSDY_FMLY_CNTR_NM
        "UNK",            # INVC_TYP_CD
        "UNK",            # INVC_TYP_NM
        "N",              # INVC_CUR_RCRD_IN
        "1753-01-01",     # INVC_BILL_DUE_DT_SK
        "175301",         # INVC_BILL_DUE_YR_MO_SK
        "2199-12-31",     # INVC_BILL_END_DT_SK
        "219912",         # INVC_BILL_END_YR_MO_SK
        "1753-01-01",     # INVC_CRT_DT_SK
        "2199-12-31",     # INVC_SUB_SBSDY_COV_END_DT_SK
        0,                # INVC_SUB_SUB_SBSDY_AMT
        "UNK",            # CLS_ID
        "UNK",            # GRP_ID
        "UNK",            # SUBGRP_ID
        0,                # CRT_RUN_CYC_EXCTN_SK
        0,                # LAST_UPDT_RUN_CYC_EXCTN_SK
        0,                # INVC_SUB_SBSDY_BILL_DISP_CD_SK
        0,                # INVC_SUB_SBSDY_FMLY_CNTR_CD_SK
        0,                # INVC_SUB_SBSDY_PRM_TYP_CD_SK
        0                 # INVC_TYP_CD_SK
    )],
    [
        "SUB_SBSDY_INCM_SK",
        "BILL_INVC_ID",
        "SUB_UNIQ_KEY",
        "CLS_PLN_ID",
        "PROD_ID",
        "PROD_BILL_CMPNT_ID",
        "INVC_SUB_SBSDY_COV_DUE_DT_SK",
        "INVC_SUB_SBSDY_COV_STRT_DT_SK",
        "INVC_SUB_SBSDY_PRM_TYP_CD",
        "INVC_SUB_SBSDY_CRT_DTM",
        "INVC_SUB_SBSDY_BILL_DISP_CD",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "BILL_ENTY_SK",
        "CLS_SK",
        "CLS_PLN_SK",
        "GRP_SK",
        "INVC_SK",
        "PROD_SK",
        "SUBGRP_SK",
        "SUB_SK",
        "INVC_SUB_SBSDY_FMLY_CNTR_CD",
        "INVC_SUB_SBSDY_FMLY_CNTR_NM",
        "INVC_TYP_CD",
        "INVC_TYP_NM",
        "INVC_CUR_RCRD_IN",
        "INVC_BILL_DUE_DT_SK",
        "INVC_BILL_DUE_YR_MO_SK",
        "INVC_BILL_END_DT_SK",
        "INVC_BILL_END_YR_MO_SK",
        "INVC_CRT_DT_SK",
        "INVC_SUB_SBSDY_COV_END_DT_SK",
        "INVC_SUB_SUB_SBSDY_AMT",
        "CLS_ID",
        "GRP_ID",
        "SUBGRP_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "INVC_SUB_SBSDY_BILL_DISP_CD_SK",
        "INVC_SUB_SBSDY_FMLY_CNTR_CD_SK",
        "INVC_SUB_SBSDY_PRM_TYP_CD_SK",
        "INVC_TYP_CD_SK"
    ]
)

# 3) df_lnk_NA: single-row DataFrame with WhereExpression for each column
df_lnk_NA = spark.createDataFrame(
    [(
        1,                # SUB_SBSDY_INCM_SK
        "NA",             # BILL_INVC_ID
        1,                # SUB_UNIQ_KEY
        "NA",             # CLS_PLN_ID
        "NA",             # PROD_ID
        "NA",             # PROD_BILL_CMPNT_ID
        "1753-01-01",     # INVC_SUB_SBSDY_COV_DUE_DT_SK
        "1753-01-01",     # INVC_SUB_SBSDY_COV_STRT_DT_SK
        "NA",             # INVC_SUB_SBSDY_PRM_TYP_CD
        "1753-01-01 00:00:00.000000",  # INVC_SUB_SBSDY_CRT_DTM
        "NA",             # INVC_SUB_SBSDY_BILL_DISP_CD
        "NA",             # SRC_SYS_CD
        "1753-01-01",     # CRT_RUN_CYC_EXCTN_DT_SK
        "1753-01-01",     # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
        1,                # BILL_ENTY_SK
        1,                # CLS_SK
        1,                # CLS_PLN_SK
        1,                # GRP_SK
        1,                # INVC_SK
        1,                # PROD_SK
        1,                # SUBGRP_SK
        1,                # SUB_SK
        "NA",             # INVC_SUB_SBSDY_FMLY_CNTR_CD
        "NA",             # INVC_SUB_SBSDY_FMLY_CNTR_NM
        "NA",             # INVC_TYP_CD
        "NA",             # INVC_TYP_NM
        "N",              # INVC_CUR_RCRD_IN
        "1753-01-01",     # INVC_BILL_DUE_DT_SK
        "175301",         # INVC_BILL_DUE_YR_MO_SK
        "2199-12-31",     # INVC_BILL_END_DT_SK
        "219912",         # INVC_BILL_END_YR_MO_SK
        "1753-01-01",     # INVC_CRT_DT_SK
        "2199-12-31",     # INVC_SUB_SBSDY_COV_END_DT_SK
        0,                # INVC_SUB_SUB_SBSDY_AMT
        "NA",             # CLS_ID
        "NA",             # GRP_ID
        "NA",             # SUBGRP_ID
        1,                # CRT_RUN_CYC_EXCTN_SK
        1,                # LAST_UPDT_RUN_CYC_EXCTN_SK
        1,                # INVC_SUB_SBSDY_BILL_DISP_CD_SK
        1,                # INVC_SUB_SBSDY_FMLY_CNTR_CD_SK
        1,                # INVC_SUB_SBSDY_PRM_TYP_CD_SK
        1                 # INVC_TYP_CD_SK
    )],
    [
        "SUB_SBSDY_INCM_SK",
        "BILL_INVC_ID",
        "SUB_UNIQ_KEY",
        "CLS_PLN_ID",
        "PROD_ID",
        "PROD_BILL_CMPNT_ID",
        "INVC_SUB_SBSDY_COV_DUE_DT_SK",
        "INVC_SUB_SBSDY_COV_STRT_DT_SK",
        "INVC_SUB_SBSDY_PRM_TYP_CD",
        "INVC_SUB_SBSDY_CRT_DTM",
        "INVC_SUB_SBSDY_BILL_DISP_CD",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "BILL_ENTY_SK",
        "CLS_SK",
        "CLS_PLN_SK",
        "GRP_SK",
        "INVC_SK",
        "PROD_SK",
        "SUBGRP_SK",
        "SUB_SK",
        "INVC_SUB_SBSDY_FMLY_CNTR_CD",
        "INVC_SUB_SBSDY_FMLY_CNTR_NM",
        "INVC_TYP_CD",
        "INVC_TYP_NM",
        "INVC_CUR_RCRD_IN",
        "INVC_BILL_DUE_DT_SK",
        "INVC_BILL_DUE_YR_MO_SK",
        "INVC_BILL_END_DT_SK",
        "INVC_BILL_END_YR_MO_SK",
        "INVC_CRT_DT_SK",
        "INVC_SUB_SBSDY_COV_END_DT_SK",
        "INVC_SUB_SUB_SBSDY_AMT",
        "CLS_ID",
        "GRP_ID",
        "SUBGRP_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "INVC_SUB_SBSDY_BILL_DISP_CD_SK",
        "INVC_SUB_SBSDY_FMLY_CNTR_CD_SK",
        "INVC_SUB_SBSDY_PRM_TYP_CD_SK",
        "INVC_TYP_CD_SK"
    ]
)

# fnl_UNK_NA (PxFunnel): union all three dataframes
df_fnl_UNK_NA = df_lnk_IdsEdwSubPrmIncFMain_Out.unionByName(df_lnk_UNK).unionByName(df_lnk_NA)

# cpy_Main_Data (PxCopy) => we produce distinct outputs by selecting columns in the required order
# 1) lnk_IdsEdwBSubSubsdyIncFExtr_OutABC (12 columns)
df_lnk_IdsEdwBSubSubsdyIncFExtr_OutABC = df_fnl_UNK_NA.select(
    F.col("BILL_INVC_ID"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID"),
    F.rpad(F.col("INVC_SUB_SBSDY_COV_DUE_DT_SK"), 10, " ").alias("INVC_SUB_SBSDY_COV_DUE_DT_SK"),
    F.rpad(F.col("INVC_SUB_SBSDY_COV_STRT_DT_SK"), 10, " ").alias("INVC_SUB_SBSDY_COV_STRT_DT_SK"),
    F.col("INVC_SUB_SBSDY_PRM_TYP_CD"),
    F.col("INVC_SUB_SBSDY_CRT_DTM"),
    F.col("INVC_SUB_SBSDY_BILL_DISP_CD"),
    F.col("SRC_SYS_CD"),
    F.col("INVC_SUB_SUB_SBSDY_AMT")
)

# 2) lnk_IdsEdwSubSubsdyIncFExtr_OutABC (46 columns)
df_lnk_IdsEdwSubSubsdyIncFExtr_OutABC = df_fnl_UNK_NA.select(
    F.col("SUB_SBSDY_INCM_SK"),
    F.col("BILL_INVC_ID"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID"),
    F.rpad(F.col("INVC_SUB_SBSDY_COV_DUE_DT_SK"), 10, " ").alias("INVC_SUB_SBSDY_COV_DUE_DT_SK"),
    F.rpad(F.col("INVC_SUB_SBSDY_COV_STRT_DT_SK"), 10, " ").alias("INVC_SUB_SBSDY_COV_STRT_DT_SK"),
    F.col("INVC_SUB_SBSDY_PRM_TYP_CD"),
    F.col("INVC_SUB_SBSDY_CRT_DTM"),
    F.col("INVC_SUB_SBSDY_BILL_DISP_CD"),
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BILL_ENTY_SK"),
    F.col("CLS_SK"),
    F.col("CLS_PLN_SK"),
    F.col("GRP_SK"),
    F.col("INVC_SK"),
    F.col("PROD_SK"),
    F.col("SUBGRP_SK"),
    F.col("SUB_SK"),
    F.col("INVC_SUB_SBSDY_FMLY_CNTR_CD"),
    F.col("INVC_SUB_SBSDY_FMLY_CNTR_NM"),
    F.col("INVC_TYP_CD"),
    F.col("INVC_TYP_NM"),
    F.rpad(F.col("INVC_CUR_RCRD_IN"), 1, " ").alias("INVC_CUR_RCRD_IN"),
    F.rpad(F.col("INVC_BILL_DUE_DT_SK"), 10, " ").alias("INVC_BILL_DUE_DT_SK"),
    F.rpad(F.col("INVC_BILL_DUE_YR_MO_SK"), 6, " ").alias("INVC_BILL_DUE_YR_MO_SK"),
    F.rpad(F.col("INVC_BILL_END_DT_SK"), 10, " ").alias("INVC_BILL_END_DT_SK"),
    F.rpad(F.col("INVC_BILL_END_YR_MO_SK"), 6, " ").alias("INVC_BILL_END_YR_MO_SK"),
    F.rpad(F.col("INVC_CRT_DT_SK"), 10, " ").alias("INVC_CRT_DT_SK"),
    F.rpad(F.col("INVC_SUB_SBSDY_COV_END_DT_SK"), 10, " ").alias("INVC_SUB_SBSDY_COV_END_DT_SK"),
    F.col("INVC_SUB_SUB_SBSDY_AMT"),
    F.col("CLS_ID"),
    F.col("GRP_ID"),
    F.col("SUBGRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("INVC_SUB_SBSDY_BILL_DISP_CD_SK"),
    F.col("INVC_SUB_SBSDY_FMLY_CNTR_CD_SK"),
    F.col("INVC_SUB_SBSDY_PRM_TYP_CD_SK"),
    F.col("INVC_TYP_CD_SK")
)

# seq_SUB_SUBSDY_INCM_F_csv_load (PxSequentialFile)
# Write SUB_SBSDY_INCM_F.dat
write_files(
    df_lnk_IdsEdwSubSubsdyIncFExtr_OutABC,
    f"{adls_path}/load/SUB_SBSDY_INCM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# seq_B_SUB_SBSDY_INCM_F_csv_load (PxSequentialFile)
# Write B_SUB_SBSDY_INCM_F.dat
write_files(
    df_lnk_IdsEdwBSubSubsdyIncFExtr_OutABC,
    f"{adls_path}/load/B_SUB_SBSDY_INCM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)