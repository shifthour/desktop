# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_2 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/12/08 13:19:45 Batch  14927_47989 PROMOTE bckcetl ids20 dsadm bls for sa/sg
# MAGIC ^1_1 11/12/08 13:12:55 Batch  14927_47581 INIT bckcett testIDSnew dsadm bls for sa/sg
# MAGIC ^1_1 11/10/08 15:58:46 Batch  14925_57530 PROMOTE bckcett testIDSnew u03651 steph for Sharon
# MAGIC ^1_1 11/10/08 15:53:35 Batch  14925_57218 INIT bckcett devlIDSnew u03651 steffy
# MAGIC 
# MAGIC       
# MAGIC ********************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  ESIDrugClmCOBExtrLoadSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:     Reads the IDS CLM_COB data for ESI claims just loaded and creates a EDW CLM_COB_F record
# MAGIC 
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC SANdrew               2008-10-10      3784                      Original Programming                                                                      devlIDSnew                    Steph Goddard          11/07/2008


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
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunCycle = get_widget_value('RunCycle','1')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','ESI')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Stage: CLM_COB (DB2Connector)
extract_query_clm_cob = f"""
SELECT 
  COB.CLM_COB_SK,
  COB.SRC_SYS_CD_SK,
  COB.CLM_ID,
  COB.CLM_COB_TYP_CD_SK,
  COB.CRT_RUN_CYC_EXCTN_SK,
  COB.LAST_UPDT_RUN_CYC_EXCTN_SK,
  COB.CLM_SK,
  CLM_COB_LIAB_TYP_CD_SK,
  ALW_AMT,
  COPAY_AMT,
  DEDCT_AMT,
  DSALW_AMT,
  MED_COINS_AMT,
  MNTL_HLTH_COINS_AMT,
  PD_AMT,
  SANC_AMT,
  COB_CAR_RSN_CD_TX,
  COB_CAR_RSN_TX
FROM {IDSOwner}.CLM_COB COB
WHERE COB.SRC_SYS_CD_SK = {SrcSysCdSk}
  AND COB.LAST_UPDT_RUN_CYC_EXCTN_SK = {RunCycle}
"""
df_CLM_COB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_clm_cob)
    .load()
)

# Stage: ids_cd_mppng_tbl (DB2Connector)
extract_query_cd_mppng = f"""
SELECT
  CD_MPPNG.CD_MPPNG_SK AS CD_MPPNG_SK,
  CD_MPPNG.TRGT_CD AS TRGT_CD,
  CD_MPPNG.TRGT_CD_NM AS TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG CD_MPPNG
"""
df_ids_cd_mppng_tbl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_cd_mppng)
    .load()
)

# Stage: refCodes2 (CHashedFileStage) - Scenario A: Replace with direct dedup
df_ids_cd_mppng_tbl_dedup = dedup_sort(
    df_ids_cd_mppng_tbl,
    ["CD_MPPNG_SK"],
    []
)

# Stage: TransCodes (CTransformerStage) with multiple reference joins
df_TransCodes = (
    df_CLM_COB.alias("Extract")
    .join(
        df_ids_cd_mppng_tbl_dedup.alias("refSrcSys"),
        F.col("Extract.SRC_SYS_CD_SK") == F.col("refSrcSys.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_ids_cd_mppng_tbl_dedup.alias("refClmCobType"),
        F.col("Extract.CLM_COB_TYP_CD_SK") == F.col("refClmCobType.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_ids_cd_mppng_tbl_dedup.alias("refLiabTyp"),
        F.col("Extract.CLM_COB_LIAB_TYP_CD_SK") == F.col("refLiabTyp.CD_MPPNG_SK"),
        "left"
    )
)

df_enriched = df_TransCodes.select(
    F.col("Extract.CLM_COB_SK").alias("CLM_COB_SK"),
    F.col("refSrcSys.TRGT_CD").alias("SRC_SYS_CD"),
    F.col("Extract.CLM_ID").alias("CLM_ID"),
    F.col("refClmCobType.TRGT_CD").alias("CLM_COB_TYP_CD"),
    F.lit(CurrentDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrentDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Extract.CLM_SK").alias("CLM_SK"),
    F.col("refLiabTyp.TRGT_CD").alias("CLM_COB_LIAB_TYP_CD"),
    F.col("Extract.ALW_AMT").alias("CLM_COB_ALW_AMT"),
    F.col("Extract.COPAY_AMT").alias("CLM_COB_COPAY_AMT"),
    F.col("Extract.DEDCT_AMT").alias("CLM_COB_DEDCT_AMT"),
    F.col("Extract.DSALW_AMT").alias("CLM_COB_DSALW_AMT"),
    F.col("Extract.MED_COINS_AMT").alias("CLM_COB_MED_COINS_AMT"),
    F.col("Extract.MNTL_HLTH_COINS_AMT").alias("CLM_COB_MNTL_HLTH_COINS_AMT"),
    F.col("Extract.PD_AMT").alias("CLM_COB_PD_AMT"),
    F.col("Extract.SANC_AMT").alias("CLM_COB_SANC_AMT"),
    F.col("Extract.COB_CAR_RSN_CD_TX").alias("CLM_COB_CAR_RSN_CD_TX"),
    F.col("Extract.COB_CAR_RSN_TX").alias("CLM_COB_CAR_RSN_TX"),
    F.col("Extract.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Extract.CLM_COB_TYP_CD_SK").alias("CLM_COB_TYP_CD_SK"),
    F.col("Extract.CLM_COB_LIAB_TYP_CD_SK").alias("CLM_COB_LIAB_TYP_CD_SK")
)

df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "SRC_SYS_CD",
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ")
).withColumn(
    "CLM_COB_TYP_CD",
    F.rpad(F.col("CLM_COB_TYP_CD"), <...>, " ")
).withColumn(
    "CLM_COB_LIAB_TYP_CD",
    F.rpad(F.col("CLM_COB_LIAB_TYP_CD"), <...>, " ")
)

# Stage: CLM_COB_F (CSeqFileStage) - Write to .dat
write_files(
    df_enriched,
    f"{adls_path}/load/CLM_COB_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)