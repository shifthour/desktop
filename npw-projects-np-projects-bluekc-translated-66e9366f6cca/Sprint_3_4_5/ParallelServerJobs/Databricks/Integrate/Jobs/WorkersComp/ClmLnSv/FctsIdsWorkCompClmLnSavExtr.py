# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY: FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM_LN table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                          Change Description                                                   Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)------------------------                                        -----------------------------------------------------------------------                  ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2016-12-14\(9)5628 WORK_COMPNSTN_CLM_LN \(9)    Original Programming\(9)\(9)\(9)          IntegrateDev2                         Kalyan Neelam          2017-03-15 
# MAGIC                                                                 (Facets to IDS) ETL Report
# MAGIC 
# MAGIC Akhila Manickavelu\(9)  2017-05-23\(9)5628 WORK_COMPNSTN              \(9)    Removed the Driver table join since\(9)                            IntegrateDev2                      Kalyan Neelam          2017-05-25
# MAGIC                                                                 (Facets to IDS) ETL Report                           the data is taken from IDS itself for the
# MAGIC                                                                                                                                      latest run cycle

# MAGIC This job Extracts the Claims data from IDS WORK COMP claim tables to generate the load file WORK_COMPNSTN_CLM_LN_SAV.dat to be send to IDS WORK_COMPNSTN_CLM_LN_SAV table
# MAGIC WORK_COMPNSTN_CLM_LN_SAV.dat load the target ids table WORK_COMPNSTN_CLM_LN_SAV for
# MAGIC Join with P_CLM_LN_SAV_XREF for getting the SAV_TYP_CD value
# MAGIC Create the Pecking order to get the right Preceded value for   DSALW_TYP_CD
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
RunCycle = get_widget_value('RunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
WorkCompOwner = get_widget_value('WorkCompOwner','')
workcomp_secret_name = get_widget_value('workcomp_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
DrivTable = get_widget_value('DrivTable','')
FacetsDB = get_widget_value('FacetsDB','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')

# DB Config for IDS connections
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# -------------------------------------------------------------------------------------------------------------------
# Stage: Clm_ln_sav (DB2ConnectorPX) : Read
# -------------------------------------------------------------------------------------------------------------------
extract_query_Clm_ln_sav = f"SELECT CLM_ID, CLM_LN_SEQ_NO, CLM_LN_SAV_TYP_CD, CRT_RUN_CYC_EXCTN_SK FROM {WorkCompOwner}.WORK_COMPNSTN_CLM_LN_SAV"
df_Clm_ln_sav = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Clm_ln_sav)
    .load()
)

# -------------------------------------------------------------------------------------------------------------------
# Stage: CD_MPPNG (DB2ConnectorPX) : Read
# -------------------------------------------------------------------------------------------------------------------
extract_query_CD_MPPNG = f"""
SELECT DISTINCT
TRGT_CD,
SRC_CD,
CD_MPPNG_SK,
SRC_DOMAIN_NM,
TRGT_DOMAIN_NM,
SRC_CLCTN_CD,
SRC_SYS_CD
FROM {IDSOwner}.CD_MPPNG 
WHERE
TRGT_CLCTN_CD = 'IDS'
AND TRGT_DOMAIN_NM  IN ('CLAIM LINE DISALLOW TYPE','CLAIM LINE SAVINGS TYPE')
"""
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CD_MPPNG)
    .load()
)

# -------------------------------------------------------------------------------------------------------------------
# Stage: Fltr_PXRef (PxFilter)
# -------------------------------------------------------------------------------------------------------------------
# Output link 0: Lnk_CdMppng
df_Fltr_PXRef_0 = df_CD_MPPNG.filter(
    "SRC_DOMAIN_NM = 'CLAIM LINE DISALLOW TYPE' AND TRGT_DOMAIN_NM = 'CLAIM LINE DISALLOW TYPE' AND SRC_SYS_CD='FACETS' AND SRC_CLCTN_CD='FACETS DBO'"
).select(
    F.col("TRGT_CD"),
    F.col("SRC_CD")
)

# Output link 1: Lnk_Cd_Mp_Sk
df_Fltr_PXRef_1 = df_CD_MPPNG.filter(
    "TRGT_DOMAIN_NM = 'CLAIM LINE SAVINGS TYPE'"
).select(
    F.col("TRGT_CD"),
    F.col("CD_MPPNG_SK")
)

# -------------------------------------------------------------------------------------------------------------------
# Stage: P_CLM_LN_SAV_XREF (DB2ConnectorPX) : Read
# -------------------------------------------------------------------------------------------------------------------
extract_query_P_CLM_LN_SAV_XREF = f"""
SELECT DISTINCT
P_CLM_LN_SAV_XREF.CLM_LN_DSALW_TYP_CD,
P_CLM_LN_SAV_XREF.CLM_LN_SAV_TYP_CD
FROM
{IDSOwner}.P_CLM_LN_SAV_XREF P_CLM_LN_SAV_XREF
WHERE
P_CLM_LN_SAV_XREF.CLM_NTWK_STTUS_CD IN ('I','P')
AND P_CLM_LN_SAV_XREF.EXCD_RESP_CD='P'
"""
df_P_CLM_LN_SAV_XREF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_P_CLM_LN_SAV_XREF)
    .load()
)

# -------------------------------------------------------------------------------------------------------------------
# Stage: WorkCompnstnClmLnSav (DB2ConnectorPX) : Read
# -------------------------------------------------------------------------------------------------------------------
extract_query_WorkCompnstnClmLnSav = f"""
SELECT
 WORK_COMPNSTN_CLM.CLM_ID,
 WORK_COMPNSTN_CLM_LN.CLM_LN_SEQ_NO,
 WORK_COMPNSTN_CLM_LN_DSALW.CLM_LN_DSALW_TYP_CD,
 WORK_COMPNSTN_CLM_LN_DSALW.DSALW_AMT,
 WORK_COMPNSTN_CLM_LN_DSALW.CLM_LN_DSALW_EXCD_SK,
 DSALW_EXCD.EXCD_ID,
 DSALW_EXCD.BYPS_IN
FROM
{WorkCompOwner}.WORK_COMPNSTN_CLM WORK_COMPNSTN_CLM
INNER JOIN {WorkCompOwner}.WORK_COMPNSTN_CLM_LN  WORK_COMPNSTN_CLM_LN 
  ON WORK_COMPNSTN_CLM_LN.CLM_ID=WORK_COMPNSTN_CLM.CLM_ID
LEFT OUTER JOIN  {WorkCompOwner}.WORK_COMPNSTN_CLM_LN_DSALW  WORK_COMPNSTN_CLM_LN_DSALW 
  ON WORK_COMPNSTN_CLM_LN.CLM_ID=WORK_COMPNSTN_CLM_LN_DSALW.CLM_ID 
  AND WORK_COMPNSTN_CLM_LN.CLM_LN_SEQ_NO=WORK_COMPNSTN_CLM_LN_DSALW.CLM_LN_SEQ_NO
LEFT OUTER JOIN {IDSOwner}.DSALW_EXCD DSALW_EXCD 
  ON WORK_COMPNSTN_CLM_LN_DSALW.CLM_LN_DSALW_EXCD_SK = DSALW_EXCD.EXCD_SK
LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CD_MPPNG 
  ON WORK_COMPNSTN_CLM.CLM_FINL_DISP_CD_SK=CD_MPPNG.CD_MPPNG_SK
LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CD_MPPNG_STTUS 
  ON WORK_COMPNSTN_CLM.CLM_STTUS_CD_SK=CD_MPPNG_STTUS.CD_MPPNG_SK
WHERE 
 WORK_COMPNSTN_CLM.SRC_SYS_CD='FACETS'
 AND CD_MPPNG.TRGT_CD='ACPTD'
 AND WORK_COMPNSTN_CLM.PD_DT >= DSALW_EXCD.EFF_DT_SK
 AND WORK_COMPNSTN_CLM.PD_DT  <= DSALW_EXCD.TERM_DT_SK
 AND CD_MPPNG_STTUS.TRGT_CD IN ('A02','A08','A09')
 AND WORK_COMPNSTN_CLM.LAST_UPDT_RUN_CYC_EXCTN_SK={RunCycle}
"""
df_WorkCompnstnClmLnSav = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_WorkCompnstnClmLnSav)
    .load()
)

# -------------------------------------------------------------------------------------------------------------------
# Stage: Lkp_cdmap (PxLookup)
# Primary link -> df_WorkCompnstnClmLnSav
# Reference link -> df_Fltr_PXRef_0, left join on (CLM_LN_DSALW_TYP_CD = TRGT_CD)
# -------------------------------------------------------------------------------------------------------------------
df_Lkp_cdmap = (
    df_WorkCompnstnClmLnSav.alias("Lnk_WorkCompClmLnSav")
    .join(
        df_Fltr_PXRef_0.alias("Lnk_CdMppng"),
        F.col("Lnk_WorkCompClmLnSav.CLM_LN_DSALW_TYP_CD") == F.col("Lnk_CdMppng.TRGT_CD"),
        how="left"
    )
    .select(
        F.col("Lnk_WorkCompClmLnSav.CLM_ID").alias("CLM_ID"),
        F.col("Lnk_WorkCompClmLnSav.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("Lnk_WorkCompClmLnSav.CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD"),
        F.col("Lnk_WorkCompClmLnSav.DSALW_AMT").alias("DSALW_AMT"),
        F.col("Lnk_WorkCompClmLnSav.CLM_LN_DSALW_EXCD_SK").alias("CLM_LN_DSALW_EXCD_SK"),
        F.col("Lnk_WorkCompClmLnSav.EXCD_ID").alias("EXCD_ID"),
        F.col("Lnk_WorkCompClmLnSav.BYPS_IN").alias("BYPS_IN"),
        F.col("Lnk_CdMppng.SRC_CD").alias("SRC_CD")
    )
)

# -------------------------------------------------------------------------------------------------------------------
# Stage: Xfm_PeckingOrder (CTransformerStage)
# -------------------------------------------------------------------------------------------------------------------
df_Xfm_PeckingOrder = df_Lkp_cdmap.withColumn(
    "PECKING_ORDER",
    F.when(F.col("SRC_CD") == 'XE', F.lit(1))
    .when(F.col("SRC_CD") == 'LT', F.lit(2))
    .when(F.col("SRC_CD") == 'DC', F.lit(3))
    .when(F.col("SRC_CD") == 'DP', F.lit(4))
    .when(F.col("SRC_CD") == 'CAP', F.lit(5))
    .when(F.col("SRC_CD") == 'CE', F.lit(6))
    .when(F.col("SRC_CD") == 'PN', F.lit(7))
    .when(F.col("SRC_CD") == 'SD', F.lit(8))
    .when(F.col("SRC_CD") == 'UM', F.lit(9))
    .when(F.col("SRC_CD") == 'DS', F.lit(10))
    .when(F.col("SRC_CD") == 'RW', F.lit(11))
    .when(F.col("SRC_CD") == 'SE', F.lit(12))
    .when(F.trim(F.col("SRC_CD")) == 'PI', F.lit(13))
    .when(F.col("SRC_CD") == 'SP', F.lit(14))
    .when(F.col("SRC_CD") == 'DX', F.lit(15))
    .when(F.col("SRC_CD") == 'AX', F.lit(16))
    .when(F.col("SRC_CD") == 'ATAU', F.lit(17))
    .when(F.col("SRC_CD") == 'DA', F.lit(18))
    .when(F.col("SRC_CD") == 'AA', F.lit(19))
    .when(F.col("SRC_CD") == 'DTDU', F.lit(20))
    .when(F.col("SRC_CD") == 'PC', F.lit(21))
    .when(F.col("SRC_CD") == 'XX', F.lit(22))
    .otherwise(F.lit(23))
).withColumn(
    "BYPASSORDER",
    F.when(F.col("BYPS_IN") == 'Y', F.lit(1)).otherwise(F.lit(2))
)

df_Xfm_PeckingOrder = df_Xfm_PeckingOrder.select(
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_DSALW_TYP_CD",
    "DSALW_AMT",
    "CLM_LN_DSALW_EXCD_SK",
    "EXCD_ID",
    "BYPS_IN",
    "SRC_CD",
    "PECKING_ORDER",
    "BYPASSORDER"
)

# -------------------------------------------------------------------------------------------------------------------
# Stage: RmDups_Claim (PxRemDup)
# RetainRecord=first; KeysThatDefineDuplicates=CLM_ID,CLM_LN_SEQ_NO
# KeySortOrder => BYPASSORDER asc, PECKING_ORDER asc
# -------------------------------------------------------------------------------------------------------------------
df_RmDups_Claim = dedup_sort(
    df_Xfm_PeckingOrder,
    partition_cols=["CLM_ID", "CLM_LN_SEQ_NO"],
    sort_cols=[("BYPASSORDER","A"),("PECKING_ORDER","A")]
)

df_RmDups_Claim = df_RmDups_Claim.select(
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_DSALW_TYP_CD",
    "DSALW_AMT",
    "CLM_LN_DSALW_EXCD_SK",
    "EXCD_ID",
    "BYPS_IN",
    "PECKING_ORDER",
    "SRC_CD"
)

# -------------------------------------------------------------------------------------------------------------------
# Stage: Lkp_PXref (PxLookup)
# Primary link -> df_RmDups_Claim
# Reference link -> df_P_CLM_LN_SAV_XREF, left join on (SRC_CD = CLM_LN_DSALW_TYP_CD)
# -------------------------------------------------------------------------------------------------------------------
df_Lkp_PXref = (
    df_RmDups_Claim.alias("Lnk_Dsalw")
    .join(
        df_P_CLM_LN_SAV_XREF.alias("Lnk_Xref"),
        F.col("Lnk_Dsalw.SRC_CD") == F.col("Lnk_Xref.CLM_LN_DSALW_TYP_CD"),
        how="left"
    )
    .select(
        F.col("Lnk_Dsalw.CLM_ID").alias("CLM_ID"),
        F.col("Lnk_Dsalw.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("Lnk_Dsalw.CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD"),
        F.col("Lnk_Dsalw.DSALW_AMT").alias("DSALW_AMT"),
        F.col("Lnk_Dsalw.CLM_LN_DSALW_EXCD_SK").alias("CLM_LN_DSALW_EXCD_SK"),
        F.col("Lnk_Dsalw.EXCD_ID").alias("EXCD_ID"),
        F.col("Lnk_Dsalw.BYPS_IN").alias("BYPS_IN"),
        F.col("Lnk_Dsalw.PECKING_ORDER").alias("PECKING_ORDER"),
        F.col("Lnk_Dsalw.SRC_CD").alias("SRC_CD"),
        F.col("Lnk_Xref.CLM_LN_SAV_TYP_CD").alias("CLM_LN_SAV_TYP_CD")
    )
)

# -------------------------------------------------------------------------------------------------------------------
# Stage: Lkup_clm_ln_sv (PxLookup)
# Primary link -> df_Lkp_PXref
# Reference link 1 -> df_Clm_ln_sav, left join on (CLM_ID, CLM_LN_SEQ_NO, CLM_LN_SAV_TYP_CD)
# Reference link 2 -> df_Fltr_PXRef_1, left join on (CLM_LN_SAV_TYP_CD = TRGT_CD)
# -------------------------------------------------------------------------------------------------------------------
df_lkup_clm_ln_sv = (
    df_Lkp_PXref.alias("Lnk_tofindruncyc")
    .join(
        df_Clm_ln_sav.alias("Lnk_Clm_ln_sav"),
        (
            (F.col("Lnk_tofindruncyc.CLM_ID") == F.col("Lnk_Clm_ln_sav.CLM_ID"))
            & (F.col("Lnk_tofindruncyc.CLM_LN_SEQ_NO") == F.col("Lnk_Clm_ln_sav.CLM_LN_SEQ_NO"))
            & (F.col("Lnk_tofindruncyc.CLM_LN_SAV_TYP_CD") == F.col("Lnk_Clm_ln_sav.CLM_LN_SAV_TYP_CD"))
        ),
        how="left"
    )
    .join(
        df_Fltr_PXRef_1.alias("Lnk_Cd_Mp_Sk"),
        F.col("Lnk_tofindruncyc.CLM_LN_SAV_TYP_CD") == F.col("Lnk_Cd_Mp_Sk.TRGT_CD"),
        how="left"
    )
    .select(
        F.col("Lnk_tofindruncyc.CLM_ID").alias("CLM_ID"),
        F.col("Lnk_tofindruncyc.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("Lnk_tofindruncyc.CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD"),
        F.col("Lnk_tofindruncyc.DSALW_AMT").alias("DSALW_AMT"),
        F.col("Lnk_tofindruncyc.CLM_LN_DSALW_EXCD_SK").alias("CLM_LN_DSALW_EXCD_SK"),
        F.col("Lnk_tofindruncyc.EXCD_ID").alias("EXCD_ID"),
        F.col("Lnk_tofindruncyc.BYPS_IN").alias("BYPS_IN"),
        F.col("Lnk_tofindruncyc.PECKING_ORDER").alias("PECKING_ORDER"),
        F.col("Lnk_tofindruncyc.SRC_CD").alias("SRC_CD"),
        F.col("Lnk_tofindruncyc.CLM_LN_SAV_TYP_CD").alias("CLM_LN_SAV_TYP_CD"),
        F.col("Lnk_Cd_Mp_Sk.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.col("Lnk_Clm_ln_sav.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
    )
)

# -------------------------------------------------------------------------------------------------------------------
# Stage: BusinessLogic (CTransformerStage)
# -------------------------------------------------------------------------------------------------------------------
df_BusinessLogic = (
    df_lkup_clm_ln_sv
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn("CLM_LN_SAV_TYP_CD",
        F.when(
            F.col("CLM_LN_SAV_TYP_CD").isNull() | (trim(F.col("CLM_LN_SAV_TYP_CD")) == ''),
            F.lit("UNK")
        ).otherwise(F.col("CLM_LN_SAV_TYP_CD"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK",
        F.when(
            F.col("CRT_RUN_CYC_EXCTN_SK").isNull(),
            F.lit(RunCycle)
        ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(RunCycle))
    .withColumn("CLM_LN_SAV_TYP_CD_SK",
        F.when(
            F.col("CD_MPPNG_SK").isNull(),
            F.lit(0)
        ).otherwise(F.col("CD_MPPNG_SK"))
    )
    .withColumn("SAV_AMT",
        F.when(
            F.col("DSALW_AMT").isNull(),
            F.lit(0.00)
        ).otherwise(F.col("DSALW_AMT"))
    )
)

df_BusinessLogic = df_BusinessLogic.select(
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("SRC_SYS_CD"),
    F.col("CLM_LN_SAV_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_SAV_TYP_CD_SK"),
    F.col("SAV_AMT")
)

# -------------------------------------------------------------------------------------------------------------------
# Stage: WORK_COMPNSTN_CLM_LN_SAV (PxSequentialFile) : Write
# File path: #$FilePath#/load/WORK_COMPNSTN_CLM_LN_SAV.dat => f"{adls_path}/load/WORK_COMPNSTN_CLM_LN_SAV.dat"
# -------------------------------------------------------------------------------------------------------------------
write_files(
    df_BusinessLogic,
    f"{adls_path}/load/WORK_COMPNSTN_CLM_LN_SAV.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)