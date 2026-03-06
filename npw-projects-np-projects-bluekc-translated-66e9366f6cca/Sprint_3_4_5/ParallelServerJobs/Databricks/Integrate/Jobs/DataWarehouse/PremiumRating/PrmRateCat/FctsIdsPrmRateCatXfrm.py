# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  FctsIdsPrmRateCatXfm
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC     Applies Strip Field and Transformation rules for Premium Rating infromation 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC 
# MAGIC Jag Yelavarthi                    2014-04-09       #5345                            Parallel rewrite from Server job                            IntegrateWrhsDevl      SAndrew                      2014-10-20

# MAGIC These lookups and valdations are added here to support Balancing table needs
# MAGIC JobName: FctsIdsPrmRateCatXfm
# MAGIC 
# MAGIC Strip field function and Tranforamtion rules are applied on the data Extracted from Facets
# MAGIC Strip Field function is applied here on all the TEXT fields
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad, concat_ws
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

# The following parameters come from the job
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunID = get_widget_value('RunID','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')

# --------------------------------------------------------------------------------
# Stage: ds_PrmRateCatExtr (PxDataSet)
# --------------------------------------------------------------------------------
df_ds_PrmRateCatExtr = spark.read.parquet(
    f"{adls_path}/ds/PRM_RATE_CAT.{SrcSysCd}.extr.{RunID}.parquet"
)
df_ds_PrmRateCatExtr = df_ds_PrmRateCatExtr.select(
    col("PDRT_CK"),
    col("PDRC_MODIFIER"),
    col("PDRC_SEX_CD"),
    col("PDRC_SMOKER_CD"),
    col("PDRC_EFF_DT"),
    col("PDRC_TERM_DT"),
    col("PDRA_REF_DTM")
)

# --------------------------------------------------------------------------------
# Stage: StripField (CTransformerStage)
# --------------------------------------------------------------------------------
df_StripField_out = df_ds_PrmRateCatExtr.select(
    col("PDRT_CK").alias("PDRT_CK"),
    trim(strip_field(col("PDRC_MODIFIER"))).alias("PDRC_MODIFIER"),
    strip_field(col("PDRC_SEX_CD")).alias("PDRC_SEX_CD"),
    strip_field(col("PDRC_SMOKER_CD")).alias("PDRC_SMOKER_CD"),
    col("PDRC_EFF_DT").alias("PDRC_EFF_DT"),
    col("PDRC_TERM_DT").alias("PDRC_TERM_DT"),
    col("PDRA_REF_DTM").alias("PDRA_REF_DTM")
)

# --------------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
#   This stage outputs two links:
#     1) lnk_PrmRateCat_Out
#     2) BalanceData
# --------------------------------------------------------------------------------
# Input to BusinessRules
df_BusinessRules_in = df_StripField_out

# lnk_PrmRateCat_Out
df_BusinessRules_out_PrmRateCat_Out = df_BusinessRules_in.select(
    concat_ws(";",
        col("PDRT_CK"),
        col("PDRC_MODIFIER"),
        col("PDRC_SEX_CD"),
        col("PDRC_SMOKER_CD"),
        col("PDRC_EFF_DT"),
        lit(SrcSysCd)
    ).alias("PRI_NAT_KEY_STRING"),
    lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    lit(0).alias("PRM_RATE_CAT_SK"),
    col("PDRT_CK").alias("PRM_RATE_UNIQ_KEY"),
    col("PDRC_MODIFIER").alias("TIER_MOD_ID"),
    col("PDRC_SEX_CD").alias("PRM_RATE_CAT_GNDR_CD"),
    col("PDRC_SMOKER_CD").alias("PRM_RATE_CAT_SMKR_CD"),
    # Stage variable svEffDt = FORMAT.DATE.EE(...); assume it's a pre-defined UDF
    format_date_ee(col("PDRC_EFF_DT"), lit("SYBASE"), lit("TIMESTAMP"), lit("CCYY-MM-DD")).alias("EFF_DT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    lit(0).alias("PRM_RATE_SK"),
    format_date_ee(col("PDRC_TERM_DT"), lit("SYBASE"), lit("TIMESTAMP"), lit("CCYY-MM-DD")).alias("TERM_DT"),
    col("PDRA_REF_DTM").alias("AGE_BAND_REF_ID")
)

# BalanceData
df_BusinessRules_out_BalanceData = df_BusinessRules_in.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("PDRT_CK").alias("PRM_RATE_UNIQ_KEY"),
    col("PDRC_MODIFIER").alias("TIER_MOD_ID"),
    col("PDRC_SEX_CD").alias("PRM_RATE_CAT_GNDR_CD"),
    col("PDRC_SMOKER_CD").alias("PRM_RATE_CAT_SMKR_CD"),
    format_date_ee(col("PDRC_EFF_DT"), lit("SYBASE"), lit("TIMESTAMP"), lit("CCYY-MM-DD")).alias("EFF_DT")
)

# --------------------------------------------------------------------------------
# Stage: ds_PrmRateCatXfm (PxDataSet) - Write
# --------------------------------------------------------------------------------
# Write df_BusinessRules_out_PrmRateCat_Out to parquet, preserving column order and applying rpad where needed
ds_PrmRateCatXfm_cols = [
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "PRM_RATE_CAT_SK",
    "PRM_RATE_UNIQ_KEY",
    "TIER_MOD_ID",
    "PRM_RATE_CAT_GNDR_CD",
    "PRM_RATE_CAT_SMKR_CD",
    "EFF_DT",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SRC_SYS_CD_SK",
    "PRM_RATE_SK",
    "TERM_DT",
    "AGE_BAND_REF_ID"
]

df_ds_PrmRateCatXfm = df_BusinessRules_out_PrmRateCat_Out.select(
    col("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("PRM_RATE_CAT_SK"),
    col("PRM_RATE_UNIQ_KEY"),
    rpad(col("TIER_MOD_ID"), 12, " ").alias("TIER_MOD_ID"),
    rpad(col("PRM_RATE_CAT_GNDR_CD"), 1, " ").alias("PRM_RATE_CAT_GNDR_CD"),
    rpad(col("PRM_RATE_CAT_SMKR_CD"), 1, " ").alias("PRM_RATE_CAT_SMKR_CD"),
    rpad(col("EFF_DT"), 10, " ").alias("EFF_DT"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SRC_SYS_CD_SK"),
    col("PRM_RATE_SK"),
    rpad(col("TERM_DT"), 10, " ").alias("TERM_DT"),
    col("AGE_BAND_REF_ID")
)

write_files(
    df_ds_PrmRateCatXfm,
    f"PRM_RATE_CAT.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: db2_Clndr_Dt_Eff_Dt_Lkp (DB2ConnectorPX) - Read from IDS
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids = f"""
SELECT DISTINCT
 CLNDR_DT_SK,
 CLNDR_DT
FROM {IDSOwner}.CLNDR_DT
WHERE CLNDR_DT_SK NOT IN ('INVALID','NA','NULL','UNK')
"""
df_db2_Clndr_Dt_Eff_Dt_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids.strip())
    .load()
)

# --------------------------------------------------------------------------------
# Stage: ds_CD_MPPNG_LkpData (PxDataSet) - Read
# --------------------------------------------------------------------------------
df_ds_CD_MPPNG_LkpData = spark.read.parquet(
    f"{adls_path}/ds/CD_MPPNG.parquet"
)
df_ds_CD_MPPNG_LkpData = df_ds_CD_MPPNG_LkpData.select(
    col("CD_MPPNG_SK"),
    col("SRC_CD"),
    col("SRC_CD_NM"),
    col("SRC_CLCTN_CD"),
    col("SRC_DRVD_LKUP_VAL"),
    col("SRC_DOMAIN_NM"),
    col("SRC_SYS_CD"),
    col("TRGT_CD"),
    col("TRGT_CD_NM"),
    col("TRGT_CLCTN_CD"),
    col("TRGT_DOMAIN_NM")
)

# --------------------------------------------------------------------------------
# Stage: fltr_FilterData (PxFilter)
#   Split into lnkGenderLkpData (OutputLink 0) and lnkSmokerLkpData (OutputLink 1)
# --------------------------------------------------------------------------------
df_fltr_FilterData_1 = df_ds_CD_MPPNG_LkpData.filter(
    (col("SRC_SYS_CD") == "FACETS") &
    (col("SRC_CLCTN_CD") == "FACETS DBO") &
    (col("TRGT_CLCTN_CD") == "IDS") &
    (col("SRC_DOMAIN_NM") == "GENDER") &
    (col("TRGT_DOMAIN_NM") == "GENDER")
)

df_fltr_FilterData_2 = df_ds_CD_MPPNG_LkpData.filter(
    (col("SRC_SYS_CD") == "FACETS") &
    (col("SRC_CLCTN_CD") == "FACETS DBO") &
    (col("TRGT_CLCTN_CD") == "IDS") &
    (col("SRC_DOMAIN_NM") == "PREMIUM RATE CATEGORY SMOKER") &
    (col("TRGT_DOMAIN_NM") == "PREMIUM RATE CATEGORY SMOKER")
)

df_lnkGenderLkpData = df_fltr_FilterData_1.select(
    col("SRC_CD").alias("SRC_CD"),
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD")
)

df_lnkSmokerLkpData = df_fltr_FilterData_2.select(
    col("SRC_CD").alias("SRC_CD"),
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD")
)

# --------------------------------------------------------------------------------
# Stage: lkp_Codes (PxLookup)
#   Primary link: df_BusinessRules_out_BalanceData ("BalanceData")
#   Lookup link 1: df_lnkGenderLkpData (left join on PRM_RATE_CAT_GNDR_CD = SRC_CD)
#   Lookup link 2: df_lnkSmokerLkpData (left join on PRM_RATE_CAT_SMKR_CD = SRC_CD)
#   Lookup link 3: df_db2_Clndr_Dt_Eff_Dt_Lkp (left join on EFF_DT = CLNDR_DT_SK)
# --------------------------------------------------------------------------------
df_lkp_Codes_joined = (
    df_BusinessRules_out_BalanceData
    .alias("BalanceData")
    .join(
        df_lnkGenderLkpData.alias("lnkGenderLkpData"),
        on=[col("BalanceData.PRM_RATE_CAT_GNDR_CD") == col("lnkGenderLkpData.SRC_CD")],
        how="left"
    )
    .join(
        df_lnkSmokerLkpData.alias("lnkSmokerLkpData"),
        on=[col("BalanceData.PRM_RATE_CAT_SMKR_CD") == col("lnkSmokerLkpData.SRC_CD")],
        how="left"
    )
    .join(
        df_db2_Clndr_Dt_Eff_Dt_Lkp.alias("Ref_Eff_Dt"),
        on=[col("BalanceData.EFF_DT") == col("Ref_Eff_Dt.CLNDR_DT_SK")],
        how="left"
    )
)

df_lkp_Codes = df_lkp_Codes_joined.select(
    col("BalanceData.PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
    col("BalanceData.TIER_MOD_ID").alias("TIER_MOD_ID"),
    col("lnkGenderLkpData.TRGT_CD").alias("PRM_RATE_CAT_GNDR_CD"),
    col("lnkSmokerLkpData.TRGT_CD").alias("PRM_RATE_CAT_SMKR_CD"),
    col("Ref_Eff_Dt.CLNDR_DT_SK").alias("EFF_DT_SK"),
    col("BalanceData.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)

# --------------------------------------------------------------------------------
# Stage: xfm_LkpValidation (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfm_LkpValidation_out = df_lkp_Codes.select(
    col("PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
    col("TIER_MOD_ID").alias("TIER_MOD_ID"),
    when(col("PRM_RATE_CAT_GNDR_CD").isNull(), "UNK").otherwise(col("PRM_RATE_CAT_GNDR_CD")).alias("PRM_RATE_CAT_GNDR_CD"),
    when(col("PRM_RATE_CAT_SMKR_CD").isNull(), "UNK").otherwise(col("PRM_RATE_CAT_SMKR_CD")).alias("PRM_RATE_CAT_SMKR_CD"),
    when(col("EFF_DT_SK").isNull(), "1753-01-01").otherwise(col("EFF_DT_SK")).alias("EFF_DT_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)

# --------------------------------------------------------------------------------
# Stage: seq_B_PRM_RATE_CAT (PxSequentialFile) - Write .dat
# --------------------------------------------------------------------------------
# Preserve column order, apply rpad if needed
df_seq_B_PRM_RATE_CAT = df_xfm_LkpValidation_out.select(
    col("PRM_RATE_UNIQ_KEY"),
    rpad(col("TIER_MOD_ID"), 12, " ").alias("TIER_MOD_ID"),
    rpad(col("PRM_RATE_CAT_GNDR_CD"), 1, " ").alias("PRM_RATE_CAT_GNDR_CD"),
    rpad(col("PRM_RATE_CAT_SMKR_CD"), 1, " ").alias("PRM_RATE_CAT_SMKR_CD"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    col("SRC_SYS_CD_SK")
)

write_files(
    df_seq_B_PRM_RATE_CAT,
    f"{adls_path}/load/B_PRM_RATE_CAT.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)