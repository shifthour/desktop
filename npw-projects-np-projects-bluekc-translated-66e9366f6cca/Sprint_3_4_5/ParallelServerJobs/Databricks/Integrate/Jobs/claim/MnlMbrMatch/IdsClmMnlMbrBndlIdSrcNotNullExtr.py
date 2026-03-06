# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY: IdsClmMnlMbrBndlIdCntl
# MAGIC 
# MAGIC PROCESSING: Read all records from P_CLM_MBRSH_ERR_RECYC table where MBR_MNL_MATCH_BUNDLE_ID is NULL or 0 and check if the combination of columns which are used to generate the BUNDLE_ID exist in the same P table. If yes, then use the same bundle id and assign it to the new records else create a new bundle id and assign the same to the new records.
# MAGIC 
# MAGIC *** This job works for rows where below fields are NOT NULL or not blank ***
# MAGIC 
# MAGIC SUB_SSN
# MAGIC GRP_ID
# MAGIC PATN_FIRST_NM
# MAGIC PATN_LAST_NM
# MAGIC PATN_DOB 
# MAGIC GNDR_CD
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                  Date                 Change Description                                                                                                        Project #                        Development Project       Code Reviewer          Date Reviewed  
# MAGIC -------------------------------   --------------------     -----------------------------------------------------------------------------------------------------------------------------------    ----------------------------------    ------------------------------------    ----------------------------      -------------------------
# MAGIC Kaushik Kapoor         07/19/2018      Original Programming                                                                                                      5828 - Stop Loss           IntegrateDev2                  Hugh Sisson              2018-08-13

# MAGIC This job will create new or use existing Bundle_ID based on the below mentioned columns. If a match found in P_CLM_MBRSH_ERR_RECYC table based on below columns then existing BUNDLE_ID will be used and assigned it to the new rows else new BUNDLE_ID will be created and eventually will create a Load Ready file.
# MAGIC **Column Names used for BUNDLE_ID**
# MAGIC 
# MAGIC 1) SRC_SYS_CD - Use BCBSSC for both Medical and Pharmacy and for rest use the actual SRC_SYS_CD
# MAGIC 
# MAGIC 2) PATN_SSN
# MAGIC 3) PATN_FNAME
# MAGIC 4) PATN_LNAME
# MAGIC 5) PATN_DOB
# MAGIC 6) PATN_GNDR
# MAGIC 7) SUB_SSN
# MAGIC 8) GRP_ID
# MAGIC 
# MAGIC *** This job works for rows where below fields are NOT NULL or not blank ***
# MAGIC SUB_SSN, GRP_ID,  PATN_FNAME, PATN_LNAME, PATN_DOB and GNDR_CD
# MAGIC ******* This code exclude rows for SRC_SYS_CD = 'BCAMEDFEP' ******
# MAGIC Grouped on SUB_SSN, GRP_ID, PATN_FIRST_NM, PATN_LAST_NM, PATN_DOB, GNDR_CD, PATN_SSN and SRC_SYS_CD
# MAGIC All records where SUB_SSN
# MAGIC GRP_ID, PATN_FIRST_NM, PATN_LAST_NM, PATN_DOB and GNDR_CD are NOT NULL and <> 'UNK'
# MAGIC New Bundle Creation
# MAGIC All records where BUNDLE_ID is already assigned
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
MaxBundle = get_widget_value('MaxBundle','')
RunDate = get_widget_value('RunDate','')
ExcludeSrcSysCd = get_widget_value('ExcludeSrcSysCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

query_db2_P_ERR_RECYC = f"""
SELECT  CLM_SK,
        CLM_ID,
        CASE WHEN LEFT(TRIM(SRC_SYS_CD),6) = 'BCBSSC' THEN 'BCBSSC' ELSE TRIM(SRC_SYS_CD) END AS SRC_SYS_CD,
        CLM_TYP_CD,
        CLM_SUBTYP_CD,
        CLM_SVC_STRT_DT_SK,
        SRC_SYS_GRP_PFX,
        SRC_SYS_GRP_ID,
        SRC_SYS_GRP_SFX,
        SUB_SSN,
        PATN_LAST_NM,
        PATN_FIRST_NM,
        PATN_GNDR_CD,
        PATN_BRTH_DT_SK,
        ERR_CD,
        ERR_DESC,
        FEP_MBR_ID,
        SUB_FIRST_NM,
        SUB_LAST_NM,
        SRC_SYS_SUB_ID,
        SRC_SYS_MBR_SFX_NO,
        GRP_ID,
        FILE_DT_SK,
        PATN_SSN,
        MBR_MNL_MATCH_BUNDLE_ID
FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC
WHERE (MBR_MNL_MATCH_BUNDLE_ID IS NULL OR MBR_MNL_MATCH_BUNDLE_ID = 0)
  AND SRC_SYS_CD NOT IN ('{ExcludeSrcSysCd}')
  AND (SUB_SSN IS NOT NULL AND TRIM(SUB_SSN) <> '' AND TRIM(SUB_SSN) <> 'UNK'
       AND PATN_LAST_NM IS NOT NULL AND TRIM(PATN_LAST_NM) <> '' AND TRIM(PATN_LAST_NM) <> 'UNK'
       AND PATN_FIRST_NM IS NOT NULL AND TRIM(PATN_FIRST_NM) <> '' AND TRIM(PATN_FIRST_NM) <> 'UNK'
       AND PATN_GNDR_CD IS NOT NULL AND TRIM(PATN_GNDR_CD) <> '' AND TRIM(PATN_GNDR_CD) <> 'UNK'
       AND PATN_BRTH_DT_SK IS NOT NULL AND TRIM(PATN_BRTH_DT_SK) <> '' AND TRIM(PATN_BRTH_DT_SK) <> 'UNK'
       AND GRP_ID IS NOT NULL AND TRIM(GRP_ID) <> '' AND TRIM(GRP_ID) <> 'UNK')
"""

df_db2_P_ERR_RECYC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_P_ERR_RECYC)
    .load()
)

query_db2_Key_Bundle_Err_Recyc = f"""
SELECT DISTINCT
       CASE WHEN LEFT(TRIM(SRC_SYS_CD),6) = 'BCBSSC' THEN 'BCBSSC' ELSE TRIM(SRC_SYS_CD) END AS SRC_SYS_CD,
       SUB_SSN,
       PATN_LAST_NM,
       PATN_FIRST_NM,
       PATN_GNDR_CD,
       PATN_BRTH_DT_SK,
       GRP_ID,
       MBR_MNL_MATCH_BUNDLE_ID
FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC
WHERE MBR_MNL_MATCH_BUNDLE_ID IS NOT NULL
  AND MBR_MNL_MATCH_BUNDLE_ID <> 0
  AND SRC_SYS_CD NOT IN ('{ExcludeSrcSysCd}')
"""

df_db2_Key_Bundle_Err_Recyc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_Key_Bundle_Err_Recyc)
    .load()
)

df_lkp_Err_Recyc = (
    df_db2_P_ERR_RECYC.alias("lnk_Null_Bundle")
    .join(
        df_db2_Key_Bundle_Err_Recyc.alias("lnk_Key_Bundle"),
        (
            (F.col("lnk_Null_Bundle.SRC_SYS_CD") == F.col("lnk_Key_Bundle.SRC_SYS_CD"))
            & (F.col("lnk_Null_Bundle.SUB_SSN") == F.col("lnk_Key_Bundle.SUB_SSN"))
            & (F.col("lnk_Null_Bundle.PATN_LAST_NM") == F.col("lnk_Key_Bundle.PATN_LAST_NM"))
            & (F.col("lnk_Null_Bundle.PATN_FIRST_NM") == F.col("lnk_Key_Bundle.PATN_FIRST_NM"))
            & (F.col("lnk_Null_Bundle.PATN_GNDR_CD") == F.col("lnk_Key_Bundle.PATN_GNDR_CD"))
            & (F.col("lnk_Null_Bundle.PATN_BRTH_DT_SK") == F.col("lnk_Key_Bundle.PATN_BRTH_DT_SK"))
            & (F.col("lnk_Null_Bundle.GRP_ID") == F.col("lnk_Key_Bundle.GRP_ID"))
        ),
        "left"
    )
)

df_lkp_Err_Recyc_matched = df_lkp_Err_Recyc.filter(F.col("lnk_Key_Bundle.MBR_MNL_MATCH_BUNDLE_ID").isNotNull())
df_lkp_Err_Recyc_notmatched = df_lkp_Err_Recyc.filter(F.col("lnk_Key_Bundle.MBR_MNL_MATCH_BUNDLE_ID").isNull())

df_lnk_Existing_Bndl = df_lkp_Err_Recyc_matched.select(
    F.col("lnk_Null_Bundle.CLM_SK").alias("CLM_SK"),
    F.col("lnk_Null_Bundle.CLM_ID").alias("CLM_ID"),
    F.col("lnk_Null_Bundle.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_Null_Bundle.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("lnk_Null_Bundle.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("lnk_Null_Bundle.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("lnk_Null_Bundle.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("lnk_Null_Bundle.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("lnk_Null_Bundle.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("lnk_Null_Bundle.SUB_SSN").alias("SUB_SSN"),
    F.col("lnk_Null_Bundle.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("lnk_Null_Bundle.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("lnk_Null_Bundle.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("lnk_Null_Bundle.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("lnk_Null_Bundle.ERR_CD").alias("ERR_CD"),
    F.col("lnk_Null_Bundle.ERR_DESC").alias("ERR_DESC"),
    F.col("lnk_Null_Bundle.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("lnk_Null_Bundle.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("lnk_Null_Bundle.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("lnk_Null_Bundle.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    F.col("lnk_Null_Bundle.SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    F.col("lnk_Null_Bundle.GRP_ID").alias("GRP_ID"),
    F.col("lnk_Null_Bundle.FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("lnk_Null_Bundle.PATN_SSN").alias("PATN_SSN"),
    F.col("lnk_Key_Bundle.MBR_MNL_MATCH_BUNDLE_ID").alias("MBR_MNL_MATCH_BUNDLE_ID")
)

df_lnk_MatchNotFound = df_lkp_Err_Recyc_notmatched.select(
    F.col("lnk_Null_Bundle.CLM_SK").alias("CLM_SK"),
    F.col("lnk_Null_Bundle.CLM_ID").alias("CLM_ID"),
    F.col("lnk_Null_Bundle.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_Null_Bundle.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("lnk_Null_Bundle.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("lnk_Null_Bundle.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("lnk_Null_Bundle.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("lnk_Null_Bundle.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("lnk_Null_Bundle.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("lnk_Null_Bundle.SUB_SSN").alias("SUB_SSN"),
    F.col("lnk_Null_Bundle.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("lnk_Null_Bundle.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("lnk_Null_Bundle.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("lnk_Null_Bundle.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("lnk_Null_Bundle.ERR_CD").alias("ERR_CD"),
    F.col("lnk_Null_Bundle.ERR_DESC").alias("ERR_DESC"),
    F.col("lnk_Null_Bundle.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("lnk_Null_Bundle.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("lnk_Null_Bundle.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("lnk_Null_Bundle.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    F.col("lnk_Null_Bundle.SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    F.col("lnk_Null_Bundle.GRP_ID").alias("GRP_ID"),
    F.col("lnk_Null_Bundle.FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("lnk_Null_Bundle.PATN_SSN").alias("PATN_SSN")
)

df_cp_Unmatched_All_Data = df_lnk_MatchNotFound.select(
    "CLM_SK",
    "CLM_ID",
    "SRC_SYS_CD",
    "CLM_TYP_CD",
    "CLM_SUBTYP_CD",
    "CLM_SVC_STRT_DT_SK",
    "SRC_SYS_GRP_PFX",
    "SRC_SYS_GRP_ID",
    "SRC_SYS_GRP_SFX",
    "SUB_SSN",
    "PATN_LAST_NM",
    "PATN_FIRST_NM",
    "PATN_GNDR_CD",
    "PATN_BRTH_DT_SK",
    "ERR_CD",
    "ERR_DESC",
    "FEP_MBR_ID",
    "SUB_FIRST_NM",
    "SUB_LAST_NM",
    "SRC_SYS_SUB_ID",
    "SRC_SYS_MBR_SFX_NO",
    "GRP_ID",
    "FILE_DT_SK",
    "PATN_SSN"
)

df_cp_Unmatched_Agg = df_lnk_MatchNotFound.select(
    "SRC_SYS_CD",
    "SUB_SSN",
    "PATN_LAST_NM",
    "PATN_FIRST_NM",
    "PATN_GNDR_CD",
    "PATN_BRTH_DT_SK",
    "GRP_ID",
    "PATN_SSN"
)

df_rd_Keep_First = dedup_sort(
    df_cp_Unmatched_Agg,
    [
        "SRC_SYS_CD",
        "GRP_ID",
        "SUB_SSN",
        "PATN_FIRST_NM",
        "PATN_LAST_NM",
        "PATN_BRTH_DT_SK",
        "PATN_GNDR_CD"
    ],
    []
)

df_lnk_Create_Bundle = df_rd_Keep_First.select(
    "SRC_SYS_CD",
    "SUB_SSN",
    "PATN_LAST_NM",
    "PATN_FIRST_NM",
    "PATN_GNDR_CD",
    "PATN_BRTH_DT_SK",
    "GRP_ID",
    "PATN_SSN"
)

df_xfm_Create_Bundle = df_lnk_Create_Bundle.withColumn(
    "svRowNum", F.monotonically_increasing_id() + F.lit(1)
)

df_lnk_New_Bundle = df_xfm_Create_Bundle.select(
    "SRC_SYS_CD",
    "SUB_SSN",
    "PATN_LAST_NM",
    "PATN_FIRST_NM",
    "PATN_GNDR_CD",
    "PATN_BRTH_DT_SK",
    "GRP_ID",
    (
        F.lit(MaxBundle).cast("long") + F.col("svRowNum")
    ).alias("MBR_MNL_MATCH_BUNDLE_ID")
)

df_lnk_max_Bndl = df_xfm_Create_Bundle.select(
    F.lit(RunDate).alias("RUNDATE"),
    (
        F.lit(MaxBundle).cast("long") + F.col("svRowNum")
    ).alias("MBR_MNL_MATCH_BUNDLE_ID")
)

df_agg_Max_Bndl = (
    df_lnk_max_Bndl.groupBy("RUNDATE")
    .agg(F.max("MBR_MNL_MATCH_BUNDLE_ID").alias("MBR_MNL_MATCH_BUNDLE_ID"))
)

df_xfm_Convert_Int = df_agg_Max_Bndl.withColumn(
    "MBR_MNL_MATCH_BUNDLE_ID",
    F.col("MBR_MNL_MATCH_BUNDLE_ID").cast(IntegerType())
)

df_seqf_Max_Bundle = df_xfm_Convert_Int.select("MBR_MNL_MATCH_BUNDLE_ID")

write_files(
    df_seqf_Max_Bundle,
    f"{adls_path_publish}/external/P_CLM_MBRSH_ERR_RECYC_MAX_BNDL.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)

df_jn_New_Bndl = df_cp_Unmatched_All_Data.alias("lnk_All_Data").join(
    df_lnk_New_Bundle.alias("lnk_New_Bundle"),
    (
        (F.col("lnk_All_Data.GRP_ID") == F.col("lnk_New_Bundle.GRP_ID"))
        & (F.col("lnk_All_Data.PATN_BRTH_DT_SK") == F.col("lnk_New_Bundle.PATN_BRTH_DT_SK"))
        & (F.col("lnk_All_Data.PATN_FIRST_NM") == F.col("lnk_New_Bundle.PATN_FIRST_NM"))
        & (F.col("lnk_All_Data.PATN_GNDR_CD") == F.col("lnk_New_Bundle.PATN_GNDR_CD"))
        & (F.col("lnk_All_Data.PATN_LAST_NM") == F.col("lnk_New_Bundle.PATN_LAST_NM"))
        & (F.col("lnk_All_Data.SRC_SYS_CD") == F.col("lnk_New_Bundle.SRC_SYS_CD"))
        & (F.col("lnk_All_Data.SUB_SSN") == F.col("lnk_New_Bundle.SUB_SSN"))
    ),
    "inner"
)

df_jn_New_Bndl_out = df_jn_New_Bndl.select(
    F.col("lnk_All_Data.CLM_SK").alias("CLM_SK"),
    F.col("lnk_All_Data.CLM_ID").alias("CLM_ID"),
    F.col("lnk_All_Data.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("lnk_All_Data.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("lnk_All_Data.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("lnk_All_Data.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("lnk_All_Data.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("lnk_All_Data.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("lnk_All_Data.ERR_CD").alias("ERR_CD"),
    F.col("lnk_All_Data.ERR_DESC").alias("ERR_DESC"),
    F.col("lnk_All_Data.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("lnk_All_Data.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    F.col("lnk_All_Data.SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    F.col("lnk_All_Data.FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("lnk_All_Data.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_All_Data.SUB_SSN").alias("SUB_SSN"),
    F.col("lnk_All_Data.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("lnk_All_Data.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("lnk_All_Data.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("lnk_All_Data.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("lnk_All_Data.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("lnk_All_Data.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("lnk_All_Data.GRP_ID").alias("GRP_ID"),
    F.col("lnk_All_Data.PATN_SSN").alias("PATN_SSN"),
    F.col("lnk_New_Bundle.MBR_MNL_MATCH_BUNDLE_ID").alias("MBR_MNL_MATCH_BUNDLE_ID")
)

df_fnl_Merge_All_Existing = df_lnk_Existing_Bndl.select(
    "CLM_SK",
    "CLM_ID",
    "CLM_TYP_CD",
    "CLM_SUBTYP_CD",
    "CLM_SVC_STRT_DT_SK",
    "SRC_SYS_GRP_PFX",
    "SRC_SYS_GRP_ID",
    "SRC_SYS_GRP_SFX",
    "ERR_CD",
    "ERR_DESC",
    "FEP_MBR_ID",
    "SRC_SYS_SUB_ID",
    "SRC_SYS_MBR_SFX_NO",
    "FILE_DT_SK",
    "SRC_SYS_CD",
    "SUB_SSN",
    "PATN_LAST_NM",
    "PATN_FIRST_NM",
    "PATN_GNDR_CD",
    "PATN_BRTH_DT_SK",
    "SUB_FIRST_NM",
    "SUB_LAST_NM",
    "GRP_ID",
    "PATN_SSN",
    "MBR_MNL_MATCH_BUNDLE_ID"
)

df_fnl_Merge_All_New = df_jn_New_Bndl_out.select(
    "CLM_SK",
    "CLM_ID",
    "CLM_TYP_CD",
    "CLM_SUBTYP_CD",
    "CLM_SVC_STRT_DT_SK",
    "SRC_SYS_GRP_PFX",
    "SRC_SYS_GRP_ID",
    "SRC_SYS_GRP_SFX",
    "ERR_CD",
    "ERR_DESC",
    "FEP_MBR_ID",
    "SRC_SYS_SUB_ID",
    "SRC_SYS_MBR_SFX_NO",
    "FILE_DT_SK",
    "SRC_SYS_CD",
    "SUB_SSN",
    "PATN_LAST_NM",
    "PATN_FIRST_NM",
    "PATN_GNDR_CD",
    "PATN_BRTH_DT_SK",
    "SUB_FIRST_NM",
    "SUB_LAST_NM",
    "GRP_ID",
    "PATN_SSN",
    "MBR_MNL_MATCH_BUNDLE_ID"
)

df_fnl_Merge_All = df_fnl_Merge_All_Existing.unionByName(df_fnl_Merge_All_New)

df_final = df_fnl_Merge_All.select(
    F.col("CLM_SK"),
    F.col("CLM_ID"),
    F.col("CLM_TYP_CD"),
    F.col("CLM_SUBTYP_CD"),
    F.rpad(F.col("CLM_SVC_STRT_DT_SK"), 10, " ").alias("CLM_SVC_STRT_DT_SK"),
    F.col("SRC_SYS_GRP_PFX"),
    F.col("SRC_SYS_GRP_ID"),
    F.col("SRC_SYS_GRP_SFX"),
    F.col("ERR_CD"),
    F.col("ERR_DESC"),
    F.col("FEP_MBR_ID"),
    F.col("SRC_SYS_SUB_ID"),
    F.rpad(F.col("SRC_SYS_MBR_SFX_NO"), 3, " ").alias("SRC_SYS_MBR_SFX_NO"),
    F.rpad(F.col("FILE_DT_SK"), 10, " ").alias("FILE_DT_SK"),
    F.col("SRC_SYS_CD"),
    F.col("SUB_SSN"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM"),
    F.col("PATN_GNDR_CD"),
    F.rpad(F.col("PATN_BRTH_DT_SK"), 10, " ").alias("PATN_BRTH_DT_SK"),
    F.col("SUB_FIRST_NM"),
    F.col("SUB_LAST_NM"),
    F.col("GRP_ID"),
    F.col("PATN_SSN"),
    F.col("MBR_MNL_MATCH_BUNDLE_ID")
)

write_files(
    df_final,
    f"{adls_path}/load/P_CLM_MBRSH_ERR_RECYC_BNDL.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)