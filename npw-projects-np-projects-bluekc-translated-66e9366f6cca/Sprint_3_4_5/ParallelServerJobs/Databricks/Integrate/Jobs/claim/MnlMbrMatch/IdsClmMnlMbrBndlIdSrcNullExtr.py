# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY: IdsClmMnlMbrBndlIdCntl
# MAGIC 
# MAGIC PROCESSING: Read all records from P_CLM_MBRSH_ERR_RECYC table where MBR_MNL_MATCH_BUNDLE_ID is NULL or 0 and check if the combination of columns which are used to generate the BUNDLE_ID exist in the same P table. If yes, then use the same bundle id and assign it to the new records else create a new bundle id and assign the same to the new records.
# MAGIC 
# MAGIC *** This job works for rows where either of the below fields are NULL or blank ***
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
# MAGIC Developer                 Date                 Change Description                                                                                                                    Project #                     Development Project     Code Reviewer          Date Reviewed  
# MAGIC ------------------------------   --------------------     ------------------------------------------------------------------------------------------------------------------------------------------------   --------------------------------   ----------------------------------    ----------------------------      -------------------------
# MAGIC Kaushik Kapoor        08/06/2018      Original Programming                                                                                                                  5828 - Stop Loss        IntegrateDev2               Hugh Sisson               2018-08-13

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
# MAGIC 9) CLM_SK
# MAGIC 
# MAGIC *** This job works for rows where either of the below fields are NULL or blank ***
# MAGIC SUB_SSN, GRP_ID,  PATN_FNAME, PATN_LNAME, PATN_DOB and GNDR_CD
# MAGIC ******* This code exclude rows for SRC_SYS_CD = 'BCAMEDFEP' ******
# MAGIC All records where SUB_SSN
# MAGIC GRP_ID, PATN_FIRST_NM, PATN_LAST_NM, PATN_DOB and GNDR_CD are NULL OR <> 'UNK'
# MAGIC New Bundle Creation
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as f
from pyspark.sql import Window
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
extract_query_db2_P_ERR_RECYC = f"""
SELECT
    CLM_SK,
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
    MBR_MNL_MATCH_BUNDLE_ID,
    ROW_NUMBER() OVER (
        PARTITION BY
          SRC_SYS_CD,
          SUB_SSN,
          PATN_LAST_NM,
          PATN_FIRST_NM,
          PATN_GNDR_CD,
          PATN_BRTH_DT_SK,
          GRP_ID
        ORDER BY
          SRC_SYS_CD,
          SUB_SSN,
          PATN_LAST_NM,
          PATN_FIRST_NM,
          PATN_GNDR_CD,
          PATN_BRTH_DT_SK,
          GRP_ID ASC
    ) AS ROWNUM
FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC
WHERE
    (MBR_MNL_MATCH_BUNDLE_ID IS NULL OR MBR_MNL_MATCH_BUNDLE_ID = 0)
    AND SRC_SYS_CD NOT IN ('{ExcludeSrcSysCd}')
    AND (
        SUB_SSN IS NULL OR TRIM(SUB_SSN) = '' OR TRIM(SUB_SSN) = 'UNK'
        OR PATN_LAST_NM IS NULL OR TRIM(PATN_LAST_NM) = '' OR TRIM(PATN_LAST_NM) = 'UNK'
        OR PATN_FIRST_NM IS NULL OR TRIM(PATN_FIRST_NM) = '' OR TRIM(PATN_FIRST_NM) = 'UNK'
        OR PATN_GNDR_CD IS NULL OR TRIM(PATN_GNDR_CD) = '' OR TRIM(PATN_GNDR_CD) = 'UNK'
        OR PATN_BRTH_DT_SK IS NULL OR TRIM(PATN_BRTH_DT_SK) = '' OR TRIM(PATN_BRTH_DT_SK) = 'UNK'
        OR GRP_ID IS NULL OR TRIM(GRP_ID) = '' OR TRIM(GRP_ID) = 'UNK'
    )
"""
df_db2_P_ERR_RECYC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_P_ERR_RECYC)
    .load()
)

df_Transformer_177_lnk_Null_Bundle = df_db2_P_ERR_RECYC.filter(f.col("ROWNUM") == 1).select(
    f.col("CLM_SK").alias("CLM_SK"),
    f.col("CLM_ID").alias("CLM_ID"),
    f.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    f.col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    f.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    f.col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    f.col("SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    f.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    f.col("SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    f.col("SUB_SSN").alias("SUB_SSN"),
    f.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    f.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    f.col("PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    f.col("PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    f.col("ERR_CD").alias("ERR_CD"),
    f.col("ERR_DESC").alias("ERR_DESC"),
    f.col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    f.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    f.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    f.col("SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    f.col("SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    f.col("GRP_ID").alias("GRP_ID"),
    f.col("FILE_DT_SK").alias("FILE_DT_SK"),
    f.col("PATN_SSN").alias("PATN_SSN"),
    f.col("MBR_MNL_MATCH_BUNDLE_ID").alias("MBR_MNL_MATCH_BUNDLE_ID")
)

df_Transformer_177_DSLink180 = df_db2_P_ERR_RECYC.filter(f.col("ROWNUM") > 1).select(
    f.col("CLM_SK").alias("CLM_SK"),
    f.col("CLM_ID").alias("CLM_ID"),
    f.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    f.col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    f.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    f.col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    f.col("SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    f.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    f.col("SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    f.col("SUB_SSN").alias("SUB_SSN"),
    f.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    f.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    f.col("PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    f.col("PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    f.col("ERR_CD").alias("ERR_CD"),
    f.col("ERR_DESC").alias("ERR_DESC"),
    f.col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    f.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    f.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    f.col("SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    f.col("SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    f.col("GRP_ID").alias("GRP_ID"),
    f.col("FILE_DT_SK").alias("FILE_DT_SK"),
    f.col("PATN_SSN").alias("PATN_SSN"),
    f.col("MBR_MNL_MATCH_BUNDLE_ID").alias("MBR_MNL_MATCH_BUNDLE_ID")
)

w_xfm_Create_Bundle = Window.orderBy("CLM_SK")
df_xfm_Create_Bundle = (
    df_Transformer_177_lnk_Null_Bundle
    .withColumn("svRowNum", f.row_number().over(w_xfm_Create_Bundle))
    .withColumn("computed_bndl_id", f.lit(MaxBundle).cast(IntegerType()) + f.col("svRowNum"))
)

df_xfm_Create_Bundle_lnk_New_Bundle = df_xfm_Create_Bundle.select(
    f.col("CLM_SK").alias("CLM_SK"),
    f.col("CLM_ID").alias("CLM_ID"),
    f.col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    f.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    f.col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    f.col("SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    f.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    f.col("SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    f.col("ERR_CD").alias("ERR_CD"),
    f.col("ERR_DESC").alias("ERR_DESC"),
    f.col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    f.col("SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    f.col("SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    f.col("FILE_DT_SK").alias("FILE_DT_SK"),
    f.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    f.col("SUB_SSN").alias("SUB_SSN"),
    f.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    f.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    f.col("PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    f.col("PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    f.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    f.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    f.col("GRP_ID").alias("GRP_ID"),
    f.col("PATN_SSN").alias("PATN_SSN"),
    (f.col("computed_bndl_id")).alias("MBR_MNL_MATCH_BUNDLE_ID")
)

df_xfm_Create_Bundle_lnk_max_Bndl = df_xfm_Create_Bundle.select(
    f.lit(RunDate).alias("RUNDATE"),
    (f.col("computed_bndl_id")).alias("MBR_MNL_MATCH_BUNDLE_ID")
)

df_agg_Max_Bndl = (
    df_xfm_Create_Bundle_lnk_max_Bndl
    .groupBy("RUNDATE")
    .agg(f.max("MBR_MNL_MATCH_BUNDLE_ID").alias("MBR_MNL_MATCH_BUNDLE_ID"))
)

df_xfm_Convert_int = df_agg_Max_Bndl.select(
    f.col("MBR_MNL_MATCH_BUNDLE_ID").cast(IntegerType()).alias("MBR_MNL_MATCH_BUNDLE_ID")
)

df_seq_Max_Bundle_final = df_xfm_Convert_int.select("MBR_MNL_MATCH_BUNDLE_ID")
write_files(
    df_seq_Max_Bundle_final,
    f"{adls_path_publish}/external/P_CLM_MBRSH_ERR_RECYC_MAX_BNDL.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Copy_182_DSLink184 = df_xfm_Create_Bundle_lnk_New_Bundle.select(
    f.col("CLM_SK").alias("CLM_SK"),
    f.col("CLM_ID").alias("CLM_ID"),
    f.col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    f.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    f.col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    f.col("SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    f.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    f.col("SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    f.col("ERR_CD").alias("ERR_CD"),
    f.col("ERR_DESC").alias("ERR_DESC"),
    f.col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    f.col("SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    f.col("SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    f.col("FILE_DT_SK").alias("FILE_DT_SK"),
    f.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    f.col("SUB_SSN").alias("SUB_SSN"),
    f.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    f.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    f.col("PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    f.col("PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    f.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    f.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    f.col("GRP_ID").alias("GRP_ID"),
    f.col("PATN_SSN").alias("PATN_SSN"),
    f.col("MBR_MNL_MATCH_BUNDLE_ID").alias("MBR_MNL_MATCH_BUNDLE_ID")
)

df_Copy_182_DSLink186 = df_xfm_Create_Bundle_lnk_New_Bundle.select(
    f.col("CLM_SK").alias("CLM_SK"),
    f.col("CLM_ID").alias("CLM_ID"),
    f.col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    f.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    f.col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    f.col("SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    f.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    f.col("SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    f.col("ERR_CD").alias("ERR_CD"),
    f.col("ERR_DESC").alias("ERR_DESC"),
    f.col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    f.col("SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    f.col("SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    f.col("FILE_DT_SK").alias("FILE_DT_SK"),
    f.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    f.col("SUB_SSN").alias("SUB_SSN"),
    f.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    f.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    f.col("PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    f.col("PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    f.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    f.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    f.col("GRP_ID").alias("GRP_ID"),
    f.col("PATN_SSN").alias("PATN_SSN"),
    f.col("MBR_MNL_MATCH_BUNDLE_ID").alias("MBR_MNL_MATCH_BUNDLE_ID")
)

df_Lookup_187 = (
    df_Transformer_177_DSLink180.alias("DSLink180")
    .join(
        df_Copy_182_DSLink186.alias("DSLink186"),
        (
            (f.col("DSLink180.SRC_SYS_CD") == f.col("DSLink186.SRC_SYS_CD"))
            & (f.col("DSLink180.SUB_SSN") == f.col("DSLink186.SUB_SSN"))
            & (f.col("DSLink180.PATN_LAST_NM") == f.col("DSLink186.PATN_LAST_NM"))
            & (f.col("DSLink180.PATN_FIRST_NM") == f.col("DSLink186.PATN_FIRST_NM"))
            & (f.col("DSLink180.PATN_GNDR_CD") == f.col("DSLink186.PATN_GNDR_CD"))
            & (f.col("DSLink180.PATN_BRTH_DT_SK") == f.col("DSLink186.PATN_BRTH_DT_SK"))
            & (f.col("DSLink180.GRP_ID") == f.col("DSLink186.GRP_ID"))
        ),
        "inner"
    )
    .select(
        f.col("DSLink180.CLM_SK").alias("CLM_SK"),
        f.col("DSLink180.CLM_ID").alias("CLM_ID"),
        f.col("DSLink180.CLM_TYP_CD").alias("CLM_TYP_CD"),
        f.col("DSLink180.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
        f.col("DSLink180.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
        f.col("DSLink180.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
        f.col("DSLink180.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
        f.col("DSLink180.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
        f.col("DSLink180.ERR_CD").alias("ERR_CD"),
        f.col("DSLink180.ERR_DESC").alias("ERR_DESC"),
        f.col("DSLink180.FEP_MBR_ID").alias("FEP_MBR_ID"),
        f.col("DSLink180.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
        f.col("DSLink180.SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
        f.col("DSLink180.FILE_DT_SK").alias("FILE_DT_SK"),
        f.col("DSLink180.SRC_SYS_CD").alias("SRC_SYS_CD"),
        f.col("DSLink180.SUB_SSN").alias("SUB_SSN"),
        f.col("DSLink180.PATN_LAST_NM").alias("PATN_LAST_NM"),
        f.col("DSLink180.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
        f.col("DSLink180.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
        f.col("DSLink180.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
        f.col("DSLink180.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        f.col("DSLink180.SUB_LAST_NM").alias("SUB_LAST_NM"),
        f.col("DSLink180.GRP_ID").alias("GRP_ID"),
        f.col("DSLink180.PATN_SSN").alias("PATN_SSN"),
        f.col("DSLink186.MBR_MNL_MATCH_BUNDLE_ID").alias("MBR_MNL_MATCH_BUNDLE_ID")
    )
)

df_Transformer_193_DSLink194 = df_Lookup_187.select(
    f.col("CLM_SK").alias("CLM_SK"),
    f.col("CLM_ID").alias("CLM_ID"),
    f.col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    f.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    f.col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    f.col("SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    f.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    f.col("SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    f.col("ERR_CD").alias("ERR_CD"),
    f.col("ERR_DESC").alias("ERR_DESC"),
    f.col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    f.col("SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    f.col("SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    f.col("FILE_DT_SK").alias("FILE_DT_SK"),
    f.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    f.col("SUB_SSN").alias("SUB_SSN"),
    f.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    f.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    f.col("PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    f.col("PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    f.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    f.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    f.col("GRP_ID").alias("GRP_ID"),
    f.col("PATN_SSN").alias("PATN_SSN"),
    f.col("MBR_MNL_MATCH_BUNDLE_ID").alias("MBR_MNL_MATCH_BUNDLE_ID")
)

df_Funnel_190_DSLink184 = df_Copy_182_DSLink184.select(
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
df_Funnel_190_DSLink194 = df_Transformer_193_DSLink194.select(
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

df_Funnel_190 = df_Funnel_190_DSLink184.unionByName(df_Funnel_190_DSLink194)

df_seq_P_CLM_MBRSH_ERR_RECYC_Load_final = (
    df_Funnel_190
    .withColumn("CLM_SVC_STRT_DT_SK", rpad(f.col("CLM_SVC_STRT_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_MBR_SFX_NO", rpad(f.col("SRC_SYS_MBR_SFX_NO"), 3, " "))
    .withColumn("FILE_DT_SK", rpad(f.col("FILE_DT_SK"), 10, " "))
    .select(
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
)

write_files(
    df_seq_P_CLM_MBRSH_ERR_RECYC_Load_final,
    f"{adls_path}/load/P_CLM_MBRSH_ERR_RECYC_BNDL.dat",
    delimiter="|",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)