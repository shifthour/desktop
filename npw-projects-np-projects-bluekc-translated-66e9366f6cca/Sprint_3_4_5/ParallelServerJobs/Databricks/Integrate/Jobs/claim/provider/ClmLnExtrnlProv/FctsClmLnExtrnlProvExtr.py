# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2024 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FctsClmLnExtrnlProvExtr
# MAGIC 
# MAGIC DESCRIPTION:  Pulls data from CMC_CDPP_LI_ITS_PR
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	odbc_Facets_CMC_CDPP_LI_ITS_PR 
# MAGIC                 Joined to
# MAGIC                 TmpClaim to get specific records by date
# MAGIC   
# MAGIC HASH FILES:  hf_clm_nasco_dup_bypass
# MAGIC                            
# MAGIC PROCESSING:    Output file is created with a temp. name.  
# MAGIC 
# MAGIC OUTPUTS:   Sequential file .
# MAGIC MODIFICATIONS:
# MAGIC Developer                      Date                 Project/Altiris #      Change Description                                             Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                   --------------------     ------------------------      -----------------------------------------------------------------------      --------------------------------       -------------------------------   ----------------------------       
# MAGIC Revathi BoojiReddy      2024-06-03          US 616333            Original Programing                                               IntegrateDev1              Jeyaprasanna             2024-06-27

# MAGIC Apply business logic
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Extract Facets Claim Line External Provider Data
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnExtrnlProvPK
# COMMAND ----------

RunID = get_widget_value("RunID", "")
DriverTable = get_widget_value("DriverTable", "")
FacetsOwner = get_widget_value("FacetsOwner", "")
facets_secret_name = get_widget_value("facets_secret_name", "")
CurrRunCycle = get_widget_value("CurrRunCycle", "")
CurrentDate = get_widget_value("CurrentDate", "")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "")

df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"""
SELECT
Cast(Trim(TMP.CLM_ID) as char(12)) as CLCL_ID
      ,PR.CDML_SEQ_NO
      ,PR.CDPP_PERF_ID
      ,PR.CDPP_PERF_ID_NPI
      ,PR.CDPP_PERF_NAME
      ,PR.CDPP_PERF_SPEC
      ,PR.CDPP_PERF_TYPE
      ,PR.CDPP_TAXONOMY_CD
      ,PR.CDPP_SVC_FAC_ST
      ,PR.CDPP_SVC_FAC_ZIP
      ,PR.CDPP_SVC_FAC_ID
      ,PR.CDPP_SVC_FAC_QUAL
      ,PR.CDPP_CLASS_PROV
      ,PR.CDPP_PRPR_TYP_AVLB
      ,PR.CDPP_ADDL_DATA
      ,PR.ATXR_SOURCE_ID
      ,PR.CDPP_PERF_IHS_IND_NVL
      ,PR.CDPP_FLEX_NTWK_GRP_NVL
      ,PR.CDPP_MARKET_ID_NVL
      ,PR.CDPP_TIER_DSGN_IND_NVL
FROM  {FacetsOwner}.CMC_CDPP_LI_ITS_PR PR
,tempdb..{DriverTable} TMP
WHERE TMP.CLM_ID = PR.CLCL_ID
"""

df_odbc_Facets_CMC_CDPP_LI_ITS_PR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_tTrimFields = df_odbc_Facets_CMC_CDPP_LI_ITS_PR.select(
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.col("CDPP_PERF_ID").alias("CDPP_PERF_ID"),
    F.col("CDPP_PERF_ID_NPI").alias("CDPP_PERF_ID_NPI"),
    F.col("CDPP_PERF_NAME").alias("CDPP_PERF_NAME"),
    F.col("CDPP_PERF_SPEC").alias("CDPP_PERF_SPEC"),
    F.col("CDPP_PERF_TYPE").alias("CDPP_PERF_TYPE"),
    trim("CDPP_TAXONOMY_CD").alias("CDPP_TAXONOMY_CD"),
    F.col("CDPP_SVC_FAC_ST").alias("CDPP_SVC_FAC_ST"),
    F.col("CDPP_SVC_FAC_ZIP").alias("CDPP_SVC_FAC_ZIP"),
    F.col("CDPP_SVC_FAC_ID").alias("CDPP_SVC_FAC_ID"),
    F.col("CDPP_SVC_FAC_QUAL").alias("CDPP_SVC_FAC_QUAL"),
    F.col("CDPP_CLASS_PROV").alias("CDPP_CLASS_PROV"),
    F.col("CDPP_PRPR_TYP_AVLB").alias("CDPP_PRPR_TYP_AVLB"),
    F.col("CDPP_ADDL_DATA").alias("CDPP_ADDL_DATA"),
    F.col("ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID"),
    F.col("CDPP_PERF_IHS_IND_NVL").alias("CDPP_PERF_IHS_IND_NVL"),
    F.col("CDPP_FLEX_NTWK_GRP_NVL").alias("CDPP_FLEX_NTWK_GRP_NVL"),
    F.col("CDPP_MARKET_ID_NVL").alias("CDPP_MARKET_ID_NVL"),
    F.col("CDPP_TIER_DSGN_IND_NVL").alias("CDPP_TIER_DSGN_IND_NVL")
)

df_businessRules_temp = (
    df_tTrimFields.alias("lnk_Strip")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        F.col("lnk_Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
        "left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("lnk_Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
    .withColumn(
        "ClmId",
        F.when(
            (F.col("lnk_Strip.CLCL_ID").isNotNull()) & (trim("lnk_Strip.CLCL_ID") != ""),
            trim("lnk_Strip.CLCL_ID")
        ).otherwise("NA")
    )
    .withColumn(
        "ClmLnId",
        F.when(
            (F.col("lnk_Strip.CDML_SEQ_NO").isNotNull()) & (trim("lnk_Strip.CDML_SEQ_NO") != ""),
            F.col("lnk_Strip.CDML_SEQ_NO")
        ).otherwise("NA")
    )
)

df_ClmLnExtrnlProv = df_businessRules_temp.filter(
    F.col("nasco_dup_lkup.CLM_ID").isNull()
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.lit(0).alias("CLM_LN_EXTRNL_PROV_SK"),
    F.concat_ws(";", F.lit("FACETS"), F.col("ClmId"), F.col("lnk_Strip.CDML_SEQ_NO")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ClmId").alias("CLM_ID"),
    F.col("lnk_Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("lnk_Strip.CDPP_PERF_ID").alias("SVC_PROV_ID"),
    F.col("lnk_Strip.CDPP_PERF_ID_NPI").alias("SVC_PROV_NPI"),
    F.col("lnk_Strip.CDPP_PERF_NAME").alias("SVC_PROV_NM"),
    F.col("lnk_Strip.CDPP_PERF_SPEC").alias("SVC_PROV_SPEC_TX"),
    F.col("lnk_Strip.CDPP_PERF_TYPE").alias("SVC_PROV_TYP_TX"),
    F.col("lnk_Strip.CDPP_TAXONOMY_CD").alias("CDPP_TAXONOMY_CD"),
    F.col("lnk_Strip.CDPP_SVC_FAC_ST").alias("CDPP_SVC_FAC_ST"),
    F.col("lnk_Strip.CDPP_SVC_FAC_ZIP").alias("SVC_FCLTY_LOC_ZIP_CD"),
    F.col("lnk_Strip.CDPP_SVC_FAC_ID").alias("SVC_FCLTY_LOC_NO"),
    F.col("lnk_Strip.CDPP_SVC_FAC_QUAL").alias("SVC_FCLTY_LOC_NO_QLFR_TX"),
    F.col("lnk_Strip.CDPP_CLASS_PROV").alias("CDPP_CLASS_PROV"),
    F.col("lnk_Strip.CDPP_PRPR_TYP_AVLB").alias("SVC_PROV_TYP_PPO_AVLBL_IN"),
    F.col("lnk_Strip.CDPP_ADDL_DATA").alias("ADTNL_DATA_ELE_TX"),
    F.col("lnk_Strip.ATXR_SOURCE_ID").alias("ATCHMT_SRC_ID_DTM"),
    F.col("lnk_Strip.CDPP_PERF_IHS_IND_NVL").alias("SVC_PROV_INDN_HLTH_SVC_IN"),
    F.col("lnk_Strip.CDPP_FLEX_NTWK_GRP_NVL").alias("FLEX_NTWK_PROV_CST_GRPNG_TX"),
    F.col("lnk_Strip.CDPP_MARKET_ID_NVL").alias("MKT_ID"),
    F.col("lnk_Strip.CDPP_TIER_DSGN_IND_NVL").alias("HOST_PROV_ITS_TIER_DSGTN_TX"),
    F.col("lnk_Strip.CDPP_SVC_FAC_ZIP").alias("SVC_PROV_ZIP_CD")
)

df_reversals = df_businessRules_temp.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull())
    & (F.col("fcts_reversals.CLCL_CUR_STS").isin(["89", "91", "99"]))
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.lit(0).alias("CLM_LN_EXTRNL_PROV_SK"),
    F.concat(F.lit("FACETS;"), F.col("ClmId"), F.lit("R;"), F.col("lnk_Strip.CDML_SEQ_NO")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
    F.col("lnk_Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("lnk_Strip.CDPP_PERF_ID").alias("SVC_PROV_ID"),
    F.col("lnk_Strip.CDPP_PERF_ID_NPI").alias("SVC_PROV_NPI"),
    F.col("lnk_Strip.CDPP_PERF_NAME").alias("SVC_PROV_NM"),
    F.col("lnk_Strip.CDPP_PERF_SPEC").alias("SVC_PROV_SPEC_TX"),
    F.col("lnk_Strip.CDPP_PERF_TYPE").alias("SVC_PROV_TYP_TX"),
    F.col("lnk_Strip.CDPP_TAXONOMY_CD").alias("CDPP_TAXONOMY_CD"),
    F.col("lnk_Strip.CDPP_SVC_FAC_ST").alias("CDPP_SVC_FAC_ST"),
    F.col("lnk_Strip.CDPP_SVC_FAC_ZIP").alias("SVC_FCLTY_LOC_ZIP_CD"),
    F.col("lnk_Strip.CDPP_SVC_FAC_ID").alias("SVC_FCLTY_LOC_NO"),
    F.col("lnk_Strip.CDPP_SVC_FAC_QUAL").alias("SVC_FCLTY_LOC_NO_QLFR_TX"),
    F.col("lnk_Strip.CDPP_CLASS_PROV").alias("CDPP_CLASS_PROV"),
    F.col("lnk_Strip.CDPP_PRPR_TYP_AVLB").alias("SVC_PROV_TYP_PPO_AVLBL_IN"),
    F.col("lnk_Strip.CDPP_ADDL_DATA").alias("ADTNL_DATA_ELE_TX"),
    F.col("lnk_Strip.ATXR_SOURCE_ID").alias("ATCHMT_SRC_ID_DTM"),
    F.col("lnk_Strip.CDPP_PERF_IHS_IND_NVL").alias("SVC_PROV_INDN_HLTH_SVC_IN"),
    F.col("lnk_Strip.CDPP_FLEX_NTWK_GRP_NVL").alias("FLEX_NTWK_PROV_CST_GRPNG_TX"),
    F.col("lnk_Strip.CDPP_MARKET_ID_NVL").alias("MKT_ID"),
    F.col("lnk_Strip.CDPP_TIER_DSGN_IND_NVL").alias("HOST_PROV_ITS_TIER_DSGTN_TX"),
    F.col("lnk_Strip.CDPP_SVC_FAC_ZIP").alias("SVC_PROV_ZIP_CD")
)

df_collector = df_ClmLnExtrnlProv.unionByName(df_reversals)

df_xfm = df_collector.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD_SK",
    "SRC_SYS_CD",
    "CLM_LN_EXTRNL_PROV_SK",
    "PRI_KEY_STRING",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "SVC_PROV_ID",
    "SVC_PROV_NPI",
    "SVC_PROV_NM",
    "SVC_PROV_SPEC_TX",
    "SVC_PROV_TYP_TX",
    "CDPP_TAXONOMY_CD",
    "CDPP_SVC_FAC_ST",
    "SVC_FCLTY_LOC_ZIP_CD",
    "SVC_FCLTY_LOC_NO",
    "SVC_FCLTY_LOC_NO_QLFR_TX",
    "CDPP_CLASS_PROV",
    "SVC_PROV_TYP_PPO_AVLBL_IN",
    "ADTNL_DATA_ELE_TX",
    "ATCHMT_SRC_ID_DTM",
    "SVC_PROV_INDN_HLTH_SVC_IN",
    "FLEX_NTWK_PROV_CST_GRPNG_TX",
    "MKT_ID",
    "HOST_PROV_ITS_TIER_DSGTN_TX",
    "SVC_PROV_ZIP_CD"
).withColumn("CLM_LN_SK", F.lit(0))

params_ClmLnExtrnlProvPK = {
    "CurrRunCycle": CurrRunCycle
}

df_ClmLnExtrnlProvPK = ClmLnExtrnlProvPK(df_xfm, params_ClmLnExtrnlProvPK)

df_final = df_ClmLnExtrnlProvPK.select(
    rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_EXTRNL_PROV_SK"),
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    rpad(F.col("SVC_PROV_ID"), 13, " ").alias("SVC_PROV_ID"),
    F.col("SVC_PROV_NPI"),
    rpad(F.col("SVC_PROV_NM"), 31, " ").alias("SVC_PROV_NM"),
    rpad(F.col("SVC_PROV_SPEC_TX"), 2, " ").alias("SVC_PROV_SPEC_TX"),
    rpad(F.col("SVC_PROV_TYP_TX"), 2, " ").alias("SVC_PROV_TYP_TX"),
    F.col("CDPP_TAXONOMY_CD"),
    rpad(F.col("CDPP_SVC_FAC_ST"), 2, " ").alias("CDPP_SVC_FAC_ST"),
    rpad(F.col("SVC_FCLTY_LOC_ZIP_CD"), 9, " ").alias("SVC_FCLTY_LOC_ZIP_CD"),
    F.col("SVC_FCLTY_LOC_NO"),
    rpad(F.col("SVC_FCLTY_LOC_NO_QLFR_TX"), 2, " ").alias("SVC_FCLTY_LOC_NO_QLFR_TX"),
    rpad(F.col("CDPP_CLASS_PROV"), 1, " ").alias("CDPP_CLASS_PROV"),
    rpad(F.col("SVC_PROV_TYP_PPO_AVLBL_IN"), 1, " ").alias("SVC_PROV_TYP_PPO_AVLBL_IN"),
    F.col("ADTNL_DATA_ELE_TX"),
    F.col("ATCHMT_SRC_ID_DTM"),
    rpad(F.col("SVC_PROV_INDN_HLTH_SVC_IN"), 1, " ").alias("SVC_PROV_INDN_HLTH_SVC_IN"),
    rpad(F.col("FLEX_NTWK_PROV_CST_GRPNG_TX"), 3, " ").alias("FLEX_NTWK_PROV_CST_GRPNG_TX"),
    rpad(F.col("MKT_ID"), 4, " ").alias("MKT_ID"),
    rpad(F.col("HOST_PROV_ITS_TIER_DSGTN_TX"), 1, " ").alias("HOST_PROV_ITS_TIER_DSGTN_TX"),
    rpad(F.col("SVC_PROV_ZIP_CD"), 9, " ").alias("SVC_PROV_ZIP_CD"),
    F.col("CLM_LN_SK")
)

write_files(
    df_final,
    f"{adls_path}/key/FctsClmLnExtrnlProvExtr.FctsClmLnExtrnlProv.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)