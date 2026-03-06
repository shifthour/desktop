# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmLnClnclEditExtr
# MAGIC CALLED:  LhoFctsClmOnDmdExtr1Seq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CDCE_LI_EDIT  to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	CMC_CDCE_LI_EDIT
# MAGIC                 Joined to
# MAGIC                 TmpClaim to get specific records by date
# MAGIC   
# MAGIC HASH FILES:  hf_clm_nasco_dup_bypass
# MAGIC 
# MAGIC TRANSFORMS:  STRIP.FIELD
# MAGIC                            
# MAGIC PROCESSING:    Output file is created with a temp. name.  
# MAGIC 
# MAGIC OUTPUTS:   Sequential file .
# MAGIC MODIFICATIONS:
# MAGIC =============================================================================================================================================================
# MAGIC Developer                    Date                 	Project                                                     Change Description                                         Development Project             Code Reviewer          Date Reviewed    
# MAGIC =============================================================================================================================================================
# MAGIC Manasa Andru          2020-08-10                        US -  263726                                        Original Programming                                              IntegrateDev2                     Jaideep Mankala       10/09/2020
# MAGIC 	
# MAGIC Prabhu ES               2022-03-29                         S2S                                             MSSQL ODBC conn params added                                 IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC Balancing
# MAGIC Apply business logic
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Extract Facets Claim Line Clinical Edit Data
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC This container is used in:
# MAGIC FctsClmLnClnclEditExtr
# MAGIC NascoClmLnClnclEditTrns
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnClnclEditPK
# COMMAND ----------

# Retrieve job parameters
RunID = get_widget_value('RunID','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
EditTypeCDSysChgDate = get_widget_value('EditTypeCDSysChgDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')

# ------------------------------------------------------------------------------
# Stage: hf_clm_fcts_reversals (CHashedFileStage) - Scenario C => Read from Parquet
# ------------------------------------------------------------------------------
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# ------------------------------------------------------------------------------
# Stage: clm_nasco_dup_bypass (CHashedFileStage) - Scenario C => Read from Parquet
# ------------------------------------------------------------------------------
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# ------------------------------------------------------------------------------
# Stage: CMC_CDCE_LI_EDIT (ODBCConnector)
# ------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)
extract_query = (
    f"SELECT CL_LINE.CLCL_ID,\n"
    f"       CL_LINE.CDML_SEQ_NO,\n"
    f"       CL_LINE.MEME_CK,\n"
    f"       CL_LINE.CDCE_EDIT_ACT,\n"
    f"       CL_LINE.CDCE_EDIT_TYPE,\n"
    f"       CL_LINE.CDCE_FMT_IND,\n"
    f"       CL_LINE.CDCE_COMB_EXCD,\n"
    f"       CL_LINE.CDCE_CLCL_ID,\n"
    f"       CL_LINE.CDCE_CDML_SEQ_NO,\n"
    f"       CL_LINE.CDCE_LOCK_TOKEN,\n"
    f"       CL_LINE.ATXR_SOURCE_ID,\n"
    f"       CL.CLCL_ACPT_DTM,\n"
    f"       CL_LINE.CDCE_VEND_EDIT_IND\n"
    f"  FROM {LhoFacetsStgOwner}.CMC_CDCE_LI_EDIT CL_LINE\n"
    f"       INNER JOIN tempdb..{DriverTable} TMP\n"
    f"           ON TMP.CLM_ID = CL_LINE.CLCL_ID\n"
    f"       INNER JOIN {LhoFacetsStgOwner}.CMC_CLCL_CLAIM CL\n"
    f"           ON CL_LINE.CLCL_ID = CL.CLCL_ID"
)

df_CMC_CDCE_LI_EDIT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# ------------------------------------------------------------------------------
# Stage: tTrimFields (CTransformerStage)
# ------------------------------------------------------------------------------
df_tTrimFields = (
    df_CMC_CDCE_LI_EDIT
    .withColumn("CLCL_ID", strip_field(F.col("CLCL_ID")))
    .withColumn("CDML_SEQ_NO", F.col("CDML_SEQ_NO"))
    .withColumn("MEME_CK", F.col("MEME_CK"))
    .withColumn("CDCE_EDIT_ACT", strip_field(F.col("CDCE_EDIT_ACT")))
    .withColumn(
        "CDCE_EDIT_TYPE",
        F.when(
            F.col("CLCL_ACPT_DTM") < F.to_date(F.lit(EditTypeCDSysChgDate), "yyyy-MM-dd"),
            strip_field(F.col("CDCE_EDIT_TYPE"))
        ).otherwise(F.lit("NA"))
    )
    .withColumn("CDCE_FMT_IND", strip_field(F.col("CDCE_FMT_IND")))
    .withColumn("CDCE_COMB_EXCD", strip_field(F.col("CDCE_COMB_EXCD")))
    .withColumn("CDCE_CLCL_ID", strip_field(F.col("CDCE_CLCL_ID")))
    .withColumn("CDCE_CDML_SEQ_NO", F.col("CDCE_CDML_SEQ_NO"))
    .withColumn("CDCE_LOCK_TOKEN", F.col("CDCE_LOCK_TOKEN"))
    .withColumn(
        "EXCD_EDIT_TYPE",
        F.when(
            F.col("CLCL_ACPT_DTM") >= F.to_date(F.lit(EditTypeCDSysChgDate), "yyyy-MM-dd"),
            F.concat(F.col("CDCE_VEND_EDIT_IND"), F.col("CDCE_EDIT_TYPE"))
        ).otherwise(F.lit("NA"))
    )
    .withColumn("ATXR_SOURCE_ID", F.col("ATXR_SOURCE_ID"))
)

# ------------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
#   Primary link => df_tTrimFields (alias "Strip")
#   Lookup link => df_hf_clm_fcts_reversals (alias "fcts_reversals") left join
#   Lookup link => df_clm_nasco_dup_bypass (alias "nasco_dup_lkup") left join
# ------------------------------------------------------------------------------
df_BusinessRules_joined = (
    df_tTrimFields.alias("Strip")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
        how="left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        how="left"
    )
)

# Create stage variables as columns
df_BusinessRules_vars = (
    df_BusinessRules_joined
    .withColumn(
        "sv_ClmId",
        F.when(
            (F.col("Strip.CLCL_ID").isNotNull()) & (trim(F.col("Strip.CLCL_ID")) != ""),
            trim(F.col("Strip.CLCL_ID"))
        ).otherwise(F.lit("NA"))
    )
    .withColumn(
        "sv_ClmLnId",
        F.when(
            (F.col("Strip.CDML_SEQ_NO").isNotNull()) & (trim(F.col("Strip.CDML_SEQ_NO")) != ""),
            F.col("Strip.CDML_SEQ_NO")
        ).otherwise(F.lit("NA"))
    )
    .withColumn("sv_SrcSystemCd", F.lit(SrcSysCd))
)

# Output link 1: ClmLnClnclEdit (constraint: nasco_dup_lkup.CLM_ID is null)
df_ClmLnClnclEdit = (
    df_BusinessRules_vars
    .filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("sv_SrcSystemCd").alias("SRC_SYS_CD"),
        F.concat(F.col("sv_SrcSystemCd"), F.lit(";"), F.col("sv_ClmId"), F.lit(";"), F.col("Strip.CDML_SEQ_NO")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_CLNCL_EDIT_SK"),
        F.col("sv_ClmId").alias("CLM_ID"),
        F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(
            (F.col("Strip.CDCE_EDIT_ACT").isNull()) | (F.length(trim(F.col("Strip.CDCE_EDIT_ACT"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("Strip.CDCE_EDIT_ACT")))).alias("CLM_LN_CLNCL_EDIT_ACTN_CD"),
        F.when(
            (F.col("Strip.CDCE_FMT_IND").isNull()) | (F.length(trim(F.col("Strip.CDCE_FMT_IND"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("Strip.CDCE_FMT_IND")))).alias("CLM_LN_CLNCL_EDIT_FMT_CHG_CD"),
        F.when(
            (F.col("Strip.CDCE_EDIT_TYPE").isNull()) | (F.length(trim(F.col("Strip.CDCE_EDIT_TYPE"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("Strip.CDCE_EDIT_TYPE")))).alias("CLM_LN_CLNCL_EDIT_TYP_CD"),
        F.when(
            (F.col("Strip.CDCE_COMB_EXCD").isNull()) | (F.length(trim(F.col("Strip.CDCE_COMB_EXCD"))) == 0),
            F.lit("N")
        ).otherwise(F.lit("Y")).alias("COMBND_CHRG_IN"),
        F.when(
            (F.col("Strip.CDCE_CLCL_ID").isNotNull()) & (trim(F.col("Strip.CDCE_CLCL_ID")) != ""),
            F.col("Strip.CDCE_CLCL_ID")
        ).otherwise(F.lit("NA")).alias("REF_CLM_ID"),
        F.when(
            (F.col("Strip.CDCE_CLCL_ID").isNotNull()) & (trim(F.col("Strip.CDCE_CLCL_ID")) != ""),
            F.col("Strip.CDCE_CDML_SEQ_NO")
        ).otherwise(F.lit(0)).alias("REF_CLM_LN_SEQ_NO"),
        trim(F.col("Strip.EXCD_EDIT_TYPE")).alias("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD")
    )
)

# Output link 2: reversals (constraint: fcts_reversals.CLCL_ID is not null AND CLCL_CUR_STS in [89,91,99])
df_reversals = (
    df_BusinessRules_vars
    .filter(
        (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
        (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("sv_SrcSystemCd").alias("SRC_SYS_CD"),
        F.concat(F.col("sv_SrcSystemCd"), F.lit(";"), F.col("sv_ClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_CLNCL_EDIT_SK"),
        F.concat(F.col("sv_ClmId"), F.lit("R")).alias("CLM_ID"),
        F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(
            (F.col("Strip.CDCE_EDIT_ACT").isNull()) | (F.length(trim(F.col("Strip.CDCE_EDIT_ACT"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("Strip.CDCE_EDIT_ACT")))).alias("CLM_LN_CLNCL_EDIT_ACTN_CD"),
        F.when(
            (F.col("Strip.CDCE_FMT_IND").isNull()) | (F.length(trim(F.col("Strip.CDCE_FMT_IND"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("Strip.CDCE_FMT_IND")))).alias("CLM_LN_CLNCL_EDIT_FMT_CHG_CD"),
        F.when(
            (F.col("Strip.CDCE_EDIT_TYPE").isNull()) | (F.length(trim(F.col("Strip.CDCE_EDIT_TYPE"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("Strip.CDCE_EDIT_TYPE")))).alias("CLM_LN_CLNCL_EDIT_TYP_CD"),
        F.when(
            (F.col("Strip.CDCE_COMB_EXCD").isNull()) | (F.length(trim(F.col("Strip.CDCE_COMB_EXCD"))) == 0),
            F.lit("N")
        ).otherwise(F.lit("Y")).alias("COMBND_CHRG_IN"),
        F.when(
            (F.col("Strip.CDCE_CLCL_ID").isNotNull()) & (trim(F.col("Strip.CDCE_CLCL_ID")) != ""),
            F.col("Strip.CDCE_CLCL_ID")
        ).otherwise(F.lit("NA")).alias("REF_CLM_ID"),
        F.when(
            (F.col("Strip.CDCE_CLCL_ID").isNotNull()) & (trim(F.col("Strip.CDCE_CLCL_ID")) != ""),
            F.col("Strip.CDCE_CDML_SEQ_NO")
        ).otherwise(F.lit(0)).alias("REF_CLM_LN_SEQ_NO"),
        trim(F.col("Strip.EXCD_EDIT_TYPE")).alias("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD")
    )
)

# ------------------------------------------------------------------------------
# Stage: Collector (CCollector) => union the two dataframes
# ------------------------------------------------------------------------------
df_Collector = df_reversals.unionByName(df_ClmLnClnclEdit)

# ------------------------------------------------------------------------------
# Stage: SnapShot (CTransformerStage)
#   Input: df_Collector => output to two links: "Pkey" and "Snapshot"
# ------------------------------------------------------------------------------
# We use one base DataFrame for reference
df_SnapShot_base = df_Collector

# Output link: "Transform" => columns exactly as listed
# (The job calls it "Transform" internally, but we can just keep df_SnapShot_base as is.)

# Link "Pkey"
df_SnapShot_pkey = df_SnapShot_base.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_LN_CLNCL_EDIT_SK").alias("CLM_LN_CLNCL_EDIT_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_CLNCL_EDIT_ACTN_CD").alias("CLM_LN_CLNCL_EDIT_ACTN_CD"),
    F.col("CLM_LN_CLNCL_EDIT_FMT_CHG_CD").alias("CLM_LN_CLNCL_EDIT_FMT_CHG_CD"),
    F.col("CLM_LN_CLNCL_EDIT_TYP_CD").alias("CLM_LN_CLNCL_EDIT_TYP_CD"),
    F.col("COMBND_CHRG_IN").alias("COMBND_CHRG_IN"),
    F.col("REF_CLM_ID").alias("REF_CLM_ID"),
    F.col("REF_CLM_LN_SEQ_NO").alias("REF_CLM_LN_SEQ_NO"),
    F.when(
        (F.col("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD") == "") | (F.col("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD").isNull()),
        F.lit("NA")
    ).otherwise(F.col("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD")).alias("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD")
)

# Link "Snapshot"
df_SnapShot_snapshot = df_SnapShot_base.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO")
)

# ------------------------------------------------------------------------------
# Stage: B_CLM_LN_CLNCL_EDIT (CSeqFileStage) => writes df_SnapShot_snapshot to file
# ------------------------------------------------------------------------------
# Apply rpad for any char columns (CLM_ID is char(18))
df_SnapShot_snapshot_write = (
    df_SnapShot_snapshot
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
)

b_clm_ln_clncl_edit_path = f"{adls_path}/load/B_CLM_LN_CLNCL_EDIT.{SrcSysCd}.dat.{RunID}"
write_files(
    df_SnapShot_snapshot_write,
    b_clm_ln_clncl_edit_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ------------------------------------------------------------------------------
# Stage: ClmLnClnclEditPK (CContainerStage) => shared container call
# ------------------------------------------------------------------------------
params_ClmLnClnclEditPK = {
    "CurrRunCycle": CurrRunCycle
}
df_ClmLnClnclEditPK = ClmLnClnclEditPK(df_SnapShot_pkey, params_ClmLnClnclEditPK)

# ------------------------------------------------------------------------------
# Stage: FctsClmLnClnclEdit (CSeqFileStage) => writes df_ClmLnClnclEditPK to file
# ------------------------------------------------------------------------------
# Final rpad for char columns:
# INSRT_UPDT_CD (char(10)), DISCARD_IN (char(1)), PASS_THRU_IN (char(1)), CLM_ID (char(18)), COMBND_CHRG_IN (char(1))
df_FctsClmLnClnclEdit_final = (
    df_ClmLnClnclEditPK
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_LN_CLNCL_EDIT_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_CLNCL_EDIT_ACTN_CD",
        "CLM_LN_CLNCL_EDIT_FMT_CHG_CD",
        "CLM_LN_CLNCL_EDIT_TYP_CD",
        "COMBND_CHRG_IN",
        "REF_CLM_ID",
        "REF_CLM_LN_SEQ_NO",
        "CLM_LN_CLNCL_EDIT_EXCD_TYP_CD"
    )
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
    .withColumn("COMBND_CHRG_IN", F.rpad(F.col("COMBND_CHRG_IN"), 1, " "))
)

fcts_clm_ln_clncl_edit_path = f"{adls_path}/key/LhoFctsClmLnClnclEditExtr.LhoFctsClmLnClnclEdit.dat.{RunID}"
write_files(
    df_FctsClmLnClnclEdit_final,
    fcts_clm_ln_clncl_edit_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)