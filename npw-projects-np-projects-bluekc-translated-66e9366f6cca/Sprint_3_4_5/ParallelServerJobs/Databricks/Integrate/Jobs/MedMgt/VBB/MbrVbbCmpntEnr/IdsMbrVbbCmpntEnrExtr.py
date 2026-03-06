# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Extracs data and assigns Primary Keys for the IDS table MBR_VBB_CMPNT_ENR.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Raja Gummadi         2013-05-13          4963 VBB Phase III     Initial Programming                                                                      IntegrateNewDevl           Bhoomi Dasari           7/8/2013
# MAGIC Raja Gummadi         2013-10-16          4963 VBB                    Added snapshot file to the job                                                     IntegrateNewDevl 
# MAGIC Dan Long                2014-10-03                  TFS-9769            Added new stage hf_remove_dups to remove duplicate             IntegrateNewDevl            Kalyan Neelam           2014-10-06
# MAGIC                                                                                                data before creating the B_MBR_VBB_CMPNT_ENR.dat
# MAGIC                                                                                                file.

# MAGIC IDS MBR_VBB_CMPNT_ENR Extract
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
from pyspark.sql.functions import col, lit, when, regexp_replace, length, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/MbrVbbCmpntEnrPK
# COMMAND ----------

# Retrieve job parameters
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
VBBOwner = get_widget_value("VBBOwner","")
vbb_secret_name = get_widget_value("vbb_secret_name","")
SrcSysCd = get_widget_value("SrcSysCd","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
RunId = get_widget_value("RunId","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
LastRunDtm = get_widget_value("LastRunDtm","")
CurrDate = get_widget_value("CurrDate","")
Environment = get_widget_value("Environment","")

# Stage: IHMFCONSTITUENT (CODBCStage) - multiple outputs

jdbc_url_vbb, jdbc_props_vbb = get_db_config(vbb_secret_name)

# 1) Extract
extract_query = f"""
SELECT 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_MEMBER_ID, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.INPR_ID, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_CONTACT_IND, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_ENROLL_DT, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_TERM_DT, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_TERM_REASON, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.VNVN_ID, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.VNPR_SEQ_NO, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_STATUS, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_ENROLLED_BY, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_TERMED_BY, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_COMPLETE_DT, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_CONTACT_CHNG_DT, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.HIEL_LOG_LEVEL1, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.HIEL_LOG_LEVEL2, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.HIEL_LOG_LEVEL3, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_CREATE_DT, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_UPDATE_DT,
TZIM_MEHP_MEMBER_HLTH_PGM.HIPL_ID 
FROM 
{VBBOwner}.TZIM_MEIP_MEMBER_INCENTIVE_PGM TZIM_MEIP_MEMBER_INCENTIVE_PGM 
LEFT JOIN {VBBOwner}.TZIM_MEHP_MEMBER_HLTH_PGM TZIM_MEHP_MEMBER_HLTH_PGM
ON TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_MEMBER_ID = TZIM_MEHP_MEMBER_HLTH_PGM.MEHP_MEMBER_ID
WHERE TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_UPDATE_DT > '{LastRunDtm}'
"""
df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_vbb)
    .options(**jdbc_props_vbb)
    .option("query", extract_query)
    .load()
)

# 2) Xref
xref_query = f"""
SELECT 
TZIM_MKCR_MEMBER_KEY_CROSS_REFERENCE.MKCR_GUID, 
TZIM_MKCR_MEMBER_KEY_CROSS_REFERENCE.MKCR_SEGMENT_VALUE 
FROM {VBBOwner}.TZIM_MKCR_MEMBER_KEY_CROSS_REFERENCE TZIM_MKCR_MEMBER_KEY_CROSS_REFERENCE
WHERE
TZIM_MKCR_MEMBER_KEY_CROSS_REFERENCE.MKCR_SEGMENT_NAME = 'MEME_CK'
"""
df_Xref = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_vbb)
    .options(**jdbc_props_vbb)
    .option("query", xref_query)
    .load()
)

# 3) Achvlvl
achvlvl_query = f"""
SELECT 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.INPR_ID, 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_MEMBER_ID,
Count(TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_MEMBER_ID)
FROM 
{VBBOwner}.TZIM_MEIP_MEMBER_INCENTIVE_PGM TZIM_MEIP_MEMBER_INCENTIVE_PGM 
INNER JOIN {VBBOwner}.TZIM_MEIA_MEMBER_INCENTIVE_PGM_ACHV TZIM_MEIA_MEMBER_INCENTIVE_PGM_ACHV
ON 
TZIM_MEIP_MEMBER_INCENTIVE_PGM.INPR_ID = TZIM_MEIA_MEMBER_INCENTIVE_PGM_ACHV.INPR_ID AND
TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_MEMBER_ID = TZIM_MEIA_MEMBER_INCENTIVE_PGM_ACHV.MEIP_MEMBER_ID AND
TZIM_MEIA_MEMBER_INCENTIVE_PGM_ACHV.MEIA_EARNED_DT IS NOT NULL
GROUP BY
TZIM_MEIP_MEMBER_INCENTIVE_PGM.INPR_ID,
TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_MEMBER_ID
ORDER BY
TZIM_MEIP_MEMBER_INCENTIVE_PGM.INPR_ID
"""
df_Achvlvl_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_vbb)
    .options(**jdbc_props_vbb)
    .option("query", achvlvl_query)
    .load()
)
df_Achvlvl = df_Achvlvl_raw.select(
    col("INPR_ID"),
    col("MEIP_MEMBER_ID"),
    col("count(TZIM_MEIP_MEMBER_INCENTIVE_PGM.MEIP_MEMBER_ID)").alias("COUNT"),
)

# Stage: Trim (CTransformerStage)
# We replicate stage variable logic:

df_Extract_clean = df_Extract.withColumn(
    "_INPR_ID_CLEAN",
    regexp_replace(F.col("INPR_ID"), "[\n\r\t]", "")
).withColumn(
    "_INPR_ID_TRIM",
    trim(col("_INPR_ID_CLEAN"))
)

df_Extract_sv = df_Extract_clean.withColumn(
    "svInprId",
    when((col("_INPR_ID_TRIM").isNull()) | (length(col("_INPR_ID_TRIM")) == 0), lit("N")).otherwise(lit("Y"))
).withColumn(
    "svGoodRecord",
    when(col("svInprId") == "Y", lit("Y")).otherwise(lit("N"))
)

# Constraint: svGoodRecord = 'Y' => CmpntData, else => ErrorFile
df_CmpntData_pre = df_Extract_sv.filter(col("svGoodRecord") == "Y")
df_ErrorFile_pre = df_Extract_sv.filter(col("svGoodRecord") == "N")

# Now apply column expressions for CmpntData
df_CmpntData = df_CmpntData_pre.select(
    rpad(trim(col("MEIP_MEMBER_ID")), 36, " ").alias("MEIP_MEMBER_ID"),  # char(36)
    trim(col("INPR_ID")).alias("INPR_ID"),
    trim(col("MEIP_CONTACT_IND")).alias("MEIP_CONTACT_IND"),
    trim(col("MEIP_ENROLL_DT")).alias("MEIP_ENROLL_DT"),
    trim(col("MEIP_TERM_DT")).alias("MEIP_TERM_DT"),
    Upcase(trim(col("MEIP_TERM_REASON"))).alias("MEIP_TERM_REASON"),
    trim(col("VNVN_ID")).alias("VNVN_ID"),
    trim(col("VNPR_SEQ_NO")).alias("VNPR_SEQ_NO"),
    Upcase(trim(col("MEIP_STATUS"))).alias("MEIP_STATUS"),
    Upcase(trim(col("MEIP_ENROLLED_BY"))).alias("MEIP_ENROLLED_BY"),
    Upcase(trim(col("MEIP_TERMED_BY"))).alias("MEIP_TERMED_BY"),
    trim(col("MEIP_COMPLETE_DT")).alias("MEIP_COMPLETE_DT"),
    trim(col("MEIP_CONTACT_CHNG_DT")).alias("MEIP_CONTACT_CHNG_DT"),
    trim(col("HIEL_LOG_LEVEL1")).alias("HIEL_LOG_LEVEL1"),
    trim(col("HIEL_LOG_LEVEL2")).alias("HIEL_LOG_LEVEL2"),
    trim(col("HIEL_LOG_LEVEL3")).alias("HIEL_LOG_LEVEL3"),
    trim(col("MEIP_CREATE_DT")).alias("MEIP_CREATE_DT"),
    trim(col("MEIP_UPDATE_DT")).alias("MEIP_UPDATE_DT"),
    trim(col("HIPL_ID")).alias("HIPL_ID")
)

# Columns for ErrorFile
df_ErrorFile_cols = df_ErrorFile_pre.select(
    rpad(col("MEIP_MEMBER_ID"), 36, " ").alias("MEIP_MEMBER_ID"),  # char(36)
    col("INPR_ID").alias("INPR_ID"),
    col("MEIP_CONTACT_IND").alias("MEIP_CONTACT_IND"),
    col("MEIP_ENROLL_DT").alias("MEIP_ENROLL_DT"),
    col("MEIP_TERM_DT").alias("MEIP_TERM_DT"),
    col("MEIP_TERM_REASON").alias("MEIP_TERM_REASON"),
    col("VNVN_ID").alias("VNVN_ID"),
    col("VNPR_SEQ_NO").alias("VNPR_SEQ_NO"),
    col("MEIP_STATUS").alias("MEIP_STATUS"),
    col("MEIP_ENROLLED_BY").alias("MEIP_ENROLLED_BY"),
    col("MEIP_TERMED_BY").alias("MEIP_TERMED_BY"),
    col("MEIP_COMPLETE_DT").alias("MEIP_COMPLETE_DT"),
    col("MEIP_CONTACT_CHNG_DT").alias("MEIP_CONTACT_CHNG_DT"),
    col("HIEL_LOG_LEVEL1").alias("HIEL_LOG_LEVEL1"),
    col("HIEL_LOG_LEVEL2").alias("HIEL_LOG_LEVEL2"),
    col("HIEL_LOG_LEVEL3").alias("HIEL_LOG_LEVEL3"),
    col("MEIP_CREATE_DT").alias("MEIP_CREATE_DT"),
    col("MEIP_UPDATE_DT").alias("MEIP_UPDATE_DT")
)

# Write ErrorFile
error_file_path = f"{adls_path_publish}/external/MBR_VBB_CMPNT_ENR_ErrorsOut_{RunId}.dat"
write_files(
    df_ErrorFile_cols,
    error_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# Stage: hf_mbrvbbcmpntenr_xref (CHashedFileStage)
# Scenario A (intermediate hashed file). We deduplicate on primary key = MKCR_GUID
df_xref_dedup = df_Xref.dropDuplicates(["MKCR_GUID"])

# Stage: hf_mbrvbbcmpntenr_achv (CHashedFileStage)
# Also scenario A => deduplicate using primary keys (INPR_ID, MEIP_MEMBER_ID)
df_achvlvl_dedup = df_Achvlvl.dropDuplicates(["INPR_ID", "MEIP_MEMBER_ID"])

# Stage: Transformer_6 (CTransformerStage)
# Join logic / Stage variables
df_Transformer_6_joined = (
    df_CmpntData
    .alias("CmpntData")
    .join(df_xref_dedup.alias("xreflkup"), F.col("CmpntData.MEIP_MEMBER_ID") == F.col("xreflkup.MKCR_GUID"), "left")
    .join(
        df_achvlvl_dedup.alias("Lvllkup"),
        [
            F.col("CmpntData.INPR_ID") == F.col("Lvllkup.INPR_ID"),
            F.col("CmpntData.MEIP_MEMBER_ID") == F.col("Lvllkup.MEIP_MEMBER_ID")
        ],
        "left"
    )
)

df_Transformer_6_stagevars = (
    df_Transformer_6_joined
    .withColumn(
        "svSrcSysCrtDt",
        when((col("CmpntData.MEIP_CREATE_DT").isNull()) | (length(col("CmpntData.MEIP_CREATE_DT")) == 0),
             lit("1753-01-01 00:00:00.000"))
        .otherwise(col("CmpntData.MEIP_CREATE_DT"))
    )
    .withColumn(
        "svSrcSysUptDt",
        when((col("CmpntData.MEIP_UPDATE_DT").isNull()) | (length(col("CmpntData.MEIP_UPDATE_DT")) == 0),
             lit("1753-01-01 00:00:00.000"))
        .otherwise(col("CmpntData.MEIP_UPDATE_DT"))
    )
    .withColumn(
        "svMbrCntctLastUpdtDtSk",
        when((col("CmpntData.MEIP_CONTACT_CHNG_DT").isNull()) | (length(col("CmpntData.MEIP_CONTACT_CHNG_DT")) == 0),
             lit("1753-01-01"))
        .otherwise(F.substring(col("CmpntData.MEIP_CONTACT_CHNG_DT"), 1, 10))
    )
    .withColumn(
        "svMbrVbbCmpntCmpltnDtSk",
        when((col("CmpntData.MEIP_COMPLETE_DT").isNull()) | (length(col("CmpntData.MEIP_COMPLETE_DT")) == 0),
             lit("1753-01-01"))
        .otherwise(F.substring(col("CmpntData.MEIP_COMPLETE_DT"), 1, 10))
    )
    .withColumn(
        "svMbrVbbCmpntEnrDtSk",
        when((col("CmpntData.MEIP_ENROLL_DT").isNull()) | (length(col("CmpntData.MEIP_ENROLL_DT")) == 0),
             lit("1753-01-01"))
        .otherwise(F.substring(col("CmpntData.MEIP_ENROLL_DT"), 1, 10))
    )
    .withColumn(
        "svMbrVbbCmpntTermDtSk",
        when((col("CmpntData.MEIP_TERM_DT").isNull()) | (length(col("CmpntData.MEIP_TERM_DT")) == 0),
             lit("2199-12-31"))
        .otherwise(F.substring(col("CmpntData.MEIP_TERM_DT"), 1, 10))
    )
)

# Output links from Transformer_6:
# 1) AllCol
df_Transformer_6_AllCol = df_Transformer_6_stagevars.select(
    when(
        (col("xreflkup.MKCR_GUID").isNull()) | (length(col("xreflkup.MKCR_GUID")) == 0),
        lit("0")
    ).otherwise(col("xreflkup.MKCR_SEGMENT_VALUE")).alias("MBR_UNIQ_KEY"),
    col("CmpntData.INPR_ID").alias("VBB_CMPNT_UNIQ_KEY"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"),10," ").alias("INSRT_UPDT_CD"),
    rpad(lit("N"),1," ").alias("DISCARD_IN"),
    rpad(lit("Y"),1," ").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD_1"),
    F.concat(F.trim(col("xreflkup.MKCR_SEGMENT_VALUE")), lit(";"),
             F.trim(col("CmpntData.INPR_ID")), lit(";"), lit(SrcSysCd)).alias("PRI_KEY_STRING"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("MBR_VBB_CMPNT_ENR_SK"),
    col("CmpntData.HIEL_LOG_LEVEL1").alias("HIEL_LOG_LEVEL1"),
    col("CmpntData.HIEL_LOG_LEVEL2").alias("HIEL_LOG_LEVEL2"),
    col("CmpntData.HIEL_LOG_LEVEL3").alias("HIEL_LOG_LEVEL3"),
    when((col("CmpntData.MEIP_ENROLLED_BY").isNull()) | (length(col("CmpntData.MEIP_ENROLLED_BY")) == 0),
         lit("NA")).otherwise(col("CmpntData.MEIP_ENROLLED_BY")).alias("MEIP_ENROLLED_BY"),
    when((col("CmpntData.MEIP_STATUS").isNull()) | (length(col("CmpntData.MEIP_STATUS")) == 0),
         lit("NA")).otherwise(col("CmpntData.MEIP_STATUS")).alias("MEIP_STATUS"),
    when((col("CmpntData.MEIP_TERMED_BY").isNull()) | (length(col("CmpntData.MEIP_TERMED_BY")) == 0),
         lit("NA")).otherwise(col("CmpntData.MEIP_TERMED_BY")).alias("MEIP_TERMED_BY"),
    when((col("CmpntData.MEIP_TERM_REASON").isNull()) | (length(col("CmpntData.MEIP_TERM_REASON")) == 0),
         lit("NA")).otherwise(col("CmpntData.MEIP_TERM_REASON")).alias("MEIP_TERM_REASON"),
    when((col("CmpntData.MEIP_CONTACT_IND").isNull()) | (length(col("CmpntData.MEIP_CONTACT_IND")) == 0),
         lit("0")).otherwise(col("CmpntData.MEIP_CONTACT_IND")).alias("MBR_CNTCT_IN"),
    rpad(col("svMbrCntctLastUpdtDtSk"),10," ").alias("MBR_CNTCT_LAST_UPDT_DT_SK"),
    rpad(col("svMbrVbbCmpntCmpltnDtSk"),10," ").alias("MBR_VBB_CMPNT_CMPLTN_DT_SK"),
    rpad(col("svMbrVbbCmpntEnrDtSk"),10," ").alias("MBR_VBB_CMPNT_ENR_DT_SK"),
    rpad(col("svMbrVbbCmpntTermDtSk"),10," ").alias("MBR_VBB_CMPNT_TERM_DT_SK"),
    col("svSrcSysCrtDt").alias("SRC_SYS_CRT_DTM"),
    col("svSrcSysUptDt").alias("SRC_SYS_UPDT_DTM"),
    when((col("CmpntData.VNVN_ID").isNull()) | (length(col("CmpntData.VNVN_ID")) == 0),
         lit("0")).otherwise(col("CmpntData.VNVN_ID")).alias("VBB_VNDR_UNIQ_KEY"),
    when((col("CmpntData.VNPR_SEQ_NO").isNull()) | (length(col("CmpntData.VNPR_SEQ_NO")) == 0),
         lit("0")).otherwise(col("CmpntData.VNPR_SEQ_NO")).alias("VNDR_PGM_SEQ_NO"),
    when((col("Lvllkup.INPR_ID").isNull()) | (length(col("Lvllkup.INPR_ID")) == 0),
         lit("0")).otherwise(col("Lvllkup.COUNT")).alias("MBR_CMPLD_ACHV_LVL_CT"),
    col("CmpntData.MEIP_MEMBER_ID").alias("TRZ_MBR_UNVRS_ID"),
    col("CmpntData.HIPL_ID").alias("HIPL_ID")
)

# 2) Transform
df_Transformer_6_Transform = df_Transformer_6_stagevars.select(
    when(
        (col("xreflkup.MKCR_GUID").isNull()) | (length(col("xreflkup.MKCR_GUID")) == 0),
        lit("0")
    ).otherwise(col("xreflkup.MKCR_SEGMENT_VALUE")).alias("MBR_UNIQ_KEY"),
    col("CmpntData.INPR_ID").alias("VBB_CMPNT_UNIQ_KEY"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

# 3) Snapshot
df_Transformer_6_Snapshot = df_Transformer_6_stagevars.select(
    when(
        (col("xreflkup.MKCR_GUID").isNull()) | (length(col("xreflkup.MKCR_GUID")) == 0),
        lit("0")
    ).otherwise(col("xreflkup.MKCR_SEGMENT_VALUE")).alias("MBR_UNIQ_KEY"),
    col("CmpntData.INPR_ID").alias("VBB_CMPNT_UNIQ_KEY"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

# Stage: hf_remove_dups -> scenario A (intermediate hashed file)
# Deduplicate on [MBR_UNIQ_KEY, VBB_CMPNT_UNIQ_KEY, SRC_SYS_CD_SK]
df_Snapshot_dedup = df_Transformer_6_Snapshot.dropDuplicates(["MBR_UNIQ_KEY", "VBB_CMPNT_UNIQ_KEY", "SRC_SYS_CD_SK"])

# Output to B_MBR_VBB_CMPNT_ENR (CSeqFileStage)
df_B_MBR_VBB_CMPNT_ENR = df_Snapshot_dedup.select(
    col("MBR_UNIQ_KEY"),
    col("VBB_CMPNT_UNIQ_KEY"),
    col("SRC_SYS_CD_SK")
)
b_mbr_file_path = f"{adls_path}/load/B_MBR_VBB_CMPNT_ENR.dat"
write_files(
    df_B_MBR_VBB_CMPNT_ENR,
    b_mbr_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# Stage: MbrVbbCmpntEnrPK (CContainerStage)
# Call the shared container with two inputs
params_MbrVbbCmpntEnrPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}
df_key = MbrVbbCmpntEnrPK(df_Transformer_6_Transform, df_Transformer_6_AllCol, params_MbrVbbCmpntEnrPK)

# Stage: MBR_VBB_PLN_ENR (DB2Connector => DB=IDS) to read
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
vbb_plnenr_query = f"""
SELECT VBB_CMPNT.VBB_CMPNT_UNIQ_KEY as VBB_CMPNT_UNIQ_KEY,
MBR_VBB_PLN_ENR.MBR_UNIQ_KEY as MBR_UNIQ_KEY,
MBR_VBB_PLN_ENR.MBR_VBB_PLN_ENR_SK as MBR_VBB_PLN_ENR_SK
FROM {IDSOwner}.VBB_CMPNT VBB_CMPNT, {IDSOwner}.MBR_VBB_PLN_ENR MBR_VBB_PLN_ENR
WHERE MBR_VBB_PLN_ENR.VBB_PLN_SK = VBB_CMPNT.VBB_PLN_SK
{IDSOwner}.VBB_CMPNT VBB_CMPNT, {IDSOwner}.MBR_VBB_PLN_ENR MBR_VBB_PLN_ENR
"""
df_VbbPlnEnr_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", vbb_plnenr_query)
    .load()
)

df_VbbPlnEnr = df_VbbPlnEnr_raw.select(
    col("VBB_CMPNT_UNIQ_KEY"),
    col("MBR_UNIQ_KEY"),
    col("MBR_VBB_PLN_ENR_SK")
)

# Stage: hf_mbrvbbplnenrlkup => scenario A
df_MbrVbbPlnEnrLkup_dedup = df_VbbPlnEnr.dropDuplicates(["VBB_CMPNT_UNIQ_KEY", "MBR_UNIQ_KEY"])

# Stage: Transformer_7
df_Transformer_7_joined = (
    df_key.alias("Key")
    .join(
        df_MbrVbbPlnEnrLkup_dedup.alias("MbrVbbPlnEnrLkup"),
        [
            col("Key.VBB_CMPNT_UNIQ_KEY") == col("MbrVbbPlnEnrLkup.VBB_CMPNT_UNIQ_KEY"),
            col("Key.MBR_UNIQ_KEY") == col("MbrVbbPlnEnrLkup.MBR_UNIQ_KEY")
        ],
        "left"
    )
)

df_KeyFile = df_Transformer_7_joined.select(
    col("Key.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("Key.INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    rpad(col("Key.DISCARD_IN"),1," ").alias("DISCARD_IN"),
    rpad(col("Key.PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    col("Key.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Key.ERR_CT").alias("ERR_CT"),
    col("Key.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Key.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("Key.MBR_VBB_CMPNT_ENR_SK").alias("MBR_VBB_CMPNT_ENR_SK"),
    col("Key.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("Key.VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
    col("Key.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Key.HIEL_LOG_LEVEL1").alias("HIEL_LOG_LEVEL1"),
    col("Key.HIEL_LOG_LEVEL2").alias("HIEL_LOG_LEVEL2"),
    col("Key.HIEL_LOG_LEVEL3").alias("HIEL_LOG_LEVEL3"),
    col("Key.MEIP_ENROLLED_BY").alias("MEIP_ENROLLED_BY"),
    col("Key.MEIP_STATUS").alias("MEIP_STATUS"),
    col("Key.MEIP_TERMED_BY").alias("MEIP_TERMED_BY"),
    col("Key.MEIP_TERM_REASON").alias("MEIP_TERM_REASON"),
    rpad(col("Key.MBR_CNTCT_IN"),1," ").alias("MBR_CNTCT_IN"),
    rpad(col("Key.MBR_CNTCT_LAST_UPDT_DT_SK"),10," ").alias("MBR_CNTCT_LAST_UPDT_DT_SK"),
    rpad(col("Key.MBR_VBB_CMPNT_CMPLTN_DT_SK"),10," ").alias("MBR_VBB_CMPNT_CMPLTN_DT_SK"),
    rpad(col("Key.MBR_VBB_CMPNT_ENR_DT_SK"),10," ").alias("MBR_VBB_CMPNT_ENR_DT_SK"),
    rpad(col("Key.MBR_VBB_CMPNT_TERM_DT_SK"),10," ").alias("MBR_VBB_CMPNT_TERM_DT_SK"),
    col("Key.SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
    col("Key.SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
    col("Key.VBB_VNDR_UNIQ_KEY").alias("VBB_VNDR_UNIQ_KEY"),
    col("Key.VNDR_PGM_SEQ_NO").alias("VNDR_PGM_SEQ_NO"),
    col("Key.MBR_CMPLD_ACHV_LVL_CT").alias("MBR_CMPLD_ACHV_LVL_CT"),
    col("Key.TRZ_MBR_UNVRS_ID").alias("TRZ_MBR_UNVRS_ID"),
    when((col("MbrVbbPlnEnrLkup.MBR_VBB_PLN_ENR_SK").isNull()) | (length(col("MbrVbbPlnEnrLkup.MBR_VBB_PLN_ENR_SK")) == 0),
         lit("0")).otherwise(col("MbrVbbPlnEnrLkup.MBR_VBB_PLN_ENR_SK")).alias("MBR_VBB_PLN_ENR_SK")
)

# Stage: MbrVbbCmpntEnrKey (CSeqFileStage)
mbr_vbb_cmpnt_enr_key_path = f"{adls_path}/key/IdsMbrVbbCmpntEnrExtr.MbrVbbCmpntEnr.dat.{RunId}"
write_files(
    df_KeyFile,
    mbr_vbb_cmpnt_enr_key_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)