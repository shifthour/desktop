# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC PROCESSING:   Extracs data and assigns Primary Keys for the IDS table VBB_CMPNT.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Raja Gummadi         2013-05-13          4963 VBB Phase III     Initial Programming                                                                      IntegrateNewDevl           Bhoomi Dasari           5/16/2013
# MAGIC Raja Gummadi         2013-10-16          4963 VBB                    Added snapshot file to the job                                                     IntegrateNewDevl           Kalyan Neelam           2013-10-25

# MAGIC IDS VBB_CMPNT Extract
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
from pyspark.sql.types import IntegerType, StringType, FloatType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/VbbCmpntPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
VBBOwner = get_widget_value('VBBOwner','')
vbb_secret_name = get_widget_value('vbb_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunID = get_widget_value('RunID','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
LastRunDtm = get_widget_value('LastRunDtm','')
CurrDate = get_widget_value('CurrDate','')
Environment = get_widget_value('Environment','')

jdbc_url_vbb, jdbc_props_vbb = get_db_config(vbb_secret_name)

# IHMFCONSTITUENT - OutputPin "Extract"
extract_query = f"""
SELECT 
TZIM_INPR_INCENTIVE_PGM.INPR_ID, 
TEMP.INPR_NAME, 
TEMP.INPR_MEMBER_VALIDATION, 
TEMP.INPR_PROGRAM_REIN_DAYS, 
TEMP.PTPT_ID, 
TEMP.INPR_REENROLL_IND, 
TEMP.INPR_FINISH_DAYS, 
TEMP.INPR_FINISH_RULE, 
TZIM_INPR_INCENTIVE_PGM.HIPL_ID, 
TEMP.CDVL_VALUE_FUNCTION, 
TEMP.INPR_COMPLIANCE_METHOD, 
TZIM_INPR_INCENTIVE_PGM.INPR_ID_TEMPLATE, 
TZIM_INPR_INCENTIVE_PGM.INPR_CREATE_DT, 
TZIM_INPR_INCENTIVE_PGM.INPR_UPDATE_DT 
FROM {VBBOwner}.TZIM_INPR_INCENTIVE_PGM TZIM_INPR_INCENTIVE_PGM, 
{VBBOwner}.TZIM_INPR_INCENTIVE_PGM TEMP,
{VBBOwner}.TZIM_HIPL_HEALTH_PLAN TZIM_HIPL_HEALTH_PLAN 
WHERE 
TZIM_INPR_INCENTIVE_PGM.HIPL_ID = TZIM_HIPL_HEALTH_PLAN.HIPL_ID 
AND TZIM_HIPL_HEALTH_PLAN.HIPL_ACTIVATED_IND <> 0
AND TZIM_INPR_INCENTIVE_PGM.INPR_ID_TEMPLATE > 0
AND TZIM_INPR_INCENTIVE_PGM.INPR_ID_TEMPLATE = TEMP.INPR_ID
AND TZIM_INPR_INCENTIVE_PGM.INPR_UPDATE_DT > '{LastRunDtm}'

UNION

SELECT 
TZIM_INPR_INCENTIVE_PGM.INPR_ID, 
TZIM_INPR_INCENTIVE_PGM.INPR_NAME, 
TZIM_INPR_INCENTIVE_PGM.INPR_MEMBER_VALIDATION, 
TZIM_INPR_INCENTIVE_PGM.INPR_PROGRAM_REIN_DAYS, 
TZIM_INPR_INCENTIVE_PGM.PTPT_ID, 
TZIM_INPR_INCENTIVE_PGM.INPR_REENROLL_IND, 
TZIM_INPR_INCENTIVE_PGM.INPR_FINISH_DAYS, 
TZIM_INPR_INCENTIVE_PGM.INPR_FINISH_RULE, 
TZIM_INPR_INCENTIVE_PGM.HIPL_ID, 
TZIM_INPR_INCENTIVE_PGM.CDVL_VALUE_FUNCTION, 
TZIM_INPR_INCENTIVE_PGM.INPR_COMPLIANCE_METHOD, 
TZIM_INPR_INCENTIVE_PGM.INPR_ID_TEMPLATE, 
TZIM_INPR_INCENTIVE_PGM.INPR_CREATE_DT, 
TZIM_INPR_INCENTIVE_PGM.INPR_UPDATE_DT 
FROM {VBBOwner}.TZIM_INPR_INCENTIVE_PGM TZIM_INPR_INCENTIVE_PGM, 
{VBBOwner}.TZIM_HIPL_HEALTH_PLAN TZIM_HIPL_HEALTH_PLAN 
WHERE 
TZIM_INPR_INCENTIVE_PGM.HIPL_ID = TZIM_HIPL_HEALTH_PLAN.HIPL_ID 
AND TZIM_HIPL_HEALTH_PLAN.HIPL_ACTIVATED_IND <> 0
AND TZIM_INPR_INCENTIVE_PGM.INPR_ID_TEMPLATE = 0
AND TZIM_INPR_INCENTIVE_PGM.INPR_UPDATE_DT > '{LastRunDtm}'

UNION

SELECT 
TZIM_INPR_INCENTIVE_PGM.INPR_ID, 
TZIM_INPR_INCENTIVE_PGM.INPR_NAME, 
TZIM_INPR_INCENTIVE_PGM.INPR_MEMBER_VALIDATION, 
TZIM_INPR_INCENTIVE_PGM.INPR_PROGRAM_REIN_DAYS, 
TZIM_INPR_INCENTIVE_PGM.PTPT_ID, 
TZIM_INPR_INCENTIVE_PGM.INPR_REENROLL_IND, 
TZIM_INPR_INCENTIVE_PGM.INPR_FINISH_DAYS, 
TZIM_INPR_INCENTIVE_PGM.INPR_FINISH_RULE, 
TZIM_INPR_INCENTIVE_PGM.HIPL_ID, 
TZIM_INPR_INCENTIVE_PGM.CDVL_VALUE_FUNCTION, 
TZIM_INPR_INCENTIVE_PGM.INPR_COMPLIANCE_METHOD, 
TZIM_INPR_INCENTIVE_PGM.INPR_ID_TEMPLATE, 
TZIM_INPR_INCENTIVE_PGM.INPR_CREATE_DT, 
TZIM_INPR_INCENTIVE_PGM.INPR_UPDATE_DT 
FROM {VBBOwner}.TZIM_INPR_INCENTIVE_PGM TZIM_INPR_INCENTIVE_PGM, 
{VBBOwner}.TZIM_HIPL_HEALTH_PLAN TZIM_HIPL_HEALTH_PLAN 
WHERE 
TZIM_INPR_INCENTIVE_PGM.HIPL_ID = TZIM_HIPL_HEALTH_PLAN.HIPL_ID 
AND TZIM_HIPL_HEALTH_PLAN.HIPL_ACTIVATED_IND <> 0
AND TZIM_INPR_INCENTIVE_PGM.INPR_ID_TEMPLATE NOT IN (SELECT T.INPR_ID FROM TZIM_INPR_INCENTIVE_PGM T)
AND TZIM_INPR_INCENTIVE_PGM.INPR_UPDATE_DT > '{LastRunDtm}'
"""

df_IHMFCONSTITUENT_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_vbb)
    .options(**jdbc_props_vbb)
    .option("query", extract_query)
    .load()
)

df_IHMFCONSTITUENT_Extract = df_IHMFCONSTITUENT_Extract.select(
    "INPR_ID",
    "INPR_NAME",
    "INPR_MEMBER_VALIDATION",
    "INPR_PROGRAM_REIN_DAYS",
    "PTPT_ID",
    "INPR_REENROLL_IND",
    "INPR_FINISH_DAYS",
    "INPR_FINISH_RULE",
    "HIPL_ID",
    "CDVL_VALUE_FUNCTION",
    "INPR_COMPLIANCE_METHOD",
    "INPR_ID_TEMPLATE",
    "INPR_CREATE_DT",
    "INPR_UPDATE_DT"
)

# IHMFCONSTITUENT - OutputPin "AchiLvl"
achilvl_query = f"""
SELECT 
TZIM_INPR_INCENTIVE_PGM.INPR_ID, 
Count(TZIM_IPAL_ACHIEVEMENT_LEVEL.INPR_ID)
FROM {VBBOwner}.TZIM_INPR_INCENTIVE_PGM TZIM_INPR_INCENTIVE_PGM, 
{VBBOwner}.TZIM_IPAL_ACHIEVEMENT_LEVEL TZIM_IPAL_ACHIEVEMENT_LEVEL 
WHERE 
TZIM_IPAL_ACHIEVEMENT_LEVEL.INPR_ID = TZIM_INPR_INCENTIVE_PGM.INPR_ID_TEMPLATE
AND TZIM_INPR_INCENTIVE_PGM.INPR_ID_TEMPLATE > 0
GROUP BY
TZIM_INPR_INCENTIVE_PGM.INPR_ID

UNION

SELECT 
TZIM_INPR_INCENTIVE_PGM.INPR_ID, 
Count(TZIM_IPAL_ACHIEVEMENT_LEVEL.INPR_ID)
FROM {VBBOwner}.TZIM_INPR_INCENTIVE_PGM TZIM_INPR_INCENTIVE_PGM, 
{VBBOwner}.TZIM_IPAL_ACHIEVEMENT_LEVEL TZIM_IPAL_ACHIEVEMENT_LEVEL 
WHERE 
TZIM_IPAL_ACHIEVEMENT_LEVEL.INPR_ID = TZIM_INPR_INCENTIVE_PGM.INPR_ID
AND TZIM_INPR_INCENTIVE_PGM.INPR_ID_TEMPLATE = 0
GROUP BY
TZIM_INPR_INCENTIVE_PGM.INPR_ID
"""

df_IHMFCONSTITUENT_AchiLvl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_vbb)
    .options(**jdbc_props_vbb)
    .option("query", achilvl_query)
    .load()
)

df_IHMFCONSTITUENT_AchiLvl = df_IHMFCONSTITUENT_AchiLvl.select(
    F.col("INPR_ID"),
    F.col("COUNT")
)

# IHMFCONSTITUENT - OutputPin "ptptid"
ptptid_query = f"""
SELECT 
TZIM_PTPT_PGM_TYPE.PTPT_ID, 
TZIM_PTPT_PGM_TYPE.PTPT_NAME 
FROM {VBBOwner}.TZIM_PTPT_PGM_TYPE TZIM_PTPT_PGM_TYPE
"""

df_IHMFCONSTITUENT_ptptid = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_vbb)
    .options(**jdbc_props_vbb)
    .option("query", ptptid_query)
    .load()
)

df_IHMFCONSTITUENT_ptptid = df_IHMFCONSTITUENT_ptptid.select(
    "PTPT_ID",
    "PTPT_NAME"
)

# Trim Stage (CTransformerStage) for "Extract" link
df_Trim = df_IHMFCONSTITUENT_Extract.withColumn(
    "tmp_HIPL_ID", trim(strip_field(F.col("HIPL_ID")))
).withColumn(
    "svHiplId",
    F.when(
        (F.col("tmp_HIPL_ID").isNull()) | (F.length(F.col("tmp_HIPL_ID")) == 0),
        F.lit("N")
    ).otherwise(F.lit("Y"))
).withColumn(
    "tmp_INPR_ID", trim(strip_field(F.col("INPR_ID")))
).withColumn(
    "svInprid",
    F.when(
        (F.col("tmp_INPR_ID").isNull()) | (F.length(F.col("tmp_INPR_ID")) == 0),
        F.lit("N")
    ).otherwise(F.lit("Y"))
).withColumn(
    "svFinishRules",
    F.when(
        (F.col("INPR_FINISH_RULE").cast(FloatType()) > F.lit(0.0)) & (F.col("INPR_FINISH_DAYS").cast(FloatType()) <= F.lit(0.0)),
        F.lit("N")
    ).otherwise(F.lit("Y"))
).withColumn(
    "tmp_INPR_METHOD", trim(strip_field(F.col("INPR_COMPLIANCE_METHOD")))
).withColumn(
    "svMethod",
    F.when(
        (F.col("tmp_INPR_METHOD").isNull()) | (F.length(F.col("tmp_INPR_METHOD")) == 0),
        F.lit("N")
    ).otherwise(F.lit("Y"))
).withColumn(
    "tmp_CDVL_VALUE_FUNCTION", trim(strip_field(F.col("CDVL_VALUE_FUNCTION")))
).withColumn(
    "svFunction",
    F.when(
        (F.col("tmp_CDVL_VALUE_FUNCTION").isNull()) | (F.length(F.col("tmp_CDVL_VALUE_FUNCTION")) == 0),
        F.lit("N")
    ).otherwise(F.lit("Y"))
).withColumn(
    "svGoodRecord",
    F.when(
        (F.col("svHiplId") == "Y") &
        (F.col("svFinishRules") == "Y") &
        (F.col("svFunction") == "Y") &
        (F.col("svInprid") == "Y") &
        (F.col("svMethod") == "Y"),
        F.lit("Y")
    ).otherwise(F.lit("N"))
)

df_Trim_CmpntData = df_Trim.filter(F.col("svGoodRecord") == "Y").select(
    F.trim(strip_field(F.col("INPR_ID"))).alias("INPR_ID"),
    F.upper(F.trim(F.col("INPR_NAME"))).alias("INPR_NAME"),
    F.trim(F.col("INPR_MEMBER_VALIDATION")).alias("INPR_MEMBER_VALIDATION"),
    F.trim(F.col("INPR_PROGRAM_REIN_DAYS")).alias("INPR_PROGRAM_REIN_DAYS"),
    F.trim(F.col("PTPT_ID")).alias("PTPT_ID"),
    F.trim(F.col("INPR_REENROLL_IND")).alias("INPR_REENROLL_IND"),
    F.trim(F.col("INPR_FINISH_DAYS")).alias("INPR_FINISH_DAYS"),
    F.upper(F.trim(F.col("INPR_FINISH_RULE"))).alias("INPR_FINISH_RULE"),
    F.trim(F.col("HIPL_ID")).alias("HIPL_ID"),
    F.upper(F.trim(F.col("CDVL_VALUE_FUNCTION"))).alias("CDVL_VALUE_FUNCTION"),
    F.upper(F.trim(F.col("INPR_COMPLIANCE_METHOD"))).alias("INPR_COMPLIANCE_METHOD"),
    F.trim(F.col("INPR_ID_TEMPLATE")).alias("INPR_ID_TEMPLATE"),
    F.trim(F.col("INPR_CREATE_DT")).alias("INPR_CREATE_DT"),
    F.trim(F.col("INPR_UPDATE_DT")).alias("INPR_UPDATE_DT")
)

df_Trim_ErrorFile = df_Trim.filter(F.col("svGoodRecord") == "N").select(
    "INPR_ID",
    "INPR_NAME",
    "INPR_MEMBER_VALIDATION",
    "INPR_PROGRAM_REIN_DAYS",
    "PTPT_ID",
    "INPR_REENROLL_IND",
    "INPR_FINISH_DAYS",
    "INPR_FINISH_RULE",
    "HIPL_ID",
    "CDVL_VALUE_FUNCTION",
    "INPR_COMPLIANCE_METHOD",
    "INPR_ID_TEMPLATE",
    "INPR_CREATE_DT",
    "INPR_UPDATE_DT"
)

# ErrorFile (CSeqFileStage)
file_path_error = f"{adls_path_publish}/external/VBB_CMPNT_ErrorsOut_{RunID}.dat"
write_files(
    df_Trim_ErrorFile,
    file_path_error,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# hf_vbbcmpnt_cntid (CHashedFileStage) - Scenario A => deduplicate on key INPR_ID
df_hf_vbbcmpnt_cntid = df_IHMFCONSTITUENT_AchiLvl.dropDuplicates(["INPR_ID"])

# hf_vbbcmpnt_ptptnm (CHashedFileStage) - Scenario A => deduplicate on key PTPT_ID
df_hf_vbbcmpnt_ptptnm = df_IHMFCONSTITUENT_ptptid.dropDuplicates(["PTPT_ID"])

# Transformer_6 (CTransformerStage)
df_Transformer_6_temp = (
    df_Trim_CmpntData.alias("CmpntData")
    .join(
        df_hf_vbbcmpnt_cntid.alias("lvllkup"),
        F.col("CmpntData.INPR_ID") == F.col("lvllkup.INPR_ID"),
        "left"
    )
    .join(
        df_hf_vbbcmpnt_ptptnm.alias("ptptlkup"),
        F.col("CmpntData.PTPT_ID") == F.col("ptptlkup.PTPT_ID"),
        "left"
    )
)

# Create columns for stage variables
df_Transformer_6_stagevars = df_Transformer_6_temp.withColumn(
    "svSrcSysCrtDt",
    F.when(
        (F.col("CmpntData.INPR_CREATE_DT").isNull()) | (F.length(F.col("CmpntData.INPR_CREATE_DT")) == 0),
        F.lit("1753-01-01 00:00:00.000")
    ).otherwise(F.col("CmpntData.INPR_CREATE_DT"))
).withColumn(
    "svSrcSysUptDt",
    F.when(
        (F.col("CmpntData.INPR_UPDATE_DT").isNull()) | (F.length(F.col("CmpntData.INPR_UPDATE_DT")) == 0),
        F.lit("1753-01-01 00:00:00.000")
    ).otherwise(F.col("CmpntData.INPR_UPDATE_DT"))
)

df_Transformer_6_allcol = df_Transformer_6_stagevars.select(
    F.col("CmpntData.INPR_ID").alias("VBB_CMPNT_UNIQ_KEY"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD_1"),
    F.concat(F.trim(F.col("CmpntData.INPR_ID")), F.lit(";"), F.lit(SrcSysCd)).alias("PRI_KEY_STRING"),
    F.lit(0).alias("VBB_CMPNT_SK"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CmpntData.HIPL_ID").alias("HIPL_ID"),
    F.when(
        (F.col("CmpntData.INPR_FINISH_RULE").isNull()) | (F.length(F.col("CmpntData.INPR_FINISH_RULE")) == 0),
        F.lit("NA")
    ).otherwise(F.col("CmpntData.INPR_FINISH_RULE")).alias("INPR_FINISH_RULE"),
    F.when(
        (F.col("CmpntData.CDVL_VALUE_FUNCTION").isNull()) | (F.length(F.col("CmpntData.CDVL_VALUE_FUNCTION")) == 0),
        F.lit("NA")
    ).otherwise(F.col("CmpntData.CDVL_VALUE_FUNCTION")).alias("CDVL_VALUE_FUNCTION"),
    F.when(
        (F.col("CmpntData.INPR_COMPLIANCE_METHOD").isNull()) | (F.length(F.col("CmpntData.INPR_COMPLIANCE_METHOD")) == 0),
        F.lit("NA")
    ).otherwise(F.col("CmpntData.INPR_COMPLIANCE_METHOD")).alias("INPR_COMPLIANCE_METHOD"),
    F.when(F.col("CmpntData.INPR_MEMBER_VALIDATION") == F.lit("1"), F.lit("Y")).otherwise(F.lit("N")).alias("MBR_ACHV_VALID_ALW_IN"),
    F.when(F.col("CmpntData.INPR_REENROLL_IND") == F.lit("1"), F.lit("Y")).otherwise(F.lit("N")).alias("VBB_CMPNT_REENR_IN"),
    F.col("svSrcSysCrtDt").alias("SRC_SYS_CRT_DTM"),
    F.col("svSrcSysUptDt").alias("SRC_SYS_UPDT_DTM"),
    F.when(
        (F.col("lvllkup.INPR_ID").isNull()) | (F.length(F.col("lvllkup.INPR_ID")) == 0),
        F.lit("0")
    ).otherwise(F.col("lvllkup.COUNT").cast(StringType())).alias("VBB_CMPNT_ACHV_LVL_CT"),
    F.when(
        (F.col("CmpntData.INPR_FINISH_DAYS").isNull()) | (F.length(F.col("CmpntData.INPR_FINISH_DAYS")) == 0),
        F.lit(0)
    ).otherwise(F.col("CmpntData.INPR_FINISH_DAYS").cast(IntegerType())).alias("VBB_CMPNT_FNSH_DAYS_NO"),
    F.when(
        (F.col("CmpntData.INPR_PROGRAM_REIN_DAYS").isNull()) | (F.length(F.col("CmpntData.INPR_PROGRAM_REIN_DAYS")) == 0),
        F.lit(0)
    ).otherwise(F.col("CmpntData.INPR_PROGRAM_REIN_DAYS").cast(IntegerType())).alias("VBB_CMPNT_REINST_DAYS_NO"),
    F.when(
        (F.col("CmpntData.INPR_ID_TEMPLATE").isNull()) | (F.length(F.col("CmpntData.INPR_ID_TEMPLATE")) == 0),
        F.lit(0)
    ).otherwise(F.col("CmpntData.INPR_ID_TEMPLATE").cast(IntegerType())).alias("VBB_TMPLT_UNIQ_KEY"),
    F.when(
        (F.col("CmpntData.INPR_NAME").isNull()) | (F.length(F.col("CmpntData.INPR_NAME")) == 0),
        F.lit("UNK")
    ).otherwise(F.col("CmpntData.INPR_NAME")).alias("VBB_CMPNT_NM"),
    F.when(
        (F.col("ptptlkup.PTPT_ID").isNull()) | (F.length(F.col("ptptlkup.PTPT_ID")) == 0),
        F.lit("UNK")
    ).otherwise(F.upper(F.col("ptptlkup.PTPT_NAME"))).alias("VBB_CMPNT_TYP_NM")
)

df_Transformer_6_transform = df_Transformer_6_stagevars.select(
    F.col("CmpntData.INPR_ID").alias("VBB_CMPNT_UNIQ_KEY"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

df_Transformer_6_snapshot = df_Transformer_6_stagevars.select(
    F.col("CmpntData.INPR_ID").alias("VBB_CMPNT_UNIQ_KEY"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

# B_VBB_CMPNT (CSeqFileStage) - write "Snapshot"
file_path_b_vbb_cmpnt = f"{adls_path}/load/B_VBB_CMPNT.dat"
write_files(
    df_Transformer_6_snapshot,
    file_path_b_vbb_cmpnt,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# VbbCmpntPK (CContainerStage) with 2 inputs => "Transform", "AllCol"
params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}

df_VbbCmpntPK_Key = VbbCmpntPK(df_Transformer_6_transform, df_Transformer_6_allcol, params)

# VbbCmpntKey (CSeqFileStage) - write final
file_path_vbbcmpnt_key = f"{adls_path}/key/IhmfConstituentVbbCmpntExtr.VbbCmpnt.dat.{RunID}"

# Before writing, apply rpad for char/varchar columns with defined lengths
df_VbbCmpntPK_Key_final = df_VbbCmpntPK_Key
df_VbbCmpntPK_Key_final = df_VbbCmpntPK_Key_final.withColumn(
    "INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " ")
).withColumn(
    "DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " ")
).withColumn(
    "PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " ")
).withColumn(
    "MBR_ACHV_VALID_ALW_IN", F.rpad(F.col("MBR_ACHV_VALID_ALW_IN"), 1, " ")
).withColumn(
    "VBB_CMPNT_REENR_IN", F.rpad(F.col("VBB_CMPNT_REENR_IN"), 1, " ")
)

# Select all columns in the defined order from the job
df_VbbCmpntPK_Key_final = df_VbbCmpntPK_Key_final.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "VBB_CMPNT_SK",
    "VBB_CMPNT_UNIQ_KEY",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "HIPL_ID",
    "INPR_FINISH_RULE",
    "CDVL_VALUE_FUNCTION",
    "INPR_COMPLIANCE_METHOD",
    "MBR_ACHV_VALID_ALW_IN",
    "VBB_CMPNT_REENR_IN",
    "SRC_SYS_CRT_DTM",
    "SRC_SYS_UPDT_DTM",
    "VBB_CMPNT_ACHV_LVL_CT",
    "VBB_CMPNT_FNSH_DAYS_NO",
    "VBB_CMPNT_REINST_DAYS_NO",
    "VBB_TMPLT_UNIQ_KEY",
    "VBB_CMPNT_NM",
    "VBB_CMPNT_TYP_NM"
)

write_files(
    df_VbbCmpntPK_Key_final,
    file_path_vbbcmpnt_key,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)