# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Extracs data and assigns Primary Keys for the IDS table MBR_VBB_PLN_ENR.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Raja Gummadi         2013-05-13          4963 VBB Phase III     Initial Programming                                                                      IntegrateNewDevl          Bhoomi Dasari            7/3/2013
# MAGIC Raja Gummadi         2013-10-16          4963 VBB                    Added snapshot file to the job                                                     IntegrateNewDevl           Kalyan Neelam           2013-10-25

# MAGIC IDS MBR_VBB_PLN_ENR Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
VBBOwner = get_widget_value('VBBOwner','')
vbb_secret_name = get_widget_value('vbb_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunId = get_widget_value('RunId','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
LastRunDtm = get_widget_value('LastRunDtm','')
CurrDate = get_widget_value('CurrDate','')
Environment = get_widget_value('Environment','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/MbrVbbPlnEnrPK
# COMMAND ----------

jdbc_url_vbb, jdbc_props_vbb = get_db_config(vbb_secret_name)

extract_query = (
    "SELECT "
    "TZIM_MEHP_MEMBER_HLTH_PGM.MEHP_MEMBER_ID, "
    "TZIM_MEHP_MEMBER_HLTH_PGM.HIPL_ID, "
    "TZIM_MEHP_MEMBER_HLTH_PGM.MEHP_ENROLL_DT, "
    "TZIM_MEHP_MEMBER_HLTH_PGM.MEHP_TERM_DT, "
    "TZIM_MEHP_MEMBER_HLTH_PGM.MEHP_TERM_REASON, "
    "TZIM_MEHP_MEMBER_HLTH_PGM.MEHP_STATUS, "
    "TZIM_MEHP_MEMBER_HLTH_PGM.MEHP_ENROLLED_BY, "
    "TZIM_MEHP_MEMBER_HLTH_PGM.MEHP_TERMED_BY, "
    "TZIM_MEHP_MEMBER_HLTH_PGM.MEHP_COMPLETE_DT, "
    "TZIM_MEHP_MEMBER_HLTH_PGM.HIEL_PHY_LEVEL1, "
    "TZIM_MEHP_MEMBER_HLTH_PGM.HIEL_PHY_LEVEL2, "
    "TZIM_MEHP_MEMBER_HLTH_PGM.HIEL_PHY_LEVEL3, "
    "TZIM_MEHP_MEMBER_HLTH_PGM.MEHP_CREATE_DT, "
    "TZIM_MEHP_MEMBER_HLTH_PGM.MEHP_UPDATE_DT "
    "FROM " + VBBOwner + ".TZIM_MEHP_MEMBER_HLTH_PGM"
)

df_IHMFCONSTITUENT_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_vbb)
    .options(**jdbc_props_vbb)
    .option("query", extract_query)
    .load()
)

xref_query = (
    "SELECT "
    "TZIM_MKCR_MEMBER_KEY_CROSS_REFERENCE.MKCR_GUID, "
    "TZIM_MKCR_MEMBER_KEY_CROSS_REFERENCE.MKCR_SEGMENT_VALUE "
    "FROM " + VBBOwner + ".TZIM_MKCR_MEMBER_KEY_CROSS_REFERENCE "
    "WHERE TZIM_MKCR_MEMBER_KEY_CROSS_REFERENCE.MKCR_SEGMENT_NAME = 'MEME_CK'"
)

df_IHMFCONSTITUENT_Xref = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_vbb)
    .options(**jdbc_props_vbb)
    .option("query", xref_query)
    .load()
)

df_Trim_input = df_IHMFCONSTITUENT_Extract.withColumn(
    "svHiplId",
    F.when(
        F.isnull(trim(F.regexp_replace(F.col("HIPL_ID"), "[\r\n\t]", ""))) |
        (F.length(trim(F.regexp_replace(F.col("HIPL_ID"), "[\r\n\t]", ""))) == 0),
        F.lit("N")
    ).otherwise(F.lit("Y"))
).withColumn(
    "svGoodRecord",
    F.when(F.col("svHiplId") == F.lit("Y"), F.lit("Y")).otherwise(F.lit("N"))
)

df_Trim_CmpntData = df_Trim_input.filter(
    F.col("svGoodRecord") == "Y"
).select(
    trim(F.col("MEHP_MEMBER_ID")).alias("MEHP_MEMBER_ID"),
    trim(F.col("HIPL_ID")).alias("HIPL_ID"),
    trim(F.col("MEHP_ENROLL_DT")).alias("MEHP_ENROLL_DT"),
    trim(F.col("MEHP_TERM_DT")).alias("MEHP_TERM_DT"),
    F.upper(trim(F.col("MEHP_TERM_REASON"))).alias("MEHP_TERM_REASON"),
    F.upper(trim(F.col("MEHP_STATUS"))).alias("MEHP_STATUS"),
    F.upper(trim(F.col("MEHP_ENROLLED_BY"))).alias("MEHP_ENROLLED_BY"),
    F.upper(trim(F.col("MEHP_TERMED_BY"))).alias("MEHP_TERMED_BY"),
    trim(F.col("MEHP_COMPLETE_DT")).alias("MEHP_COMPLETE_DT"),
    trim(F.col("HIEL_PHY_LEVEL1")).alias("HIEL_LOG_LEVEL1"),
    trim(F.col("HIEL_PHY_LEVEL2")).alias("HIEL_LOG_LEVEL2"),
    trim(F.col("HIEL_PHY_LEVEL3")).alias("HIEL_LOG_LEVEL3"),
    trim(F.col("MEHP_CREATE_DT")).alias("MEHP_CREATE_DT"),
    trim(F.col("MEHP_UPDATE_DT")).alias("MEHP_UPDATE_DT")
)

df_Trim_ErrorFile = df_Trim_input.filter(
    F.col("svGoodRecord") == "N"
).select(
    F.col("MEHP_MEMBER_ID"),
    F.col("HIPL_ID"),
    F.col("MEHP_ENROLL_DT"),
    F.col("MEHP_TERM_DT"),
    F.col("MEHP_TERM_REASON"),
    F.col("MEHP_STATUS"),
    F.col("MEHP_ENROLLED_BY"),
    F.col("MEHP_TERMED_BY"),
    F.col("MEHP_COMPLETE_DT"),
    F.col("HIEL_PHY_LEVEL1").alias("HIEL_LOG_LEVEL1"),
    F.col("HIEL_PHY_LEVEL2").alias("HIEL_LOG_LEVEL2"),
    F.col("HIEL_PHY_LEVEL3").alias("HIEL_LOG_LEVEL3"),
    F.col("MEHP_CREATE_DT"),
    F.col("MEHP_UPDATE_DT")
)

write_files(
    df_Trim_ErrorFile,
    f"{adls_path_publish}/external/MBR_VBB_PLN_ENR_ErrorsOut_{RunId}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

df_xref_dedup = dedup_sort(
    df_IHMFCONSTITUENT_Xref,
    partition_cols=["MKCR_GUID"],
    sort_cols=[]
)

df_Transformer_6_input = df_Trim_CmpntData.alias("CmpntData").join(
    df_xref_dedup.alias("xreflkup"),
    F.col("CmpntData.MEHP_MEMBER_ID") == F.col("xreflkup.MKCR_GUID"),
    "left"
).withColumn(
    "svSrcSysCrtDt",
    F.when(
        F.isnull(F.col("CmpntData.MEHP_CREATE_DT")) | (F.length(F.col("CmpntData.MEHP_CREATE_DT")) == 0),
        F.lit("1753-01-01 00:00:00.000")
    ).otherwise(F.col("CmpntData.MEHP_CREATE_DT"))
).withColumn(
    "svSrcSysUptDt",
    F.when(
        F.isnull(F.col("CmpntData.MEHP_UPDATE_DT")) | (F.length(F.col("CmpntData.MEHP_UPDATE_DT")) == 0),
        F.lit("1753-01-01 00:00:00.000")
    ).otherwise(F.col("CmpntData.MEHP_UPDATE_DT"))
).withColumn(
    "svMbrVbbPlnCmpltnDtSk",
    F.when(
        F.isnull(F.col("CmpntData.MEHP_COMPLETE_DT")) | (F.length(F.col("CmpntData.MEHP_COMPLETE_DT")) == 0),
        F.lit("1753-01-01")
    ).otherwise(F.substring(F.col("CmpntData.MEHP_COMPLETE_DT"), 1, 10))
).withColumn(
    "svMbrVbbPlnEnrDtSk",
    F.when(
        F.isnull(F.col("CmpntData.MEHP_ENROLL_DT")) | (F.length(F.col("CmpntData.MEHP_ENROLL_DT")) == 0),
        F.lit("1753-01-01")
    ).otherwise(F.substring(F.col("CmpntData.MEHP_ENROLL_DT"), 1, 10))
).withColumn(
    "svMbrVbbPlnTermDtSk",
    F.when(
        F.isnull(F.col("CmpntData.MEHP_TERM_DT")) | (F.length(F.col("CmpntData.MEHP_TERM_DT")) == 0),
        F.lit("2199-12-31")
    ).otherwise(F.substring(F.col("CmpntData.MEHP_TERM_DT"), 1, 10))
)

df_Transformer_6_AllCol = df_Transformer_6_input.select(
    F.when(
        F.isnull(F.col("xreflkup.MKCR_GUID")) | (F.length(F.col("xreflkup.MKCR_GUID")) == 0),
        F.lit(0)
    ).otherwise(F.col("xreflkup.MKCR_SEGMENT_VALUE")).alias("MBR_UNIQ_KEY"),
    F.col("CmpntData.HIPL_ID").alias("VBB_PLN_UNIQ_KEY"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD_1"),
    F.concat(trim(F.col("xreflkup.MKCR_SEGMENT_VALUE")), F.lit(";"), trim(F.col("CmpntData.HIPL_ID")), F.lit(";"), F.lit(SrcSysCd)).alias("PRI_KEY_STRING"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CmpntData.HIPL_ID").alias("HIPL_ID"),
    F.col("CmpntData.HIEL_LOG_LEVEL1").alias("HIEL_LOG_LEVEL1"),
    F.col("CmpntData.HIEL_LOG_LEVEL2").alias("HIEL_LOG_LEVEL2"),
    F.col("CmpntData.HIEL_LOG_LEVEL3").alias("HIEL_LOG_LEVEL3"),
    F.when(
        F.isnull(F.col("CmpntData.MEHP_ENROLLED_BY")) | (F.length(F.col("CmpntData.MEHP_ENROLLED_BY")) == 0),
        F.lit("NA")
    ).otherwise(F.col("CmpntData.MEHP_ENROLLED_BY")).alias("MEHP_ENROLLED_BY"),
    F.when(
        F.isnull(F.col("CmpntData.MEHP_STATUS")) | (F.length(F.col("CmpntData.MEHP_STATUS")) == 0),
        F.lit("NA")
    ).otherwise(F.col("CmpntData.MEHP_STATUS")).alias("MEHP_STATUS"),
    F.when(
        F.isnull(F.col("CmpntData.MEHP_TERMED_BY")) | (F.length(F.col("CmpntData.MEHP_TERMED_BY")) == 0),
        F.lit("NA")
    ).otherwise(F.col("CmpntData.MEHP_TERMED_BY")).alias("MEHP_TERMED_BY"),
    F.when(
        F.isnull(F.col("CmpntData.MEHP_TERM_REASON")) | (F.length(F.col("CmpntData.MEHP_TERM_REASON")) == 0),
        F.lit("NA")
    ).otherwise(F.col("CmpntData.MEHP_TERM_REASON")).alias("MEHP_TERM_REASON"),
    F.col("svMbrVbbPlnCmpltnDtSk").alias("MBR_VBB_PLN_CMPLTN_DT_SK"),
    F.col("svMbrVbbPlnEnrDtSk").alias("MBR_VBB_PLN_ENR_DT_SK"),
    F.col("svMbrVbbPlnTermDtSk").alias("MBR_VBB_PLN_TERM_DT_SK"),
    F.col("svSrcSysCrtDt").alias("SRC_SYS_CRT_DTM"),
    F.col("svSrcSysUptDt").alias("SRC_SYS_UPDT_DTM"),
    F.col("CmpntData.MEHP_MEMBER_ID").alias("TRZ_MBR_UNVRS_ID")
)

df_Transformer_6_Transform = df_Transformer_6_input.select(
    F.when(
        F.isnull(F.col("xreflkup.MKCR_GUID")) | (F.length(F.col("xreflkup.MKCR_GUID")) == 0),
        F.lit(0)
    ).otherwise(F.col("xreflkup.MKCR_SEGMENT_VALUE")).alias("MBR_UNIQ_KEY"),
    F.col("CmpntData.HIPL_ID").alias("VBB_PLN_UNIQ_KEY"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

df_Transformer_6_Snapshot = df_Transformer_6_input.select(
    F.when(
        F.isnull(F.col("xreflkup.MKCR_GUID")) | (F.length(F.col("xreflkup.MKCR_GUID")) == 0),
        F.lit(0)
    ).otherwise(F.col("xreflkup.MKCR_SEGMENT_VALUE")).alias("MBR_UNIQ_KEY"),
    F.col("CmpntData.HIPL_ID").alias("VBB_PLN_UNIQ_KEY"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

write_files(
    df_Transformer_6_Snapshot.select("MBR_UNIQ_KEY", "VBB_PLN_UNIQ_KEY", "SRC_SYS_CD_SK"),
    f"{adls_path}/load/B_MBR_VBB_PLN_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

params_MbrVbbPlnEnrPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}

df_MbrVbbPlnEnrPK_Key = MbrVbbPlnEnrPK(df_Transformer_6_Transform, df_Transformer_6_AllCol, params_MbrVbbPlnEnrPK)

df_MbrVbbPlnEnrPK_Key_final = df_MbrVbbPlnEnrPK_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("MBR_VBB_PLN_ENR_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("VBB_PLN_UNIQ_KEY"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("HIPL_ID"),
    F.col("HIEL_LOG_LEVEL1"),
    F.col("HIEL_LOG_LEVEL2"),
    F.col("HIEL_LOG_LEVEL3"),
    F.col("MEHP_ENROLLED_BY"),
    F.col("MEHP_STATUS"),
    F.col("MEHP_TERMED_BY"),
    F.col("MEHP_TERM_REASON"),
    rpad(F.col("MBR_VBB_PLN_CMPLTN_DT_SK"), 10, " ").alias("MBR_VBB_PLN_CMPLTN_DT_SK"),
    rpad(F.col("MBR_VBB_PLN_ENR_DT_SK"), 10, " ").alias("MBR_VBB_PLN_ENR_DT_SK"),
    rpad(F.col("MBR_VBB_PLN_TERM_DT_SK"), 10, " ").alias("MBR_VBB_PLN_TERM_DT_SK"),
    F.col("SRC_SYS_CRT_DTM"),
    F.col("SRC_SYS_UPDT_DTM"),
    F.col("TRZ_MBR_UNVRS_ID")
)

write_files(
    df_MbrVbbPlnEnrPK_Key_final,
    f"{adls_path}/key/IdsMbrVbbPlnEnrExtr.MbrVbbPlnEnr.dat.{RunId}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)