# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  FctsClmExtr1Seq
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Steph Goddard        06/01/2004-                                   Originally Programmed
# MAGIC Steph Goddard        07/22/2004                                    added NullOptCode to user id field to force NA if spaces
# MAGIC SAndrew                  07/23/2004                                   added NullOptCodeNoCase to user id field to force NA if spaces
# MAGIC Steph Goddard        02/16/2006                                   Combined extract, transform, primary key for sequencer
# MAGIC BJ Luce                   03/20/2006                                   add hf_clm_nasco_dup_bypass, identifies claims that are nasco
# MAGIC                                                                                       dups. If the claim is on the file, a row is not generated for it in 
# MAGIC                                                                                        IDS. However, an R row will be build for it if the status if '91'
# MAGIC Sanderw                  12/08/2006   Project 1756            Reversal logix added for new status codes 89 and  and 99
# MAGIC Brent Leland            05/02/2007    IAD Prod. Supp.     Added current timestamp parameter to eliminate FORMAT.DATE
# MAGIC                                                                                       routine call to improve effeciency.
# MAGIC O. Nielsen                07/11/2008                                   New Primary Keying logic                                                             devlIDS                          Steph Goddard            07/17/2008
# MAGIC Prabhu ES               2022-02-28      S2S Remediation   MSSQL connection parameters added                                        IntegrateDev5                  Kalyan Neelam            2022-06-10

# MAGIC Facets Claim Override
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDateTime = get_widget_value('CurrDateTime','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_hf_clm_fcts_reversals = StructType([
    StructField("CLCL_ID", StringType(), False),
    StructField("CLCL_CUR_STS", StringType(), False),
    StructField("CLCL_PAID_DT", TimestampType(), False),
    StructField("CLCL_ID_ADJ_TO", StringType(), False),
    StructField("CLCL_ID_ADJ_FROM", StringType(), False)
])
df_hf_clm_fcts_reversals = (
    spark.read.format("parquet")
    .schema(schema_hf_clm_fcts_reversals)
    .load(f"{adls_path}/hf_clm_fcts_reversals.parquet")
)

schema_clm_nasco_dup_bypass = StructType([
    StructField("CLM_ID", StringType(), False)
])
df_clm_nasco_dup_bypass = (
    spark.read.format("parquet")
    .schema(schema_clm_nasco_dup_bypass)
    .load(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
)

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"""
SELECT
CLCL_ID,
CLOR_OR_ID,
MEME_CK,
CLOR_OR_AMOUNT,
CAST(Trim(CLOR_OR_VALUE) as char(30)) AS CLOR_OR_VALUE,
CLOR_OR_DT,
EXCD_ID,
CLOR_OR_USID,
CLOR_AUTO_GEN,
CLOR_LOCK_TOKEN
FROM {FacetsOwner}.CMC_CLOR_CL_OVR A,
tempdb..{DriverTable} TMP
WHERE TMP.CLM_ID = A.CLCL_ID
  AND A.CLOR_OR_ID <> 'XP'
"""
df_CMC_CLOR_CL_OVR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_StripField = df_CMC_CLOR_CL_OVR.select(
    strip_field(F.col("CLCL_ID")).alias("CLCL_ID"),
    strip_field(F.col("CLOR_OR_ID")).alias("CLOR_OR_ID"),
    F.lit(CurrDateTime).alias("EXT_TIMESTAMP"),
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("CLOR_OR_AMOUNT").alias("CLOR_OR_AMOUNT"),
    strip_field(F.col("CLOR_OR_VALUE")).alias("CLOR_OR_VALUE"),
    F.col("CLOR_OR_DT").alias("CLOR_OR_DT"),
    strip_field(F.col("EXCD_ID")).alias("EXCD_ID"),
    strip_field(F.col("CLOR_OR_USID")).alias("CLOR_OR_USID"),
    strip_field(F.col("CLOR_AUTO_GEN")).alias("CLOR_AUTO_GEN"),
    F.col("CLOR_LOCK_TOKEN").alias("CLOR_LOCK_TOKEN")
).alias("Strip")

df_BusinessRules = (
    df_StripField
    .join(df_hf_clm_fcts_reversals.alias("fcts_reversals"),
          F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"), "left")
    .join(df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
          F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"), "left")
)

df_ClmOvrd = df_BusinessRules.filter(
    F.col("nasco_dup_lkup.CLM_ID").isNull()
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"), F.trim(F.col("Strip.CLCL_ID")), F.lit(";"), F.trim(F.col("Strip.CLOR_OR_ID"))).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_OVRD_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.trim(F.col("Strip.CLCL_ID")).alias("CLM_ID"),
    F.trim(F.col("Strip.CLOR_OR_ID")).alias("CLM_OVRD_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.when(
        F.col("Strip.CLOR_OR_USID").isNull() | (F.length(F.trim(F.col("Strip.CLOR_OR_USID"))) == 0),
        F.lit("NA")
    ).otherwise(F.trim(F.col("Strip.CLOR_OR_USID"))).alias("USER_ID"),
    F.when(
        F.length(F.trim(F.col("Strip.EXCD_ID"))) == 0,
        F.lit("NA")
    ).otherwise(F.trim(F.col("Strip.EXCD_ID"))).alias("CLM_OVRD_EXCD"),
    F.trim(F.substring(F.col("Strip.CLOR_OR_DT"), 1, 10)).alias("OVERRIDE_DT"),
    F.col("Strip.CLOR_OR_AMOUNT").alias("OVERRIDE_AMT"),
    F.col("Strip.CLOR_OR_VALUE").alias("OVERRIDE_VAL_DESC")
)

df_reversals = df_BusinessRules.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    (F.col("fcts_reversals.CLCL_CUR_STS").isin("89", "91", "99"))
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"), F.trim(F.col("Strip.CLCL_ID")), F.lit("R;"), F.trim(F.col("Strip.CLOR_OR_ID"))).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_OVRD_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.concat(F.trim(F.col("Strip.CLCL_ID")), F.lit("R")).alias("CLM_ID"),
    F.trim(F.col("Strip.CLOR_OR_ID")).alias("CLM_OVRD_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.when(
        F.col("Strip.CLOR_OR_USID").isNull() | (F.length(F.trim(F.col("Strip.CLOR_OR_USID"))) == 0),
        F.lit("NA")
    ).otherwise(F.trim(F.col("Strip.CLOR_OR_USID"))).alias("USER_ID"),
    F.when(
        F.length(F.trim(F.col("Strip.EXCD_ID"))) == 0,
        F.lit("NA")
    ).otherwise(F.trim(F.col("Strip.EXCD_ID"))).alias("CLM_OVRD_EXCD"),
    F.trim(F.substring(F.col("Strip.CLOR_OR_DT"), 1, 10)).alias("OVERRIDE_DT"),
    Neg(F.col("Strip.CLOR_OR_AMOUNT")).alias("OVERRIDE_AMT"),
    F.col("Strip.CLOR_OR_VALUE").alias("OVERRIDE_VAL_DESC")
)

df_Collector = df_reversals.unionByName(df_ClmOvrd)

df_AllColl = df_Collector.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_OVRD_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_OVRD_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK"),
    F.col("USER_ID"),
    F.col("CLM_OVRD_EXCD"),
    F.col("OVERRIDE_DT"),
    F.col("OVERRIDE_AMT"),
    F.col("OVERRIDE_VAL_DESC")
).alias("Transform1")

df_Transform = df_Collector.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_OVRD_ID").alias("CLM_OVRD_ID")
)

df_Load = df_AllColl.select(
    F.col("CLM_ID").alias("CLCL_ID"),
    F.col("CLM_OVRD_ID").alias("CLOR_OR_ID")
)

df_Transformer = df_Load.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("CLOR_OR_ID").alias("CLM_OVRD_ID")
)

df_B_CLM_OVRD = df_Transformer.select(
    F.col("SRC_SYS_CD_SK"),
    rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
    rpad(F.col("CLM_OVRD_ID"), 2, " ").alias("CLM_OVRD_ID")
)

write_files(
    df_B_CLM_OVRD,
    f"{adls_path}/load/B_CLM_OVRD.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmOvrdPK
# COMMAND ----------

params = {
    "DriverTable": DriverTable,
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "CurrDateTime": CurrDateTime,
    "FacetsOwner": FacetsOwner,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner,
    "SrcSysCdSk": SrcSysCdSk
}
df_ClmOvrdPK = ClmOvrdPK(df_AllColl, df_Transform, params)

df_FctsClmOvrdExtr = df_ClmOvrdPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_OVRD_SK"),
    F.col("SRC_SYS_CD_SK"),
    rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
    F.col("CLM_OVRD_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK"),
    F.col("USER_ID"),
    rpad(F.col("CLM_OVRD_EXCD"), 10, " ").alias("CLM_OVRD_EXCD"),
    rpad(F.col("OVERRIDE_DT"), 10, " ").alias("OVERRIDE_DT"),
    F.col("OVERRIDE_AMT"),
    F.col("OVERRIDE_VAL_DESC")
)

write_files(
    df_FctsClmOvrdExtr,
    f"{adls_path}/key/FctsClmOvrdExtr.FctsClmOvrd.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)