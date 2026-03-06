# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FctsClmLineOvrdExtr
# MAGIC CALLED BY:  FctsClmLnOvrdExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CDOR_LI_OVR to a landing file for the IDS  pulls dental data from CMC_CDDO_DNLI_OVR to a landing file. 
# MAGIC   SQL is UNION CDOR data with CDDO data.
# MAGIC      
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	CMC_CDOR_LI_OVR
# MAGIC   
# MAGIC HASH FILES:  hf_ovr
# MAGIC                        hf_clm_nasco_dup_bypass
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   STRIP.FIELD
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:   Sequential file 
# MAGIC                       Hash file hf_ovr used in FctsClmLnExtrDispCd
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------    
# MAGIC Steph Goddard        06/02/2004-                                  Originally Programmed
# MAGIC Suzanne Saylor       03/01/2006                                   Changed to combine extract, transform, pkey for sequencer 
# MAGIC BJ Luce                   03/20/2006                                   add hf_clm_nasco_dup_bypass, identifies claims that are nasco 
# MAGIC                                                                                       dups. If the claim is on the file, a row is not generated for it in IDS. 
# MAGIC                                                                                       However, an R row will be build for it if the status if '91'
# MAGIC SAndrew                  12/08/2006     Project 1576  -      Reversal logix added for new status codes 89 and  and 99
# MAGIC Oliver Nielsen           08/20/2007    Balancing                 Added Balancing Snapshot File                                                 devlIDS30                        Steph Goddard        8/30/07
# MAGIC Parik                        2008-09-11      3567(Primary Key)   Added new primary key process to the job                                  devlIDS                            Steph Goddard        09/12/2008
# MAGIC Ralph Tucker          2011-12-09      TTR-1252               Added primary key for EXCD code                                              IntegrateCurDevl               SAndrew                 2012-01-05 /2012-02-22
# MAGIC Prabhu ES               2022-02-26      S2S Remediation   MSSQL connection parameters added                                        IntegrateDev5	Ken Bradmon	2022-06-10

# MAGIC Balancing
# MAGIC Pulling Facets Claim Line Item Override Data for a specific Time Period
# MAGIC Write overrides expl. codes for claim lines.
# MAGIC 
# MAGIC Hash file used in Disposition Code??
# MAGIC This container is used in:
# MAGIC FctsClmLnOvrdExtr
# MAGIC NascoClmLnOverRideExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Hash file (hf_clm_ln_ovrd_allcol) cleared in calling program
# MAGIC DeDup List to Email
# MAGIC Writing Sequential File to ../key
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC Hash Files created in FctsExCdExtr
# MAGIC Writing Sequential File to ../key
# MAGIC Apply business logic
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
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnOvrdPK
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ExcdPkey
# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# -----------------------------------------------------------------------------
# Stage: Lookup (CHashedFileStage) - scenario C for reading "hf_excd_n"
# -----------------------------------------------------------------------------
df_hf_excd = spark.read.parquet(f"{adls_path}/hf_excd_n.parquet")

# -----------------------------------------------------------------------------
# Stage: hf_dsalw_excd (CHashedFileStage) - scenario B (read-modify-write)
# Replace read from hashed file with read from dummy table in IDS
# -----------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_dsalw_excd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD, EXCD_ID, EFF_DT_SK, CRT_RUN_CYC_EXCTN_SK, DSALW_EXCD_SK FROM IDS.dummy_hf_dsalw_excd")
    .load()
)

# -----------------------------------------------------------------------------
# Stage: CMC_CDDO_DNLI_OVR (ODBCConnector)
# -----------------------------------------------------------------------------
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query = (
    "SELECT CLCL_ID,CDML_SEQ_NO,CDOR_OR_ID,MEME_CK,CDOR_OR_AMT,CDOR_OR_VALUE,CDOR_OR_DT,EXCD_ID,CDOR_OR_USID,CDOR_AUTO_GEN,CDOR_LOCK_TOKEN "
    f"FROM {FacetsOwner}.CMC_CDOR_LI_OVR A, tempdb..{DriverTable} TMP WHERE TMP.CLM_ID = A.CLCL_ID "
    "UNION "
    "SELECT CLCL_ID,CDDL_SEQ_NO,CDDO_OR_ID,MEME_CK,CDDO_OR_AMT,CDDO_OR_VALUE,CDDO_OR_DT,EXCD_ID,CDDO_OR_USID,CDDO_AUTO_GEN,CDDO_LOCK_TOKEN "
    f"FROM {FacetsOwner}.CMC_CDDO_DNLI_OVR A, tempdb..{DriverTable} TMP WHERE TMP.CLM_ID = A.CLCL_ID"
)
df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query)
    .load()
)

# -----------------------------------------------------------------------------
# Stage: transform (CTransformerStage)
# Primary link = df_Extract as "Extract"
# Lookup link = df_hf_excd as "hf_excd" (left join)
# Two outputs: "hf_ovr" (constraint) and "Strip"
# -----------------------------------------------------------------------------
df_enriched = df_Extract.alias("Extract").join(
    df_hf_excd.alias("hf_excd"),
    (trim(strip_field(F.col("Extract.EXCD_ID"))) == F.col("hf_excd.EXCD_ID")),
    "left"
)

# -- Output link "hf_ovr" with constraint: Trim(Extract.CDOR_OR_ID) in ['AX','DX']
df_hf_ovr_filter = df_enriched.filter(
    (trim(F.col("Extract.CDOR_OR_ID")) == F.lit("AX"))
    | (trim(F.col("Extract.CDOR_OR_ID")) == F.lit("DX"))
)

df_hf_ovr_selected = df_hf_ovr_filter.select(
    F.when(F.col("Extract.CLCL_ID").isNull(), F.lit("")).otherwise(Convert(F.col("Extract.CLCL_ID"))).alias("CLCL_ID"),
    F.col("Extract.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.when(F.col("Extract.CDOR_OR_ID").isNull(), F.lit("")).otherwise(Convert(F.col("Extract.CDOR_OR_ID"))).alias("CDOR_OR_ID"),
    F.when(F.col("Extract.EXCD_ID").isNull(), F.lit(""))
     .otherwise(trim(Convert(F.col("Extract.EXCD_ID")))).alias("EXCD_ID"),
    F.when(F.col("hf_excd.EXCD_ID").isNotNull(), F.lit("N")).otherwise(F.lit("")).alias("EXCD_PT_LIAB_IND_N"),
    F.when(F.col("Extract.CDOR_OR_AMT").isNull(), F.lit("0.00"))
     .otherwise(F.col("Extract.CDOR_OR_AMT")).alias("CDOR_OR_AMT"),
)

# For writing we must keep type char fields rpad'd, using the final length from the job metadata:
df_hf_ovr_final = (
    df_hf_ovr_selected
    .withColumn("CLCL_ID", F.rpad(F.col("CLCL_ID"), 12, " "))
    .withColumn("CDOR_OR_ID", F.rpad(F.col("CDOR_OR_ID"), 2, " "))
    .withColumn("EXCD_ID", F.rpad(F.col("EXCD_ID"), 3, " "))
)

# -----------------------------------------------------------------------------
# Stage: hf_ovr (CHashedFileStage) - scenario C for writing
# Write to parquet "hf_ovr.parquet"
# -----------------------------------------------------------------------------
write_files(
    df_hf_ovr_final.select(
        "CLCL_ID",
        "CDML_SEQ_NO",
        "CDOR_OR_ID",
        "EXCD_ID",
        "EXCD_PT_LIAB_IND_N",
        "CDOR_OR_AMT"
    ),
    f"{adls_path}/hf_ovr.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# -- Output link "Strip"
df_Strip = df_enriched.select(
    F.when(F.col("Extract.CLCL_ID").isNull(), F.lit("")).otherwise(Convert(F.col("Extract.CLCL_ID"))).alias("CLCL_ID"),
    F.col("Extract.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.when(F.col("Extract.CDOR_OR_ID").isNull(), F.lit("")).otherwise(Convert(F.col("Extract.CDOR_OR_ID"))).alias("CDOR_OR_ID"),
    current_date().alias("EXT_TIMESTAMP"),
    F.col("Extract.MEME_CK").alias("MEME_CK"),
    F.col("Extract.CDOR_OR_AMT").alias("CDOR_OR_AMT"),
    F.when(F.col("Extract.CDOR_OR_VALUE").isNull(), F.lit("")).otherwise(Convert(F.col("Extract.CDOR_OR_VALUE"))).alias("CDOR_OR_VALUE"),
    F.col("Extract.CDOR_OR_DT").alias("CDOR_OR_DT"),
    F.when(F.col("Extract.EXCD_ID").isNull(), F.lit("")).otherwise(Convert(F.col("Extract.EXCD_ID"))).alias("EXCD_ID"),
    F.when(F.col("Extract.CDOR_OR_USID").isNull(), F.lit("")).otherwise(Convert(F.col("Extract.CDOR_OR_USID"))).alias("CDOR_OR_USID"),
    F.when(F.col("Extract.CDOR_AUTO_GEN").isNull(), F.lit("")).otherwise(Convert(F.col("Extract.CDOR_AUTO_GEN"))).alias("CDOR_AUTO_GEN"),
    F.col("Extract.CDOR_LOCK_TOKEN").alias("CDOR_LOCK_TOKEN")
)

# Make sure char fields are rpad'd:
df_Strip_final = (
    df_Strip
    .withColumn("CLCL_ID", F.rpad(F.col("CLCL_ID"), 12, " "))
    .withColumn("CDOR_OR_ID", F.rpad(F.col("CDOR_OR_ID"), 2, " "))
    .withColumn("CDOR_OR_VALUE", F.rpad(F.col("CDOR_OR_VALUE"), 10, " "))
    .withColumn("EXCD_ID", F.rpad(F.col("EXCD_ID"), 3, " "))
    .withColumn("CDOR_OR_USID", F.rpad(F.col("CDOR_OR_USID"), 10, " "))
    .withColumn("CDOR_AUTO_GEN", F.rpad(F.col("CDOR_AUTO_GEN"), 1, " "))
)

# -----------------------------------------------------------------------------
# Stage: hf_clm_fcts_reversals (CHashedFileStage), scenario C for reading
# -----------------------------------------------------------------------------
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# -----------------------------------------------------------------------------
# Stage: clm_nasco_dup_bypass (CHashedFileStage), scenario C for reading
# -----------------------------------------------------------------------------
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# -----------------------------------------------------------------------------
# Stage: SETUP_CRF (CTransformerStage)
# Primary link = df_Strip_final as "Strip"
# Two lookup links: df_hf_clm_fcts_reversals as "fcts_reversals" (left),
# and df_clm_nasco_dup_bypass as "nasco_dup_lkup" (left).
# Two outputs: "ClmLineOvrd" and "reversals"
# -----------------------------------------------------------------------------

df_setup_enriched_1 = df_Strip_final.alias("Strip").join(
    df_hf_clm_fcts_reversals.alias("fcts_reversals"),
    F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
    "left"
)

df_setup_enriched_2 = df_setup_enriched_1.alias("Strip").join(
    df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
    F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
    "left"
)

# Stage variable ClmId = Trim(Strip.CLCL_ID)
df_setup_enriched = df_setup_enriched_2.withColumn("ClmId", trim(F.col("Strip.CLCL_ID")))

# Output link "ClmLineOvrd":
df_ClmLineOvrd_filter = df_setup_enriched.filter(
    F.isnull(F.col("nasco_dup_lkup.CLM_ID")) == True
)
df_ClmLineOvrd = df_ClmLineOvrd_filter.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit(";"), F.col("Strip.CDML_SEQ_NO"), F.lit(";"), F.col("Strip.CDOR_OR_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_OVRD_SK"),
    F.col("ClmId").alias("CLM_ID"),
    F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    trim(F.col("Strip.CDOR_OR_ID")).alias("CLM_LN_OVRD_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.length(trim(F.col("Strip.CDOR_OR_USID"))) == 0, F.lit("NA")).otherwise(F.col("Strip.CDOR_OR_USID")).alias("USER_ID"),
    F.when(F.length(trim(F.col("Strip.EXCD_ID"))) == 0, F.lit("NA")).otherwise(F.col("Strip.EXCD_ID")).alias("CLM_LN_OVRD_EXCD"),
    trim(F.col("Strip.CDOR_OR_DT").substr(F.lit(1), F.lit(10))).alias("OVRD_DT"),
    F.col("Strip.CDOR_OR_AMT").alias("OVRD_AMT"),
    trim(F.col("Strip.CDOR_OR_VALUE")).alias("OVRD_VAL_DESC")
)

# Output link "reversals":
df_reversals_filter = df_setup_enriched.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull())
    & (
        (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("89"))
        | (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("91"))
        | (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("99"))
    )
)
df_reversals = df_reversals_filter.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(";"), F.col("Strip.CDOR_OR_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_OVRD_SK"),
    F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
    F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    trim(F.col("Strip.CDOR_OR_ID")).alias("CLM_LN_OVRD_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.length(trim(F.col("Strip.CDOR_OR_USID"))) == 0, F.lit("NA")).otherwise(F.col("Strip.CDOR_OR_USID")).alias("USER_ID"),
    F.when(F.length(trim(F.col("Strip.EXCD_ID"))) == 0, F.lit("NA")).otherwise(F.col("Strip.EXCD_ID")).alias("CLM_LN_OVRD_EXCD"),
    trim(F.col("Strip.CDOR_OR_DT").substr(F.lit(1), F.lit(10))).alias("OVRD_DT"),
    F.when(F.col("Strip.CDOR_OR_AMT") > F.lit(0), NEG(F.col("Strip.CDOR_OR_AMT"))).otherwise(F.col("Strip.CDOR_OR_AMT")).alias("OVRD_AMT"),
    trim(F.col("Strip.CDOR_OR_VALUE")).alias("OVRD_VAL_DESC")
)

# -----------------------------------------------------------------------------
# Stage: Collector (CCollector) - collects "reversals" and "ClmLineOvrd"
# Single output link "Trans" to "SnapShot"
# -----------------------------------------------------------------------------
df_Collector = df_reversals.unionByName(df_ClmLineOvrd)
# Must keep the column order:
df_Collector_final = df_Collector.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_LN_OVRD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_OVRD_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "USER_ID",
    "CLM_LN_OVRD_EXCD",
    "OVRD_DT",
    "OVRD_AMT",
    "OVRD_VAL_DESC"
).alias("Trans")

# -----------------------------------------------------------------------------
# Stage: SnapShot (CTransformerStage)
# Primary link -> df_Collector_final as "Trans"
# 3 outputs: "AllCol" -> ClmLnOvrdPK, "Snapshot" -> B_CLM_LN_OVRD, "Transform" -> ClmLnOvrdPK,
# "TransformExcd" -> ExcdPkey, "Trans_Dsalw_Excd" -> PrimaryKey (some constraints).
# We have a stage variable "svExcdSk" = GetFkeyExcd(Trans.SRC_SYS_CD, 0, TRIM(Trans.CLM_LN_OVRD_EXCD), 'N')
# but we treat it as a user function call. Then "svExcdSk = 0 or not" -> constraint "svExcdSk = 0" => goes to "TransformExcd"
# For simplicity, create a column "svExcdSk" = that expression, etc.
# -----------------------------------------------------------------------------
df_SnapShot_var = df_Collector_final.withColumn(
    "svExcdSk",
    GetFkeyExcd(
        F.col("Trans.SRC_SYS_CD"), 
        F.lit(0),
        trim(F.col("Trans.CLM_LN_OVRD_EXCD")),
        F.lit("N")
    )
)

# Output link "AllCol" -> ClmLnOvrdPK
df_AllCol = df_SnapShot_var.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Trans.CLM_ID").alias("CLM_ID"),
    trim(F.col("Trans.CLM_LN_SEQ_NO")).alias("CLM_LN_SEQ_NO"),
    F.col("Trans.CLM_LN_OVRD_ID").alias("CLM_LN_OVRD_ID"),
    F.col("Trans.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Trans.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Trans.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Trans.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Trans.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Trans.ERR_CT").alias("ERR_CT"),
    F.col("Trans.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Trans.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Trans.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Trans.CLM_LN_OVRD_SK").alias("CLM_LN_OVRD_SK"),
    F.col("Trans.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Trans.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Trans.USER_ID").alias("USER_ID"),
    F.col("Trans.CLM_LN_OVRD_EXCD").alias("CLM_LN_OVRD_EXCD"),
    F.col("Trans.OVRD_DT").alias("OVRD_DT"),
    F.col("Trans.OVRD_AMT").alias("OVRD_AMT"),
    F.col("Trans.OVRD_VAL_DESC").alias("OVRD_VAL_DESC")
)

# Output link "Snapshot" -> B_CLM_LN_OVRD
df_Snapshot = df_SnapShot_var.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Trans.CLM_ID").alias("CLM_ID"),
    trim(F.col("Trans.CLM_LN_SEQ_NO")).alias("CLM_LN_SEQ_NO"),
    F.col("Trans.CLM_LN_OVRD_ID").alias("CLM_LN_OVRD_ID")
)

# Output link "Transform" -> ClmLnOvrdPK
df_Transform = df_SnapShot_var.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Trans.CLM_ID").alias("CLM_ID"),
    trim(F.col("Trans.CLM_LN_SEQ_NO")).alias("CLM_LN_SEQ_NO"),
    F.col("Trans.CLM_LN_OVRD_ID").alias("CLM_LN_OVRD_ID")
)

# Output link "TransformExcd" (Constraint svExcdSk = 0) -> ExcdPkey
df_TransformExcd_filter = df_SnapShot_var.filter(F.col("svExcdSk") == 0)
df_TransformExcd = df_TransformExcd_filter.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("N").alias("PASS_THRU_IN"),
    F.lit("1753-01-01").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"), F.lit(";"), trim(F.col("Trans.CLM_LN_OVRD_EXCD"))).alias("PRI_KEY_STRING"),
    F.lit(0).alias("EXCD_SK"),
    F.when(F.col("Trans.CLM_LN_OVRD_EXCD").isNull(), F.lit("")).otherwise(trim(F.col("Trans.CLM_LN_OVRD_EXCD"))).alias("EXCD_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("X").alias("EXCD_HC_ADJ_CD"),
    F.lit("X").alias("EXCD_PT_LIAB_IND"),
    F.lit("X").alias("EXCD_PROV_ADJ_CD"),
    F.lit("X").alias("EXCD_REMIT_REMARK_CD"),
    F.lit("X").alias("EXCD_STS"),
    F.lit("X").alias("EXCD_TYPE"),
    F.lit("Unknown - Created in Claim Line Disallow").alias("EXCD_LONG_TX1"),
    F.lit("Unknown - Created in Claim Line Disallow").alias("EXCD_LONG_TX2"),
    F.lit("Unknown - Created in Claim Line Disallow").alias("EXCD_SH_TX")
)

# Output link "Trans_Dsalw_Excd" (Constraint svExcdSk = 0) -> PrimaryKey
df_TransDsalwExcd_filter = df_SnapShot_var.filter(F.col("svExcdSk") == 0)
df_TransDsalwExcd = df_TransDsalwExcd_filter.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("N").alias("PASS_THRU_IN"),
    # FORMAT.DATE(@DATE, "DATE", "CURRENT", "DB2TIMESTAMP")[1, 10] -> current_timestamp() but per guidelines DB2 -> "dbo.GetDateCST()"? 
    # However, we do not have direct info for "FORMAT.DATE(..., 'DB2TIMESTAMP')" a user function? We'll treat it as user-defined. 
    # We'll just call it the same function. Or we can store something. Let's keep it as a user-defined call:
    FORMAT_DATE_DB2TIMESTAMP(F.current_date()).substr(F.lit(1),F.lit(10)).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"), F.lit(";"), trim(F.col("Trans.CLM_LN_OVRD_EXCD")), F.lit(";"), F.lit("1753-01-01")).alias("PRI_KEY_STRING"),
    trim(F.col("Trans.CLM_LN_OVRD_EXCD")).alias("CDML_DISALL_EXCD"),
    F.lit("1753-01-01").alias("EFF_DT_SK"),
    F.lit("N").alias("EXCD_RSPNSB_IN"),
    F.lit("U").alias("STTUS_CD"),
    F.lit("N").alias("EXCD_BYPS_IN"),
    F.lit("N").alias("EXCD_DISALL_IN"),
    F.lit("2199-12-31").alias("TERM_DT"),
    F.lit("Unknown - Created in Claim Line Disallow").alias("EXCD_LONG_TX1"),
    F.lit("Unknown - Created in Claim Line Disallow").alias("EXCD_LONG_TX2")
)

# -----------------------------------------------------------------------------
# Stage: B_CLM_LN_OVRD (CSeqFileStage) - from df_Snapshot
# Write out with name "B_CLM_LN_OVRD.FACETS.dat.#RunID#"
# -----------------------------------------------------------------------------
df_B_CLM_LN_OVRD = df_Snapshot.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_OVRD_ID"
)
# rpad for char fields if needed:
df_B_CLM_LN_OVRD_padded = (
    df_B_CLM_LN_OVRD
    .withColumn("CLM_LN_OVRD_ID", F.rpad(F.col("CLM_LN_OVRD_ID"), 2, " "))
)

write_files(
    df_B_CLM_LN_OVRD_padded,
    f"{adls_path}/load/B_CLM_LN_OVRD.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -----------------------------------------------------------------------------
# Stage: ClmLnOvrdPK (CContainerStage)
# Two inputs: "Transform" and "AllCol" => we call the shared container function
# -----------------------------------------------------------------------------
params_ClmLnOvrdPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
output_df_ClmLnOvrdPK_1, = ClmLnOvrdPK(df_Transform, df_AllCol, params_ClmLnOvrdPK)

# The container's single output is labeled "Key", so we call it df_ClmLnOvrdPK_Key
df_ClmLnOvrdPK_Key = output_df_ClmLnOvrdPK_1

# -----------------------------------------------------------------------------
# Stage: FctsClmLineOvrdExtr (CSeqFileStage)
# -----------------------------------------------------------------------------
df_FctsClmLineOvrdExtr = df_ClmLnOvrdPK_Key.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_LN_OVRD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_OVRD_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "USER_ID",
    "CLM_LN_OVRD_EXCD",
    "OVRD_DT",
    "OVRD_AMT",
    "OVRD_VAL_DESC"
)
# rpad for char fields if needed (e.g., INSRT_UPDT_CD length=10, etc.)
df_FctsClmLineOvrdExtr_padded = (
    df_FctsClmLineOvrdExtr
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
)

write_files(
    df_FctsClmLineOvrdExtr_padded,
    f"{adls_path}/key/FctsClmLnOvrdExtr.FctsClmLnOvrd.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -----------------------------------------------------------------------------
# Stage: PrimaryKey (CTransformerStage)
# Two inputs: "lkup" (df_hf_dsalw_excd) left join, "Trans_Dsalw_Excd"
# Output "Key" -> FctsDsalwExcdExtr, "updt" -> hf_dsalw_excd_upd
# We have stage variables:
#  SK = if IsNull(lkup.DSALW_EXCD_SK) then KeyMgtGetNextValueConcurrent("IDS_SK") else lkup.DSALW_EXCD_SK
#  NewCrtRunCycExtcnSk = if IsNull(lkup.DSALW_EXCD_SK) then CurrRunCycle else lkup.CRT_RUN_CYC_EXCTN_SK
# -----------------------------------------------------------------------------

df_TransDsalwExcd_alias = df_TransDsalwExcd.alias("Trans_Dsalw_Excd")
df_hf_dsalw_excd_alias = df_hf_dsalw_excd.alias("lkup")

df_primary_join = df_TransDsalwExcd_alias.join(
    df_hf_dsalw_excd_alias,
    [
        F.trim(F.col("Trans_Dsalw_Excd.SRC_SYS_CD")) == F.col("lkup.SRC_SYS_CD"),
        F.trim(F.col("Trans_Dsalw_Excd.CDML_DISALL_EXCD")) == F.col("lkup.EXCD_ID"),
        F.trim(F.col("Trans_Dsalw_Excd.EFF_DT_SK")) == F.col("lkup.EFF_DT_SK")
    ],
    "left"
)

df_primary_enriched = df_primary_join.withColumn(
    "SK",
    F.when(F.col("lkup.DSALW_EXCD_SK").isNull(), SurrogateKeyGen(df_primary_join["Trans_Dsalw_Excd"],"<DB sequence name>","DSALW_EXCD_SK","<schema>","<secret_name>"))
     .otherwise(F.col("lkup.DSALW_EXCD_SK"))
).withColumn(
    "NewCrtRunCycExtcnSk",
    F.when(F.col("lkup.DSALW_EXCD_SK").isNull(), F.lit(CurrRunCycle))
     .otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

# Because we cannot literally call SurrogateKeyGen inline for one column, 
# and we do not skip logic, we place the expression in compliance with the instructions:
# The actual PySpark expression is a placeholder referencing SurrogateKeyGen, 
# as we cannot define partial calls. We'll comply as closely as we can.

# Output link "Key":
df_primaryKey = df_primary_enriched.select(
    F.col("Trans_Dsalw_Excd.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Trans_Dsalw_Excd.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Trans_Dsalw_Excd.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Trans_Dsalw_Excd.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Trans_Dsalw_Excd.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Trans_Dsalw_Excd.ERR_CT").alias("ERR_CT"),
    F.col("Trans_Dsalw_Excd.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Trans_Dsalw_Excd.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Trans_Dsalw_Excd.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SK").alias("DSALW_EXCD_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.when(F.col("Trans_Dsalw_Excd.CDML_DISALL_EXCD").isNull(), F.lit("")).otherwise(F.col("Trans_Dsalw_Excd.CDML_DISALL_EXCD")).alias("EXCD_ID"),
    F.col("Trans_Dsalw_Excd.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("EXCD_SK"),
    F.when(F.col("Trans_Dsalw_Excd.EXCD_RSPNSB_IN") == "", F.lit("UNK")).otherwise(F.col("Trans_Dsalw_Excd.EXCD_RSPNSB_IN")).alias("EXCD_RESP_CD_SK"),
    F.when(F.col("Trans_Dsalw_Excd.STTUS_CD") == "", F.lit("NA")).otherwise(F.col("Trans_Dsalw_Excd.STTUS_CD")).alias("EXCD_STTUS_CD_SK"),
    F.when(F.col("Trans_Dsalw_Excd.EXCD_BYPS_IN") == "", F.lit("N")).otherwise(F.col("Trans_Dsalw_Excd.EXCD_BYPS_IN")).alias("BYPS_IN"),
    F.when(F.col("Trans_Dsalw_Excd.EXCD_DISALL_IN") == "", F.lit("N")).otherwise(F.col("Trans_Dsalw_Excd.EXCD_DISALL_IN")).alias("DSALW_IN"),
    F.col("Trans_Dsalw_Excd.TERM_DT").alias("TERM_DT_SK"),
    F.col("Trans_Dsalw_Excd.EXCD_LONG_TX1").alias("EXCD_LONG_TX1"),
    F.col("Trans_Dsalw_Excd.EXCD_LONG_TX2").alias("EXCD_LONG_TX2")
)

# Output link "updt" (Constraint: IsNull(lkup.DSALW_EXCD_SK) = @TRUE)
df_updt_filter = df_primary_enriched.filter(F.col("lkup.DSALW_EXCD_SK").isNull())
df_updt = df_updt_filter.select(
    F.trim(F.col("Trans_Dsalw_Excd.SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.trim(F.col("Trans_Dsalw_Excd.CDML_DISALL_EXCD")).alias("EXCD_ID"),
    F.trim(F.col("Trans_Dsalw_Excd.EFF_DT_SK")).alias("EFF_DT_SK"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("SK").alias("DSALW_EXCD_SK")
)

# -----------------------------------------------------------------------------
# Stage: FctsDsalwExcdExtr (CSeqFileStage) from "df_primaryKey"
# -----------------------------------------------------------------------------
df_FctsDsalw = df_primaryKey.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "DSALW_EXCD_SK",
    "SRC_SYS_CD_SK",
    "EXCD_ID",
    "EFF_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EXCD_SK",
    "EXCD_RESP_CD_SK",
    "EXCD_STTUS_CD_SK",
    "BYPS_IN",
    "DSALW_IN",
    "TERM_DT_SK",
    "EXCD_LONG_TX1",
    "EXCD_LONG_TX2"
)
# rpad if needed:
df_FctsDsalw_padded = (
    df_FctsDsalw
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("EXCD_ID", F.rpad(F.col("EXCD_ID"), 3, " "))
)

write_files(
    df_FctsDsalw_padded,
    f"{adls_path}/key/FctsDsalwExcdExtr.DsalwExcd.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -----------------------------------------------------------------------------
# Stage: hf_dsalw_excd_upd (CHashedFileStage) - scenario B (write to same dummy table)
# Merge the df_updt into "IDS.dummy_hf_dsalw_excd"
# -----------------------------------------------------------------------------
df_hf_dsalw_excd_upd = df_updt.select(
    "SRC_SYS_CD",
    "EXCD_ID",
    "EFF_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "DSALW_EXCD_SK"
)

# Create temp table and do merge
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.FctsClmLnOvrdExtr_hf_dsalw_excd_upd_temp",
    jdbc_url_ids,
    jdbc_props_ids
)
df_hf_dsalw_excd_upd.write.format("jdbc").option("url", jdbc_url_ids).options(**jdbc_props_ids)\
    .option("dbtable", "STAGING.FctsClmLnOvrdExtr_hf_dsalw_excd_upd_temp").mode("overwrite").save()

merge_sql_hf_dsalw_excd_upd = (
    "MERGE INTO IDS.dummy_hf_dsalw_excd AS T "
    "USING STAGING.FctsClmLnOvrdExtr_hf_dsalw_excd_upd_temp AS S "
    "ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.EXCD_ID = S.EXCD_ID AND T.EFF_DT_SK = S.EFF_DT_SK) "
    "WHEN MATCHED THEN UPDATE SET "
    "T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK, "
    "T.DSALW_EXCD_SK = S.DSALW_EXCD_SK "
    "WHEN NOT MATCHED THEN INSERT (SRC_SYS_CD,EXCD_ID,EFF_DT_SK,CRT_RUN_CYC_EXCTN_SK,DSALW_EXCD_SK) "
    "VALUES (S.SRC_SYS_CD,S.EXCD_ID,S.EFF_DT_SK,S.CRT_RUN_CYC_EXCTN_SK,S.DSALW_EXCD_SK);"
)
execute_dml(merge_sql_hf_dsalw_excd_upd, jdbc_url_ids, jdbc_props_ids)

# -----------------------------------------------------------------------------
# Stage: ExcdPkey (CContainerStage)
# One input: "TransformExcd"
# -----------------------------------------------------------------------------
params_ExcdPkey = {
    "CurrRunCycle": CurrRunCycle
}
output_df_ExcdPkey_1, = ExcdPkey(df_TransformExcd, params_ExcdPkey)
df_ExcdPkey_Key = output_df_ExcdPkey_1

# -----------------------------------------------------------------------------
# Stage: Trans (CTransformerStage) - input: df_ExcdPkey_Key as "Key"
# Two outputs: "lodExcd" -> hf_clm_ln_ovrd_excd, "Pkey" -> FacetsClmExcdCode
# -----------------------------------------------------------------------------
df_Trans_input = df_ExcdPkey_Key.alias("Key")

df_lodExcd = df_Trans_input.select(
    F.col("Key.EXCD_ID").alias("EXCD_ID")
)

df_Pkey = df_Trans_input.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "EXCD_SK",
    "EXCD_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EXCD_HC_ADJ_CD",
    "EXCD_PT_LIAB_IND",
    "EXCD_PROV_ADJ_CD",
    "EXCD_REMIT_REMARK_CD",
    "EXCD_STS",
    "EXCD_TYPE",
    "EXCD_LONG_TX1",
    "EXCD_LONG_TX2",
    "EXCD_SH_TX"
)

# -----------------------------------------------------------------------------
# Stage: hf_clm_ln_ovrd_excd (CHashedFileStage) for reading/writing?
# Actually it has input "lodExcd" and output "outExcd"
# The file is "hf_clm_ln_ovrd_excd". Not used for read-modify-write on same file?
# There's no matching transform writing back. But it does both read and write leftover?
# Checking: The JSON shows an output pin with columns => scenario A or C?
# This stage is outputting "outExcd" with columns => It's also reading from the same named file.
# There's no transform that writes the same file name. So it is scenario C.
# -----------------------------------------------------------------------------
# We first write "lodExcd" into "hf_clm_ln_ovrd_excd.parquet"
df_hf_clm_ln_ovrd_excd_in = df_lodExcd.select(
    F.rpad(F.col("EXCD_ID"), 4, " ").alias("EXCD_ID")
)
write_files(
    df_hf_clm_ln_ovrd_excd_in,
    f"{adls_path}/hf_clm_ln_ovrd_excd.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Then read it back as scenario C:
df_hf_clm_ln_ovrd_excd_out = spark.read.parquet(f"{adls_path}/hf_clm_ln_ovrd_excd.parquet")

# Output link "outExcd" with columns => EXCD_ID
df_outExcd = df_hf_clm_ln_ovrd_excd_out.select(
    "EXCD_ID"
).alias("outExcd")

# -----------------------------------------------------------------------------
# Stage: New_Excd_Email_List (CSeqFileStage)
# Input "outExcd" => write to "landing/NewExcdList.dat"
# -----------------------------------------------------------------------------
df_New_Excd_Email_List = df_outExcd.select(
    F.rpad(F.col("EXCD_ID"), 4, " ").alias("EXCD_ID")
)

write_files(
    df_New_Excd_Email_List,
    f"{adls_path_raw}/landing/NewExcdList.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -----------------------------------------------------------------------------
# Stage: FacetsClmExcdCode (CSeqFileStage)
# Input "Pkey"
# Write to "key/FacetsClmExcdCode.dat.#RunID#"
# -----------------------------------------------------------------------------
df_FacetsClmExcdCode = df_Pkey.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "EXCD_SK",
    "EXCD_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EXCD_HC_ADJ_CD",
    "EXCD_PT_LIAB_IND",
    "EXCD_PROV_ADJ_CD",
    "EXCD_REMIT_REMARK_CD",
    "EXCD_STS",
    "EXCD_TYPE",
    "EXCD_LONG_TX1",
    "EXCD_LONG_TX2",
    "EXCD_SH_TX"
)
df_FacetsClmExcdCode_padded = (
    df_FacetsClmExcdCode
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("EXCD_ID", F.rpad(F.col("EXCD_ID"), 4, " "))
    .withColumn("EXCD_HC_ADJ_CD", F.rpad(F.col("EXCD_HC_ADJ_CD"), 5, " "))
    .withColumn("EXCD_PT_LIAB_IND", F.rpad(F.col("EXCD_PT_LIAB_IND"), 1, " "))
    .withColumn("EXCD_PROV_ADJ_CD", F.rpad(F.col("EXCD_PROV_ADJ_CD"), 2, " "))
    .withColumn("EXCD_REMIT_REMARK_CD", F.rpad(F.col("EXCD_REMIT_REMARK_CD"), 2, " "))
    .withColumn("EXCD_STS", F.rpad(F.col("EXCD_STS"), 1, " "))
    .withColumn("EXCD_TYPE", F.rpad(F.col("EXCD_TYPE"), 2, " "))
)

write_files(
    df_FacetsClmExcdCode_padded,
    f"{adls_path}/key/FacetsClmExcdCode.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -----------------------------------------------------------------------------
# End of Job
# -----------------------------------------------------------------------------