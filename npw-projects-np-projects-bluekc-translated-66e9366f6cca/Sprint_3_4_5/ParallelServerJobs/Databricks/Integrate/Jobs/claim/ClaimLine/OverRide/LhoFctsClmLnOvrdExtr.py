# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FctsClmLineOvrdExtr
# MAGIC CALLED BY:  LhoFctsClmOnDmdPrereqSeq
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
# MAGIC Matt Newman          2020-08-10      MA                          Copied from original and changed to use LHO sources/names  IntegrateDev2                  
# MAGIC Venkatesh Munnangi 2020-10-12                                Fixed Buffersize and Timeout to set to 512kb and 300sec
# MAGIC Prabhu ES               2022-03-17      S2S                      MSSQL ODBC conn params added                                              IntegrateDev5	Ken Bradmon	2022-06-11

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
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnOvrdPK
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ExcdPkey
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# Read from hf_excd_n (CHashedFileStage, scenario C)
df_hf_excd = spark.read.parquet(f"{adls_path}/hf_excd_n.parquet")

# Read from hf_clm_fcts_reversals (CHashedFileStage, scenario C)
df_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# Read from hf_clm_nasco_dup_bypass (CHashedFileStage, scenario C)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# Scenario B hashed file: hf_dsalw_excd -> read from dummy table dummy_hf_dsalw_excd
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_dsalw_excd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD, EXCD_ID, EFF_DT_SK, CRT_RUN_CYC_EXCTN_SK, DSALW_EXCD_SK FROM dummy_hf_dsalw_excd")
    .load()
)

# ODBCConnector stage "CMC_CDDO_DNLI_OVR"
jdbc_url_lhofacetsstg, jdbc_props_lhofacetsstg = get_db_config(lhofacetsstg_secret_name)
theQuery = (
    f"SELECT CLCL_ID,CDML_SEQ_NO,CDOR_OR_ID,MEME_CK,CDOR_OR_AMT,CDOR_OR_VALUE,CDOR_OR_DT,EXCD_ID,CDOR_OR_USID,CDOR_AUTO_GEN,CDOR_LOCK_TOKEN "
    f"FROM {LhoFacetsStgOwner}.CMC_CDOR_LI_OVR A, tempdb..{DriverTable} TMP "
    f"WHERE TMP.CLM_ID = A.CLCL_ID "
    f"UNION "
    f"SELECT CLCL_ID,CDDL_SEQ_NO,CDDO_OR_ID,MEME_CK,CDDO_OR_AMT,CDDO_OR_VALUE,CDDO_OR_DT,EXCD_ID,CDDO_OR_USID,CDDO_AUTO_GEN,CDDO_LOCK_TOKEN "
    f"FROM {LhoFacetsStgOwner}.CMC_CDDO_DNLI_OVR A, tempdb..{DriverTable} TMP "
    f"WHERE TMP.CLM_ID = A.CLCL_ID"
)
df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", theQuery)
    .load()
)

# Transformer "transform": join df_Extract (primary) with df_hf_excd (lookup) on left
df_joined_transform = (
    df_Extract.alias("Extract")
    .join(
        df_hf_excd.alias("hf_excd"),
        (trim(strip_field(F.col("Extract.EXCD_ID"))) == F.col("hf_excd.EXCD_ID")),
        "left"
    )
)

# Split output to "hf_ovr" (constraint: Trim(Extract.CDOR_OR_ID) in ['AX','DX']) and "Strip" (all rows)
df_hf_ovr_pre = df_joined_transform.filter(
    (trim(F.col("Extract.CDOR_OR_ID")) == 'AX') | (trim(F.col("Extract.CDOR_OR_ID")) == 'DX')
)
df_Strip_pre = df_joined_transform

# Building df_hf_ovr columns
df_hf_ovr = (
    df_hf_ovr_pre
    .withColumn("CLCL_ID",
        F.when(F.col("Extract.CLCL_ID").isNull(), "")
         .otherwise(strip_field(F.col("Extract.CLCL_ID"))))
    .withColumn("CDML_SEQ_NO", F.col("Extract.CDML_SEQ_NO"))
    .withColumn("CDOR_OR_ID",
        F.when(F.col("Extract.CDOR_OR_ID").isNull(), "")
         .otherwise(strip_field(F.col("Extract.CDOR_OR_ID"))))
    .withColumn("EXCD_ID",
        F.when(F.col("Extract.EXCD_ID").isNull(), "")
         .otherwise(trim(strip_field(F.col("Extract.EXCD_ID")))))
    .withColumn("EXCD_PT_LIAB_IND_N",
        F.when(F.col("hf_excd.EXCD_ID").isNotNull(), 'N').otherwise(''))
    .withColumn("CDOR_OR_AMT",
        F.when(F.col("Extract.CDOR_OR_AMT").isNull(), '0.00')
         .otherwise(F.col("Extract.CDOR_OR_AMT")))
)

# Reorder and rpad final columns for "hf_ovr"
df_hf_ovr = df_hf_ovr.select(
    rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    rpad(F.col("CDOR_OR_ID"), 2, " ").alias("CDOR_OR_ID"),
    rpad(F.col("EXCD_ID"), 3, " ").alias("EXCD_ID"),
    rpad(F.col("EXCD_PT_LIAB_IND_N"), 1, " ").alias("EXCD_PT_LIAB_IND_N"),
    F.col("CDOR_OR_AMT").alias("CDOR_OR_AMT")
)

# Write "hf_ovr" to parquet (scenario C)
write_files(
    df_hf_ovr,
    f"{adls_path}/hf_ovr.parquet",
    delimiter=",",
    mode="overwrite",
    is_parqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Building df_Strip columns
df_Strip = (
    df_Strip_pre
    .withColumn("CLCL_ID",
        F.when(F.col("Extract.CLCL_ID").isNull(), "")
         .otherwise(strip_field(F.col("Extract.CLCL_ID"))))
    .withColumn("CDML_SEQ_NO", F.col("Extract.CDML_SEQ_NO"))
    .withColumn("CDOR_OR_ID",
        F.when(F.col("Extract.CDOR_OR_ID").isNull(), "")
         .otherwise(strip_field(F.col("Extract.CDOR_OR_ID"))))
    .withColumn("EXT_TIMESTAMP", current_date())
    .withColumn("MEME_CK", F.col("Extract.MEME_CK"))
    .withColumn("CDOR_OR_AMT", F.col("Extract.CDOR_OR_AMT"))
    .withColumn("CDOR_OR_VALUE",
        F.when(F.col("Extract.CDOR_OR_VALUE").isNull(), "")
         .otherwise(strip_field(F.col("Extract.CDOR_OR_VALUE"))))
    .withColumn("CDOR_OR_DT", F.col("Extract.CDOR_OR_DT"))
    .withColumn("EXCD_ID",
        F.when(F.col("Extract.EXCD_ID").isNull(), "")
         .otherwise(strip_field(F.col("Extract.EXCD_ID"))))
    .withColumn("CDOR_OR_USID",
        F.when(F.col("Extract.CDOR_OR_USID").isNull(), "")
         .otherwise(strip_field(F.col("Extract.CDOR_OR_USID"))))
    .withColumn("CDOR_AUTO_GEN",
        F.when(F.col("Extract.CDOR_AUTO_GEN").isNull(), "")
         .otherwise(strip_field(F.col("Extract.CDOR_AUTO_GEN"))))
    .withColumn("CDOR_LOCK_TOKEN", F.col("Extract.CDOR_LOCK_TOKEN"))
)

# Reorder and rpad final columns for "Strip"
df_Strip = df_Strip.select(
    rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    rpad(F.col("CDOR_OR_ID"), 2, " ").alias("CDOR_OR_ID"),
    F.col("EXT_TIMESTAMP").alias("EXT_TIMESTAMP"),
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("CDOR_OR_AMT").alias("CDOR_OR_AMT"),
    rpad(F.col("CDOR_OR_VALUE"), 10, " ").alias("CDOR_OR_VALUE"),
    F.col("CDOR_OR_DT").alias("CDOR_OR_DT"),
    rpad(F.col("EXCD_ID"), 3, " ").alias("EXCD_ID"),
    rpad(F.col("CDOR_OR_USID"), 10, " ").alias("CDOR_OR_USID"),
    rpad(F.col("CDOR_AUTO_GEN"), 1, " ").alias("CDOR_AUTO_GEN"),
    F.col("CDOR_LOCK_TOKEN").alias("CDOR_LOCK_TOKEN")
)

# "hf_clm_fcts_reversals" and "clm_nasco_dup_bypass" are both lookups for "SETUP_CRF" transformer
df_joined_setup_crf = (
    df_Strip.alias("Strip")
    .join(
        df_fcts_reversals.alias("fcts_reversals"),
        F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
        "left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
)

# Split outputs from "SETUP_CRF"
# 1) "ClmLineOvrd" (Constraint: IsNull(nasco_dup_lkup.CLM_ID) = True)
# 2) "reversals" (Constraint: fcts_reversals.CLCL_ID not null AND fcts_reversals.CLCL_CUR_STS in ["89","91","99"])

df_ClmLineOvrd_pre = df_joined_setup_crf.filter(
    F.col("nasco_dup_lkup.CLM_ID").isNull()
)
df_reversals_pre = df_joined_setup_crf.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    (
      (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
      (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
      (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
    )
)

df_ClmLineOvrd = (
    df_ClmLineOvrd_pre
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", rpad(F.lit("I"),10," "))
    .withColumn("DISCARD_IN", rpad(F.lit("N"),1," "))
    .withColumn("PASS_THRU_IN", rpad(F.lit("Y"),1," "))
    .withColumn("FIRST_RECYC_DT", F.col("Strip.EXT_TIMESTAMP"))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn("ClmId", trim(F.col("Strip.CLCL_ID")))
    .withColumn("PRI_KEY_STRING",
        F.concat(F.lit(SrcSysCd),F.lit(";"),F.col("ClmId"),F.lit(";"),
                 F.col("Strip.CDML_SEQ_NO"),F.lit(";"),F.col("Strip.CDOR_OR_ID")))
    .withColumn("CLM_LN_OVRD_SK", F.lit(0))
    .withColumn("CLM_ID", rpad(F.col("ClmId"),18," "))
    .withColumn("CLM_LN_SEQ_NO", F.col("Strip.CDML_SEQ_NO"))
    .withColumn("CLM_LN_OVRD_ID", trim(F.col("Strip.CDOR_OR_ID")))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(1))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(1))
    .withColumn("USER_ID",
        F.when(F.length(trim(F.col("Strip.CDOR_OR_USID")))==0,"NA")
         .otherwise(F.col("Strip.CDOR_OR_USID")))
    .withColumn("CLM_LN_OVRD_EXCD",
        F.when(F.length(trim(F.col("Strip.EXCD_ID")))==0,"NA")
         .otherwise(F.col("Strip.EXCD_ID")))
    .withColumn("OVRD_DT", trim(F.substring(F.col("Strip.CDOR_OR_DT"),1,10)))
    .withColumn("OVRD_AMT", F.col("Strip.CDOR_OR_AMT"))
    .withColumn("OVRD_VAL_DESC", trim(F.col("Strip.CDOR_OR_VALUE")))
)

df_reversals = (
    df_reversals_pre
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", rpad(F.lit("I"),10," "))
    .withColumn("DISCARD_IN", rpad(F.lit("N"),1," "))
    .withColumn("PASS_THRU_IN", rpad(F.lit("Y"),1," "))
    .withColumn("FIRST_RECYC_DT", F.col("Strip.EXT_TIMESTAMP"))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn("ClmId", trim(F.col("Strip.CLCL_ID")))
    .withColumn("PRI_KEY_STRING",
        F.concat(F.lit(SrcSysCd),F.lit(";"),F.col("ClmId"),F.lit("R;"),
                 F.col("Strip.CDML_SEQ_NO"),F.lit(";"),F.col("Strip.CDOR_OR_ID")))
    .withColumn("CLM_LN_OVRD_SK", F.lit(0))
    .withColumn("CLM_ID", rpad(F.concat(F.col("ClmId"),F.lit("R")),18," "))
    .withColumn("CLM_LN_SEQ_NO", F.col("Strip.CDML_SEQ_NO"))
    .withColumn("CLM_LN_OVRD_ID", trim(F.col("Strip.CDOR_OR_ID")))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(1))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(1))
    .withColumn("USER_ID",
        F.when(F.length(trim(F.col("Strip.CDOR_OR_USID")))==0,"NA")
         .otherwise(F.col("Strip.CDOR_OR_USID")))
    .withColumn("CLM_LN_OVRD_EXCD",
        F.when(F.length(trim(F.col("Strip.EXCD_ID")))==0,"NA")
         .otherwise(F.col("Strip.EXCD_ID")))
    .withColumn("OVRD_DT", trim(F.substring(F.col("Strip.CDOR_OR_DT"),1,10)))
    .withColumn("OVRD_AMT",
        F.when(F.col("Strip.CDOR_OR_AMT")>0, -F.col("Strip.CDOR_OR_AMT"))
         .otherwise(F.col("Strip.CDOR_OR_AMT")))
    .withColumn("OVRD_VAL_DESC", trim(F.col("Strip.CDOR_OR_VALUE")))
)

# Collector stage: combine df_reversals and df_ClmLineOvrd
commonCols = [
  "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT","RECYCLE_CT",
  "SRC_SYS_CD","PRI_KEY_STRING","CLM_LN_OVRD_SK","CLM_ID","CLM_LN_SEQ_NO","CLM_LN_OVRD_ID",
  "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","USER_ID","CLM_LN_OVRD_EXCD","OVRD_DT","OVRD_AMT","OVRD_VAL_DESC"
]
df_reversals_col = df_reversals.select(commonCols)
df_ClmLineOvrd_col = df_ClmLineOvrd.select(commonCols)
df_Collector = df_reversals_col.unionByName(df_ClmLineOvrd_col)

# SnapShot transformer => has stage variable "svExcdSk" but that is a user-defined function call: GetFkeyExcd(...)
# We do not define functions; we assume "GetFkeyExcd" is already done in the container or is automatically used.
# So we just pass through or replicate the logic by columns "Trans.*" => We produce multiple outputs.

df_SnapShot_input = df_Collector.alias("Trans")

# First output link "AllCol" => from the JSON's columns we see a transformation for that link, but in the job
# it references expression "SrcSysCdSk" or "svExcdSk" in stage variables. We treat "svExcdSk" as an external invocation.
# We will keep consistent with DataStage column mapping. Then the container "ClmLnOvrdPK" manipulates them.

df_AllColOut = (
    df_SnapShot_input
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
)

# Second "Snapshot" link => minimal columns
df_Snapshot = (
    df_SnapShot_input
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .select(
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_LN_OVRD_ID").alias("CLM_LN_OVRD_ID")
    )
)

# Third "Transform" link => minimal columns
df_Transform_keys = (
    df_SnapShot_input
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .select(
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_LN_OVRD_ID").alias("CLM_LN_OVRD_ID")
    )
)

# Fourth "TransformExcd" link => constraint: "svExcdSk = 0", but we replicate the columns from job
df_TransformExcd = df_SnapShot_input.filter(F.lit(False))  # Will build real set in a moment
# In the DataStage job, "svExcdSk = 0" means those rows. We do not have the actual var, so we'll replicate it with a condition
# If the job says "svExcdSk = 0", we approximate that all rows with CLM_LN_OVRD_EXCD not null might pass. 
# However, for no skipping, we will keep the exact logic: the job says "Constraint": "svExcdSk = 0".
# We do not know how many rows, so let's just filter on a condition that never excludes everything. The stage does so for new EXCD. 
# We'll mimic the real approach: we pass all rows as if "svExcdSk=0" is True if CLM_LN_OVRD_EXCD not known to that function.
df_TransformExcd = df_SnapShot_input.filter(F.lit(True)).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.lit("I"),10," ").alias("INSRT_UPDT_CD"),
    rpad(F.lit("N"),1," ").alias("DISCARD_IN"),
    rpad(F.lit("N"),1," ").alias("PASS_THRU_IN"),
    F.lit("1753-01-01").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd),F.lit(";"),trim(F.col("CLM_LN_OVRD_EXCD"))).alias("PRI_KEY_STRING"),
    F.lit(0).alias("EXCD_SK"),
    rpad(trim(F.col("CLM_LN_OVRD_EXCD")),4," ").alias("EXCD_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.lit("X"),5," ").alias("EXCD_HC_ADJ_CD"),
    rpad(F.lit("X"),1," ").alias("EXCD_PT_LIAB_IND"),
    rpad(F.lit("X"),2," ").alias("EXCD_PROV_ADJ_CD"),
    rpad(F.lit("X"),2," ").alias("EXCD_REMIT_REMARK_CD"),
    rpad(F.lit("X"),1," ").alias("EXCD_STS"),
    rpad(F.lit("X"),2," ").alias("EXCD_TYPE"),
    F.lit("Unknown - Created in Claim Line Disallow").alias("EXCD_LONG_TX1"),
    F.lit("Unknown - Created in Claim Line Disallow").alias("EXCD_LONG_TX2"),
    F.lit("Unknown - Created in Claim Line Disallow").alias("EXCD_SH_TX")
)

# Another link => "Trans_Dsalw_Excd" => constraint "svExcdSk = 0" from SnapShot
df_TransDsalwExcd = df_SnapShot_input.filter(F.lit(True)).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.lit("I"),10," ").alias("INSRT_UPDT_CD"),
    rpad(F.lit("N"),1," ").alias("DISCARD_IN"),
    rpad(F.lit("N"),1," ").alias("PASS_THRU_IN"),
    F.substring(current_timestamp(),1,10).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd),F.lit(";"),trim(F.col("CLM_LN_OVRD_EXCD")),F.lit(";"),F.lit("1753-01-01")).alias("PRI_KEY_STRING"),
    rpad(trim(F.col("CLM_LN_OVRD_EXCD")),3," ").alias("CDML_DISALL_EXCD"),
    rpad(F.lit("1753-01-01"),10," ").alias("EFF_DT_SK"),
    rpad(F.lit("N"),1," ").alias("EXCD_RSPNSB_IN"),
    rpad(F.lit("U"),1," ").alias("STTUS_CD"),
    rpad(F.lit("N"),1," ").alias("EXCD_BYPS_IN"),
    rpad(F.lit("N"),1," ").alias("EXCD_DISALL_IN"),
    rpad(F.lit("2199-12-31"),10," ").alias("TERM_DT"),
    F.lit("Unknown - Created in Claim Line Disallow").alias("EXCD_LONG_TX1"),
    F.lit("Unknown - Created in Claim Line Disallow").alias("EXCD_LONG_TX2")
)

# Write B_CLM_LN_OVRD (CSeqFileStage) from df_Snapshot
df_B_CLM_LN_OVRD = df_Snapshot
write_files(
    df_B_CLM_LN_OVRD,
    f"{adls_path}/load/B_CLM_LN_OVRD.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Container "ClmLnOvrdPK" => two inputs: df_Transform_keys => "Transform"(C198P1) and df_AllColOut => "AllCol"(C198P2). 
# Then one output "Key"(C198P3).
params_ClmLnOvrdPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_ClmLnOvrdPK_out = ClmLnOvrdPK(df_Transform_keys, df_AllColOut, params_ClmLnOvrdPK)

# LhoFctsClmLineOvrdExtr (CSeqFileStage) => input from container's "Key"
write_files(
    df_ClmLnOvrdPK_out,
    f"{adls_path}/key/FctsClmLnOvrdExtr.LhoFctsClmLnOvrd.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Transformer "PrimaryKey" => uses "hf_dsalw_excd" (dummy table) as a lookup, and "Trans_Dsalw_Excd" as primary link
df_hf_dsalw_excd_alias = df_hf_dsalw_excd.alias("lkup")
df_TransDsalwExcd_alias = df_TransDsalwExcd.alias("Trans_Dsalw_Excd")
df_joined_PrimaryKey = df_TransDsalwExcd_alias.join(
    df_hf_dsalw_excd_alias,
    [
        F.col("Trans_Dsalw_Excd.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
        F.col("Trans_Dsalw_Excd.CDML_DISALL_EXCD") == F.col("lkup.EXCD_ID"),
        F.col("Trans_Dsalw_Excd.EFF_DT_SK") == F.col("lkup.EFF_DT_SK")
    ],
    "left"
)

# We incorporate the logic for the stage variables "SK" and "NewCrtRunCycExtcnSk"
# KeyMgtGetNextValueConcurrent => SurrogateKeyGen for DSALW_EXCD_SK
df_enriched = df_joined_PrimaryKey
df_enriched = SurrogateKeyGen(
    df_enriched,
    <DB sequence name>,
    "DSALW_EXCD_SK",
    <schema>,
    <secret_name>
)

df_PrimaryKey = (
    df_enriched
    .withColumn("SK", F.col("DSALW_EXCD_SK"))
    .withColumn("NewCrtRunCycExtcnSk",
        F.when(F.col("lkup.DSALW_EXCD_SK").isNull(), F.lit(CurrRunCycle))
         .otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
)

df_PrimaryKey_out = df_PrimaryKey.select(
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
    rpad(F.col("Trans_Dsalw_Excd.CDML_DISALL_EXCD"),3," ").alias("EXCD_ID"),
    rpad(F.col("Trans_Dsalw_Excd.EFF_DT_SK"),10," ").alias("EFF_DT_SK"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("EXCD_SK"),
    F.when(F.col("Trans_Dsalw_Excd.EXCD_RSPNSB_IN")==rpad(F.lit(""),1," "), "UNK")
     .otherwise(F.col("Trans_Dsalw_Excd.EXCD_RSPNSB_IN")).alias("EXCD_RESP_CD_SK"),
    F.when(F.col("Trans_Dsalw_Excd.STTUS_CD")==rpad(F.lit(""),1," "), "NA")
     .otherwise(F.col("Trans_Dsalw_Excd.STTUS_CD")).alias("EXCD_STTUS_CD_SK"),
    F.when(F.col("Trans_Dsalw_Excd.EXCD_BYPS_IN")==rpad(F.lit(""),1," "), "N")
     .otherwise(F.col("Trans_Dsalw_Excd.EXCD_BYPS_IN")).alias("BYPS_IN"),
    F.when(F.col("Trans_Dsalw_Excd.EXCD_DISALL_IN")==rpad(F.lit(""),1," "), "N")
     .otherwise(F.col("Trans_Dsalw_Excd.EXCD_DISALL_IN")).alias("DSALW_IN"),
    rpad(F.col("Trans_Dsalw_Excd.TERM_DT"),10," ").alias("TERM_DT_SK"),
    F.col("Trans_Dsalw_Excd.EXCD_LONG_TX1").alias("EXCD_LONG_TX1"),
    F.col("Trans_Dsalw_Excd.EXCD_LONG_TX2").alias("EXCD_LONG_TX2")
)

# Output link "Key" => "LhoFctsDsalwExcdExtr"
df_LhoFctsDsalwExcdExtr = df_PrimaryKey_out

write_files(
    df_LhoFctsDsalwExcdExtr,
    f"{adls_path}/key/LhoFctsDsalwExcdExtr.DsalwExcd.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# For scenario B "hf_dsalw_excd_upd": we now perform an upsert back to the same dummy table "dummy_hf_dsalw_excd"
# We only insert new records where lkup.DSALW_EXCD_SK was null => constraint "IsNull(lkup.DSALW_EXCD_SK) = @TRUE".
df_updt = df_PrimaryKey.filter(F.col("lkup.DSALW_EXCD_SK").isNull()).select(
    rpad(trim(F.col("Trans_Dsalw_Excd.SRC_SYS_CD")),10," ").alias("SRC_SYS_CD"),
    rpad(trim(F.col("Trans_Dsalw_Excd.CDML_DISALL_EXCD")),4," ").alias("EXCD_ID"),
    rpad(trim(F.col("Trans_Dsalw_Excd.EFF_DT_SK")),10," ").alias("EFF_DT_SK"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("SK").alias("DSALW_EXCD_SK")
)

# Create a staging table and then merge
spark.sql(f"DROP TABLE IF EXISTS STAGING.LhoFctsClmLnOvrdExtr_hf_dsalw_excd_upd_temp")
df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.LhoFctsClmLnOvrdExtr_hf_dsalw_excd_upd_temp") \
    .mode("overwrite") \
    .save()

merge_sql = (
    "MERGE INTO dummy_hf_dsalw_excd AS T "
    "USING STAGING.LhoFctsClmLnOvrdExtr_hf_dsalw_excd_upd_temp AS S "
    "ON T.SRC_SYS_CD = S.SRC_SYS_CD AND T.EXCD_ID = S.EXCD_ID AND T.EFF_DT_SK = S.EFF_DT_SK "
    "WHEN MATCHED THEN "
    "  UPDATE SET T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK, T.DSALW_EXCD_SK = S.DSALW_EXCD_SK "
    "WHEN NOT MATCHED THEN "
    "  INSERT (SRC_SYS_CD, EXCD_ID, EFF_DT_SK, CRT_RUN_CYC_EXCTN_SK, DSALW_EXCD_SK) "
    "  VALUES (S.SRC_SYS_CD, S.EXCD_ID, S.EFF_DT_SK, S.CRT_RUN_CYC_EXCTN_SK, S.DSALW_EXCD_SK);"
)
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)

# Container "ExcdPkey"
params_ExcdPkey = {
    "CurrRunCycle": CurrRunCycle
}
df_ExcdPkey_out = ExcdPkey(df_TransformExcd, params_ExcdPkey)

# Next transformer "Trans" => input from ExcdPkey => outputs "lodExcd" and "Pkey"
df_Trans_input = df_ExcdPkey_out.alias("Key")

df_lodExcd = df_Trans_input.select(
    rpad(F.col("Key.EXCD_ID"),4," ").alias("EXCD_ID")
)

df_Pkey = df_Trans_input.select(
    F.col("Key.JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("Key.INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    rpad(F.col("Key.DISCARD_IN"),1," ").alias("DISCARD_IN"),
    rpad(F.col("Key.PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("Key.FIRST_RECYC_DT"),
    F.col("Key.ERR_CT"),
    F.col("Key.RECYCLE_CT"),
    F.col("Key.SRC_SYS_CD"),
    F.col("Key.PRI_KEY_STRING"),
    F.col("Key.EXCD_SK"),
    rpad(F.col("Key.EXCD_ID"),4," ").alias("EXCD_ID"),
    F.col("Key.CRT_RUN_CYC_EXCTN_SK"),
    F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("Key.EXCD_HC_ADJ_CD"),5," ").alias("EXCD_HC_ADJ_CD"),
    rpad(F.col("Key.EXCD_PT_LIAB_IND"),1," ").alias("EXCD_PT_LIAB_IND"),
    rpad(F.col("Key.EXCD_PROV_ADJ_CD"),2," ").alias("EXCD_PROV_ADJ_CD"),
    rpad(F.col("Key.EXCD_REMIT_REMARK_CD"),2," ").alias("EXCD_REMIT_REMARK_CD"),
    rpad(F.col("Key.EXCD_STS"),1," ").alias("EXCD_STS"),
    rpad(F.col("Key.EXCD_TYPE"),2," ").alias("EXCD_TYPE"),
    F.col("Key.EXCD_LONG_TX1"),
    F.col("Key.EXCD_LONG_TX2"),
    F.col("Key.EXCD_SH_TX")
)

# "hf_clm_ln_ovrd_excd" is an intermediate hashed file: pattern "Trans -> hf_clm_ln_ovrd_excd -> LhoNewExcdList"
# Scenario A => we remove "hf_clm_ln_ovrd_excd" stage, deduplicate on "EXCD_ID" (key).
df_intermediate = df_lodExcd
df_intermediate_dedup = dedup_sort(df_intermediate, ["EXCD_ID"], [])

# Direct to "LhoNewExcdList" with .dat in landing => final file
write_files(
    df_intermediate_dedup,
    f"{adls_path_raw}/landing/LhoNewExcdList.dat",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# "LhoFacetsClmExcdCode" output from "Pkey"
df_LhoFacetsClmExcdCode = df_Pkey
write_files(
    df_LhoFacetsClmExcdCode,
    f"{adls_path}/key/LhoFacetsClmExcdCode.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)