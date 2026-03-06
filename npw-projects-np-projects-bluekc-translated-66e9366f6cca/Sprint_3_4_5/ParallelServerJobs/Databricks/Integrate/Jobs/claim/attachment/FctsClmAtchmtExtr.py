# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmAtchmtExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CLCL_CLAIM and CMC_CLHP_HOSP to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	CMC_CLCL_CLAIM
# MAGIC                 CMC_CLHP_HOSP
# MAGIC                 Joined with 
# MAGIC                 tempdb..TMP_IDS_CLAIM  to get claims updated within time period
# MAGIC   
# MAGIC HASH FILES:  hf_hosp_atchmt,
# MAGIC                         hf_clm_nasco_dup_bypass
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   STRIP.FIELD
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name.  Job control renames to final name after job completes.
# MAGIC                  All claims need to be processed and not filtered out.   Because, there may be rows from the CMC_CLCL_CLAIM that do not have a Y in any of the four fields but will still match up to a record on the hf_hosp_atchmt.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Steph Goddard         04/01/2004-   Originally Programmed
# MAGIC Sharon Andrew         11/01/2004  - Facets claim reversal.  Added logic to test if claim status is '91'.  If so, write out record with ClaimId and an R on the end.   If there are any amount fields, make them negative.
# MAGIC Steph Goddard         02/03/2006     Changed for sequencer
# MAGIC BJ Luce                     03/20/2006     add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC Sanderw                   12/08/2006    Project 1576  - Reversal logic added for new status codes 89 and  and 99
# MAGIC                                                                  changed Trans.CLCL_EXT_REF_IND = "Y" and (IsNull (  fcts_reversal.CLCL_ID ) = @FALSE) and  fcts_reversal.CLCL_CUR_STS = '91'
# MAGIC Oliver Nielsen            08/15/2007     Balancing                 Added Snapshot File extract                                                           devlIDS30                  Steph Goddard           8/30/07
# MAGIC 
# MAGIC Bhoomi Dasari         2008-07-11      3567(Primary Key)    Changed primary key process from hash file to DB2 table            devlIDS                            Steph Goddard          07/25/2008
# MAGIC 
# MAGIC Prabhu ES               2022-02-28      S2S Remediation     MSSQL connection parameters added                                        IntegrateDev5                   Kalyan Neelam          2022-06-10

# MAGIC Hash file (hf_clm_atchmt_allcol) cleared from the container - ClmAtchmtPK
# MAGIC Writing Sequential File to /key
# MAGIC This container is used in:
# MAGIC FctsClmAtchmtExtr
# MAGIC NascoClmAtchmtExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Apply business logic
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Extract Facets Claim Diagnosis Data
# MAGIC Assign primary surrogate key
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
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmAtchmtPK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable', '')
CurrRunCycle = get_widget_value('CurrRunCycle', '')
FacetsOwner = get_widget_value('FacetsOwner', '')
facets_secret_name = get_widget_value('facets_secret_name', '')
RunID = get_widget_value('RunID', '')
CurrentDate = get_widget_value('CurrentDate', '')
SrcSysCdSk = get_widget_value('SrcSysCdSk', '')
SrcSysCd = get_widget_value('SrcSysCd', '')
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

# 1) Read Hashed File: hf_clm_fcts_reversals (Scenario C)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet").select(
    "CLCL_ID",
    "CLCL_CUR_STS",
    "CLCL_PAID_DT",
    "CLCL_ID_ADJ_TO",
    "CLCL_ID_ADJ_FROM"
)

# 2) Read Hashed File: clm_nasco_dup_bypass (Scenario C)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet").select(
    "CLM_ID"
)

# 3) ODBCConnector: CMC_CLCL_CLAIM => Output Link: Extract
extract_query_1 = f"SELECT CLCL_ID,CLCL_EXT_REF_IND,CLCL_RAD_ENC_IND,CLCL_REC_ENC_IND,CLCL_COB_EOB_IND FROM {FacetsOwner}.CMC_CLCL_CLAIM A, tempdb..{DriverTable} TMP WHERE TMP.CLM_ID = A.CLCL_ID"
df_CMC_CLCL_CLAIM_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

# 4) ODBCConnector: CMC_CLCL_CLAIM => Output Link: ClmHospIn
extract_query_2 = f"SELECT CLCL_ID,CLHP_MED_REC_NO FROM {FacetsOwner}.CMC_CLHP_HOSP HOSP, tempdb..{DriverTable} TMP WHERE HOSP.CLCL_ID = TMP.CLM_ID"
df_CMC_CLCL_CLAIM_ClmHospIn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

# 5) hf_hosp_atchmt: Intermediate hashed file (Scenario A) => deduplicate on key CLCL_ID
df_lnkClmHosp = df_CMC_CLCL_CLAIM_ClmHospIn.drop_duplicates(["CLCL_ID"])

# 6) StripField: left join with df_lnkClmHosp, filter by constraint, produce df_Trans
df_StripField_join = df_CMC_CLCL_CLAIM_Extract.alias("Extract").join(
    df_lnkClmHosp.alias("lnkClmHosp"),
    F.col("Extract.CLCL_ID") == F.col("lnkClmHosp.CLCL_ID"),
    "left"
)

df_StripField_filtered = df_StripField_join.filter(
    (F.col("Extract.CLCL_COB_EOB_IND") == "Y")
    | (F.col("Extract.CLCL_EXT_REF_IND") == "Y")
    | (F.col("Extract.CLCL_RAD_ENC_IND") == "Y")
    | (F.col("Extract.CLCL_REC_ENC_IND") == "Y")
    | (F.length(trim(F.col("lnkClmHosp.CLCL_ID"))) == 0)
)

df_Trans = df_StripField_filtered.select(
    strip_field(F.col("Extract.CLCL_ID")).alias("CLCL_ID"),
    current_date().alias("EXT_TIMESTAMP"),
    F.col("Extract.CLCL_EXT_REF_IND").alias("CLCL_EXT_REF_IND"),
    F.col("Extract.CLCL_RAD_ENC_IND").alias("CLCL_RAD_ENC_IND"),
    F.col("Extract.CLCL_REC_ENC_IND").alias("CLCL_REC_ENC_IND"),
    F.col("Extract.CLCL_COB_EOB_IND").alias("CLCL_COB_EOB_IND"),
    F.when(
        F.length(trim(F.col("lnkClmHosp.CLCL_ID"))) == 0,
        F.lit("NA")
    ).otherwise(
        strip_field(F.col("lnkClmHosp.CLHP_MED_REC_NO"))
    ).alias("CLHP_MED_REC_NO")
)

# 7) BusinessRules: left join with df_hf_clm_fcts_reversals and df_clm_nasco_dup_bypass
df_BusinessRules_join_1 = df_Trans.alias("Trans").join(
    df_hf_clm_fcts_reversals.alias("fcts_reversal"),
    F.col("Trans.CLCL_ID") == F.col("fcts_reversal.CLCL_ID"),
    "left"
)

df_BusinessRules_join_2 = df_BusinessRules_join_1.alias("tj").join(
    df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
    F.col("tj.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
    "left"
)

# In DataStage, we have Stage Variables:
#   ExternalReferralAttachmentType = "EXTRNLRFRL"
#   XRayAttachmentType = "XRAY"
#   MedicalDentalAttachmentType = "MEDDNTL"
#   OtherAttachmentType = "OTHERCAREOB"
#   ClmId = trim(Trans.CLCL_ID)
# We replicate them in-line below.

# Create each output link DataFrame from BusinessRules:

# Ext_Ref
df_Ext_Ref = df_BusinessRules_join_2.filter(
    (F.col("Trans.CLCL_EXT_REF_IND") == "Y")
    & (F.col("nasco_dup_lkup.CLM_ID").isNull())
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("Trans.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (
        F.lit("FACETS")
        .concat(F.lit(";"))
        .concat(trim(F.col("Trans.CLCL_ID")))
        .concat(F.lit(";"))
        .concat(F.lit("EXTRNLRFRL"))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_ATCHMT_SK"),
    trim(F.col("Trans.CLCL_ID")).alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("EXTRNLRFRL").alias("ATCHMT_IND"),
    F.lit("NA").alias("ATCHMT_REF_ID")
)

# X_Ray
df_X_Ray = df_BusinessRules_join_2.filter(
    (F.col("Trans.CLCL_RAD_ENC_IND") == "Y")
    & (F.col("nasco_dup_lkup.CLM_ID").isNull())
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("Trans.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (
        F.lit("FACETS")
        .concat(F.lit(";"))
        .concat(trim(F.col("Trans.CLCL_ID")))
        .concat(F.lit(";"))
        .concat(F.lit("XRAY"))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_ATCHMT_SK"),
    trim(F.col("Trans.CLCL_ID")).alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("XRAY").alias("ATCHMT_IND"),
    F.lit("NA").alias("ATCHMT_REF_ID")
)

# Med_Den_Rec
df_Med_Den_Rec = df_BusinessRules_join_2.filter(
    ((F.col("Trans.CLCL_REC_ENC_IND") == "Y") | (F.col("Trans.CLHP_MED_REC_NO") != "NA"))
    & (F.col("nasco_dup_lkup.CLM_ID").isNull())
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("Trans.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (
        F.lit("FACETS")
        .concat(F.lit(";"))
        .concat(trim(F.col("Trans.CLCL_ID")))
        .concat(F.lit(";"))
        .concat(F.lit("MEDDNTL"))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_ATCHMT_SK"),
    trim(F.col("Trans.CLCL_ID")).alias("CLM_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("MEDDNTL").alias("ATCHMT_IND"),
    F.when(
        F.col("Trans.CLHP_MED_REC_NO").isNull() | (F.length(trim(F.col("Trans.CLHP_MED_REC_NO"))) == 0),
        F.lit(" ")
    ).otherwise(
        trim(F.col("Trans.CLHP_MED_REC_NO"))
    ).alias("ATCHMT_REF_ID")
)

# Oth_Car_EOB
df_Oth_Car_EOB = df_BusinessRules_join_2.filter(
    (F.col("Trans.CLCL_COB_EOB_IND") == "Y")
    & (F.col("nasco_dup_lkup.CLM_ID").isNull())
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("Trans.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (
        F.lit("FACETS")
        .concat(F.lit(";"))
        .concat(trim(F.col("Trans.CLCL_ID")))
        .concat(F.lit(";"))
        .concat(F.lit("OTHERCAREOB"))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_ATCHMT_SK"),
    trim(F.col("Trans.CLCL_ID")).alias("CLM_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("OTHERCAREOB").alias("ATCHMT_IND"),
    F.lit("NA").alias("ATCHMT_REF_ID")
)

# Ext_Ref_Reversal
df_Ext_Ref_Reversal = df_BusinessRules_join_2.filter(
    (F.col("Trans.CLCL_EXT_REF_IND") == "Y")
    & (F.col("fcts_reversal.CLCL_ID").isNotNull())
    & (
        (F.col("fcts_reversal.CLCL_CUR_STS") == "89")
        | (F.col("fcts_reversal.CLCL_CUR_STS") == "91")
        | (F.col("fcts_reversal.CLCL_CUR_STS") == "99")
    )
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("Trans.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (
        F.lit("FACETS")
        .concat(F.lit(";"))
        .concat(trim(F.col("Trans.CLCL_ID")))
        .concat(F.lit("R;"))
        .concat(F.lit("EXTRNLRFRL"))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_ATCHMT_SK"),
    trim(F.col("Trans.CLCL_ID")).cast(StringType()).alias("CLM_ID_TEMP"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("EXTRNLRFRL").alias("ATCHMT_IND"),
    F.lit("NA").alias("ATCHMT_REF_ID")
)
# Adjust CLM_ID => ClmId + "R"
df_Ext_Ref_Reversal = df_Ext_Ref_Reversal.withColumn("CLM_ID", F.concat(F.col("CLM_ID_TEMP"), F.lit("R"))).drop("CLM_ID_TEMP")

# X_Ray_Reversal
df_X_Ray_Reversal = df_BusinessRules_join_2.filter(
    (F.col("Trans.CLCL_RAD_ENC_IND") == "Y")
    & (F.col("fcts_reversal.CLCL_ID").isNotNull())
    & (
        (F.col("fcts_reversal.CLCL_CUR_STS") == "89")
        | (F.col("fcts_reversal.CLCL_CUR_STS") == "91")
        | (F.col("fcts_reversal.CLCL_CUR_STS") == "99")
    )
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("Trans.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (
        F.lit("FACETS")
        .concat(F.lit(";"))
        .concat(trim(F.col("Trans.CLCL_ID")))
        .concat(F.lit("R;"))
        .concat(F.lit("XRAY"))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_ATCHMT_SK"),
    trim(F.col("Trans.CLCL_ID")).cast(StringType()).alias("CLM_ID_TEMP"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("XRAY").alias("ATCHMT_IND"),
    F.lit("NA").alias("ATCHMT_REF_ID")
)
df_X_Ray_Reversal = df_X_Ray_Reversal.withColumn("CLM_ID", F.concat(F.col("CLM_ID_TEMP"), F.lit("R"))).drop("CLM_ID_TEMP")

# Med_Den_Reversal
df_Med_Den_Reversal = df_BusinessRules_join_2.filter(
    ((F.col("Trans.CLCL_REC_ENC_IND") == "Y") | (F.col("Trans.CLHP_MED_REC_NO") != "NA"))
    & (F.col("fcts_reversal.CLCL_ID").isNotNull())
    & (
        (F.col("fcts_reversal.CLCL_CUR_STS") == "89")
        | (F.col("fcts_reversal.CLCL_CUR_STS") == "91")
        | (F.col("fcts_reversal.CLCL_CUR_STS") == "99")
    )
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("Trans.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (
        F.lit("FACETS")
        .concat(F.lit(";"))
        .concat(trim(F.col("Trans.CLCL_ID")))
        .concat(F.lit("R;"))
        .concat(F.lit("MEDDNTL"))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_ATCHMT_SK"),
    trim(F.col("Trans.CLCL_ID")).cast(StringType()).alias("CLM_ID_TEMP"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("MEDDNTL").alias("ATCHMT_IND"),
    F.when(
        F.col("Trans.CLHP_MED_REC_NO").isNull() | (F.length(trim(F.col("Trans.CLHP_MED_REC_NO"))) == 0),
        F.lit(" ")
    ).otherwise(
        trim(F.col("Trans.CLHP_MED_REC_NO"))
    ).alias("ATCHMT_REF_ID")
)
df_Med_Den_Reversal = df_Med_Den_Reversal.withColumn("CLM_ID", F.concat(F.col("CLM_ID_TEMP"), F.lit("R"))).drop("CLM_ID_TEMP")

# Oth_Car_Reversal
df_Oth_Car_Reversal = df_BusinessRules_join_2.filter(
    (F.col("Trans.CLCL_COB_EOB_IND") == "Y")
    & (F.col("fcts_reversal.CLCL_ID").isNotNull())
    & (
        (F.col("fcts_reversal.CLCL_CUR_STS") == "89")
        | (F.col("fcts_reversal.CLCL_CUR_STS") == "91")
        | (F.col("fcts_reversal.CLCL_CUR_STS") == "99")
    )
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("Trans.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (
        F.lit("FACETS")
        .concat(F.lit(";"))
        .concat(trim(F.col("Trans.CLCL_ID")))
        .concat(F.lit("R;"))
        .concat(F.lit("OTHERCAREOB"))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_ATCHMT_SK"),
    trim(F.col("Trans.CLCL_ID")).cast(StringType()).alias("CLM_ID_TEMP"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("OTHERCAREOB").alias("ATCHMT_IND"),
    F.lit("NA").alias("ATCHMT_REF_ID")
)
df_Oth_Car_Reversal = df_Oth_Car_Reversal.withColumn("CLM_ID", F.concat(F.col("CLM_ID_TEMP"), F.lit("R"))).drop("CLM_ID_TEMP")

# 8) Merge_Streams (CCollector): union all rows
df_Merge_Streams = (
    df_Ext_Ref
    .unionByName(df_X_Ray)
    .unionByName(df_Med_Den_Rec)
    .unionByName(df_Oth_Car_EOB)
    .unionByName(df_Ext_Ref_Reversal)
    .unionByName(df_X_Ray_Reversal)
    .unionByName(df_Med_Den_Reversal)
    .unionByName(df_Oth_Car_Reversal)
)

# 9) Snapshot stage: single input => multiple outputs
#    Output "Allcol"
df_SnapshotAllcol = df_Merge_Streams.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("ATCHMT_IND").alias("CLM_ATCHMT_TYP_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_ATCHMT_SK").alias("CLM_ATCHMT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ATCHMT_REF_ID").alias("ATCHMT_REF_ID")
)

#    Output "Snapshot"
df_SnapshotSnapshot = df_Merge_Streams.select(
    F.col("CLM_ID").alias("CLCL_ID"),
    F.col("ATCHMT_IND").alias("ATCHMT_IND")
)

#    Output "Transform"
df_SnapshotTransform = df_Merge_Streams.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("ATCHMT_IND").alias("CLM_ATCHMT_TYP_CD")
)

# 10) Another Transformer stage named "Transformer", input is df_SnapshotSnapshot => output "RowCount"
#     Stage variable: ClmAtchmtTypCdSk = GetFkeyCodes('FACETS', 0, "CLAIM ATTACHMENT TYPE", Snapshot.ATCHMT_IND, 'X')
#     We treat "GetFkeyCodes" as a user-defined function. 
df_Transformer_in = df_SnapshotSnapshot
df_Transformer = df_Transformer_in.withColumn(
    "CLM_ATCHMT_TYP_CD_SK", 
    GetFkeyCodes(
        F.lit("FACETS"), 
        F.lit(0), 
        F.lit("CLAIM ATTACHMENT TYPE"), 
        F.col("ATCHMT_IND"), 
        F.lit("X")
    )
).select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("CLM_ATCHMT_TYP_CD_SK").alias("CLM_ATCHMT_TYP_CD_SK")
)

# 11) B_CLM_ATCHMT => write to .dat file
b_clm_atchmt_path = f"{adls_path}/load/B_CLM_ATCHMT.FACETS.dat.{RunID}"
write_files(
    df_Transformer.select("SRC_SYS_CD_SK", "CLM_ID", "CLM_ATCHMT_TYP_CD_SK"),
    b_clm_atchmt_path,
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# 12) ClmAtchmtPK (Shared Container). Two inputs from Snapshot: "Allcol" and "Transform". One output "Key".
params_ClmAtchmtPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrentDate,
    "IDSOwner": IDSOwner
}
df_ClmAtchmtPK_out = ClmAtchmtPK(df_SnapshotAllcol, df_SnapshotTransform, params_ClmAtchmtPK)

# 13) IdsClmAtchmtPkey => write to .dat file
ids_clm_atchmt_pkey_path = f"{adls_path}/key/FctsClmAtchmtExtr.FctsClmAtchmt.dat.{RunID}"

# For final columns that have SqlType=char, apply rpad for the indicated length:
df_IdsClmAtchmtPkey_rpad = df_ClmAtchmtPK_out.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_ATCHMT_SK").alias("CLM_ATCHMT_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("ATCHMT_IND"), 35, " ").alias("ATCHMT_IND"),
    F.col("ATCHMT_REF_ID").alias("ATCHMT_REF_ID")
)

write_files(
    df_IdsClmAtchmtPkey_rpad,
    ids_clm_atchmt_pkey_path,
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)