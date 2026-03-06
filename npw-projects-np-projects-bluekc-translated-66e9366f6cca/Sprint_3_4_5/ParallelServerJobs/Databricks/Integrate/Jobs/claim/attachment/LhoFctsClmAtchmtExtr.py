# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmAtchmtExtr
# MAGIC CALLED BY:   LhoFctsClmExtr1Seq
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
# MAGIC Reddy Sanam        2020-10-12                                        Brought up to standards                                                             IntegrateDev2
# MAGIC Prabhu ES               2022-03-29       S2S                         MSSQL ODBC conn params added                                           IntegrateDev5	Ken Bradmon	2022-06-08

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
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmAtchmtPK
# COMMAND ----------

# Retrieve all parameters as specified
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')

# Per requirement: also create secret-name parameters for any "$<database>Owner"
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

#------------------------------------------------------------------------------
# Read from hashed-file stage hf_clm_fcts_reversals (Scenario C: direct source).
# Translated to reading parquet.
# Columns: CLCL_ID (char(12), PK), CLCL_CUR_STS (char(2)), CLCL_PAID_DT (timestamp),
#          CLCL_ID_ADJ_TO (char(12)), CLCL_ID_ADJ_FROM (char(12))
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

#------------------------------------------------------------------------------
# Read from hashed-file stage clm_nasco_dup_bypass (Scenario C: direct source).
# Translated to reading parquet.
# Columns: CLM_ID (varchar, PK)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

#------------------------------------------------------------------------------
# Read from ODBCConnector stage CMC_CLCL_CLAIM
# This stage has two output links => "Extract" and "ClmHospIn"

jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)

# 1) "Extract"
query_extract = (
    f"SELECT CLCL_ID, CLCL_EXT_REF_IND, CLCL_RAD_ENC_IND, CLCL_REC_ENC_IND, CLCL_COB_EOB_IND "
    f"FROM {LhoFacetsStgOwner}.CMC_CLCL_CLAIM A, tempdb..{DriverTable} TMP "
    f"WHERE TMP.CLM_ID = A.CLCL_ID"
)

df_CMC_CLCL_CLAIM_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_extract)
    .load()
)

# 2) "ClmHospIn"
query_clmhospin = (
    f"SELECT CLCL_ID, CLHP_MED_REC_NO "
    f"FROM {LhoFacetsStgOwner}.CMC_CLHP_HOSP HOSP, tempdb..{DriverTable} TMP "
    f"WHERE HOSP.CLCL_ID = TMP.CLM_ID"
)

df_CMC_CLCL_CLAIM_ClmHospIn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_clmhospin)
    .load()
)

#------------------------------------------------------------------------------
# Hashed-file stage hf_hosp_atchmt is an intermediate file (Scenario A).
# Pattern: CMC_CLCL_CLAIM_ClmHospIn -> hf_hosp_atchmt -> used as a lookup in StripField
# We remove the hashed-file stage and deduplicate on key=CLCL_ID.

df_CMC_CLCL_CLAIM_ClmHospInDedup = dedup_sort(
    df_CMC_CLCL_CLAIM_ClmHospIn,
    partition_cols=["CLCL_ID"],
    sort_cols=[]
)

#------------------------------------------------------------------------------
# Transformer stage "StripField"
# Primary link: df_CMC_CLCL_CLAIM_Extract as "Extract"
# Lookup link: df_CMC_CLCL_CLAIM_ClmHospInDedup as "lnkClmHosp", left join on CLCL_ID
# Constraint filter: ( CLCL_COB_EOB_IND='Y'
#                     OR CLCL_EXT_REF_IND='Y'
#                     OR CLCL_RAD_ENC_IND='Y'
#                     OR CLCL_REC_ENC_IND='Y'
#                     OR len(trim(lnkClmHosp.CLCL_ID))=0 )
# Output columns with expressions:

df_StripField_join = (
    df_CMC_CLCL_CLAIM_Extract.alias("Extract")
    .join(
        df_CMC_CLCL_CLAIM_ClmHospInDedup.alias("lnkClmHosp"),
        F.col("Extract.CLCL_ID") == F.col("lnkClmHosp.CLCL_ID"),
        "left"
    )
)

df_StripField_filtered = df_StripField_join.filter(
    (F.col("Extract.CLCL_COB_EOB_IND") == "Y")
    | (F.col("Extract.CLCL_EXT_REF_IND") == "Y")
    | (F.col("Extract.CLCL_RAD_ENC_IND") == "Y")
    | (F.col("Extract.CLCL_REC_ENC_IND") == "Y")
    | (F.length(trim(F.col("lnkClmHosp.CLCL_ID"))) == 0)
)

df_StripField = df_StripField_filtered.select(
    # CLCL_ID
    strip_field(F.col("Extract.CLCL_ID")).alias("CLCL_ID"),
    # EXT_TIMESTAMP => CurrentDate
    current_date().alias("EXT_TIMESTAMP"),
    # CLCL_EXT_REF_IND
    F.col("Extract.CLCL_EXT_REF_IND").alias("CLCL_EXT_REF_IND"),
    # CLCL_RAD_ENC_IND
    F.col("Extract.CLCL_RAD_ENC_IND").alias("CLCL_RAD_ENC_IND"),
    # CLCL_REC_ENC_IND
    F.col("Extract.CLCL_REC_ENC_IND").alias("CLCL_REC_ENC_IND"),
    # CLCL_COB_EOB_IND
    F.col("Extract.CLCL_COB_EOB_IND").alias("CLCL_COB_EOB_IND"),
    # CLHP_MED_REC_NO => if len(trim(lnkClmHosp.CLCL_ID))=0 then "NA" else strip_field(lnkClmHosp.CLHP_MED_REC_NO)
    F.when(
        F.length(trim(F.col("lnkClmHosp.CLCL_ID"))) == 0,
        F.lit("NA")
    ).otherwise(strip_field(F.col("lnkClmHosp.CLHP_MED_REC_NO"))).alias("CLHP_MED_REC_NO")
)

#------------------------------------------------------------------------------
# Transformer stage "BusinessRules"
# stage variables => ExternalReferralAttachmentType="EXTRNLRFRL", XRayAttachmentType="XRAY",
#                    MedicalDentalAttachmentType="MEDDNTL", OtherAttachmentType="OTHERCAREOB", ClmId=trim(Trans.CLCL_ID)
# input pins => left joins from df_hf_clm_fcts_reversals on CLCL_ID, df_clm_nasco_dup_bypass on CLM_ID
# primary link => df_StripField as "Trans"
# produce 8 output flows with constraints

df_BusinessRules_pre = (
    df_StripField.alias("Trans")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversal"),
        F.col("Trans.CLCL_ID") == F.col("fcts_reversal.CLCL_ID"),
        "left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("Trans.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
    .withColumn("ExternalReferralAttachmentType", F.lit("EXTRNLRFRL"))
    .withColumn("XRayAttachmentType", F.lit("XRAY"))
    .withColumn("MedicalDentalAttachmentType", F.lit("MEDDNTL"))
    .withColumn("OtherAttachmentType", F.lit("OTHERCAREOB"))
    .withColumn("ClmId", trim(F.col("Trans.CLCL_ID")))
)

# Now define each of the 8 flows (links) as separate DataFrames, then union them.

common_cols = [
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_ATCHMT_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ATCHMT_IND",
    "ATCHMT_REF_ID"
]

# 1) Ext_Ref
df_Ext_Ref = (
    df_BusinessRules_pre.filter(
        (F.col("Trans.CLCL_EXT_REF_IND") == "Y")
        & (F.col("nasco_dup_lkup.CLM_ID").isNull())
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("Trans.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("LUMERIS").alias("SRC_SYS_CD"),
        F.concat(F.lit("LUMERIS"), F.lit(";"), F.col("ClmId"), F.lit(";"), F.col("ExternalReferralAttachmentType")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_ATCHMT_SK"),
        F.col("ClmId").alias("CLM_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ExternalReferralAttachmentType").alias("ATCHMT_IND"),
        F.lit("NA").alias("ATCHMT_REF_ID")
    )
)

# 2) X_Ray
df_X_Ray = (
    df_BusinessRules_pre.filter(
        (F.col("Trans.CLCL_RAD_ENC_IND") == "Y")
        & (F.col("nasco_dup_lkup.CLM_ID").isNull())
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("Trans.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(F.lit("FACETS"), F.lit(";"), F.col("ClmId"), F.lit(";"), F.col("XRayAttachmentType")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_ATCHMT_SK"),
        F.col("ClmId").alias("CLM_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("XRayAttachmentType").alias("ATCHMT_IND"),
        F.lit("NA").alias("ATCHMT_REF_ID")
    )
)

# 3) Med_Den_Rec
df_Med_Den_Rec = (
    df_BusinessRules_pre.filter(
        ((F.col("Trans.CLCL_REC_ENC_IND") == "Y") | (F.col("Trans.CLHP_MED_REC_NO") != "NA"))
        & (F.col("nasco_dup_lkup.CLM_ID").isNull())
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("Trans.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(F.lit("FACETS"), F.lit(";"), F.col("ClmId"), F.lit(";"), F.col("MedicalDentalAttachmentType")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_ATCHMT_SK"),
        F.col("ClmId").alias("CLM_ID"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("MedicalDentalAttachmentType").alias("ATCHMT_IND"),
        F.when(
            (F.col("Trans.CLHP_MED_REC_NO").isNull()) | (F.length(trim(F.col("Trans.CLHP_MED_REC_NO"))) == 0),
            F.lit(" ")
        ).otherwise(trim(F.col("Trans.CLHP_MED_REC_NO"))).alias("ATCHMT_REF_ID")
    )
)

# 4) Oth_Car_EOB
df_Oth_Car_EOB = (
    df_BusinessRules_pre.filter(
        (F.col("Trans.CLCL_COB_EOB_IND") == "Y")
        & (F.col("nasco_dup_lkup.CLM_ID").isNull())
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("Trans.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(F.lit("FACETS"), F.lit(";"), F.col("ClmId"), F.lit(";"), F.col("OtherAttachmentType")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_ATCHMT_SK"),
        F.col("ClmId").alias("CLM_ID"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("OtherAttachmentType").alias("ATCHMT_IND"),
        F.lit("NA").alias("ATCHMT_REF_ID")
    )
)

# 5) Ext_Ref_Reversal
df_Ext_Ref_Reversal = (
    df_BusinessRules_pre.filter(
        (F.col("Trans.CLCL_EXT_REF_IND") == "Y")
        & (F.col("fcts_reversal.CLCL_ID").isNotNull())
        & (
            (F.col("fcts_reversal.CLCL_CUR_STS") == "89")
            | (F.col("fcts_reversal.CLCL_CUR_STS") == "91")
            | (F.col("fcts_reversal.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("Trans.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(F.lit("FACETS"), F.lit(";"), F.col("ClmId"), F.lit("R;"), F.col("ExternalReferralAttachmentType")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_ATCHMT_SK"),
        F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ExternalReferralAttachmentType").alias("ATCHMT_IND"),
        F.lit("NA").alias("ATCHMT_REF_ID")
    )
)

# 6) X_Ray_Reversal
df_X_Ray_Reversal = (
    df_BusinessRules_pre.filter(
        (F.col("Trans.CLCL_RAD_ENC_IND") == "Y")
        & (F.col("fcts_reversal.CLCL_ID").isNotNull())
        & (
            (F.col("fcts_reversal.CLCL_CUR_STS") == "89")
            | (F.col("fcts_reversal.CLCL_CUR_STS") == "91")
            | (F.col("fcts_reversal.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("Trans.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(F.lit("FACETS"), F.lit(";"), F.col("ClmId"), F.lit("R;"), F.col("XRayAttachmentType")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_ATCHMT_SK"),
        F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("XRayAttachmentType").alias("ATCHMT_IND"),
        F.lit("NA").alias("ATCHMT_REF_ID")
    )
)

# 7) Med_Den_Reversal
df_Med_Den_Reversal = (
    df_BusinessRules_pre.filter(
        ((F.col("Trans.CLCL_REC_ENC_IND") == "Y") | (F.col("Trans.CLHP_MED_REC_NO") != "NA"))
        & (F.col("fcts_reversal.CLCL_ID").isNotNull())
        & (
            (F.col("fcts_reversal.CLCL_CUR_STS") == "89")
            | (F.col("fcts_reversal.CLCL_CUR_STS") == "91")
            | (F.col("fcts_reversal.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("Trans.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(F.lit("FACETS"), F.lit(";"), F.col("ClmId"), F.lit("R;"), F.col("MedicalDentalAttachmentType")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_ATCHMT_SK"),
        F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("MedicalDentalAttachmentType").alias("ATCHMT_IND"),
        F.when(
            (F.col("Trans.CLHP_MED_REC_NO").isNull()) | (F.length(trim(F.col("Trans.CLHP_MED_REC_NO"))) == 0),
            F.lit(" ")
        ).otherwise(trim(F.col("Trans.CLHP_MED_REC_NO"))).alias("ATCHMT_REF_ID")
    )
)

# 8) Oth_Car_Reversal
df_Oth_Car_Reversal = (
    df_BusinessRules_pre.filter(
        (F.col("Trans.CLCL_COB_EOB_IND") == "Y")
        & (F.col("fcts_reversal.CLCL_ID").isNotNull())
        & (
            (F.col("fcts_reversal.CLCL_CUR_STS") == "89")
            | (F.col("fcts_reversal.CLCL_CUR_STS") == "91")
            | (F.col("fcts_reversal.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("Trans.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(F.lit("FACETS"), F.lit(";"), F.col("ClmId"), F.lit("R;"), F.col("OtherAttachmentType")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_ATCHMT_SK"),
        F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("OtherAttachmentType").alias("ATCHMT_IND"),
        F.lit("NA").alias("ATCHMT_REF_ID")
    )
)

#------------------------------------------------------------------------------
# Merge_Streams => CCollector (Round-Robin)
# Collect the 8 flows into one DataFrame:

df_merge_streams = (
    df_Ext_Ref
    .unionByName(df_X_Ray)
    .unionByName(df_Med_Den_Rec)
    .unionByName(df_Oth_Car_EOB)
    .unionByName(df_Ext_Ref_Reversal)
    .unionByName(df_X_Ray_Reversal)
    .unionByName(df_Med_Den_Reversal)
    .unionByName(df_Oth_Car_Reversal)
)

#------------------------------------------------------------------------------
# Transformer stage "Snapshot"
# Input: df_merge_streams => output pins: "Allcol", "Snapshot", "Transform"

# 1) "Allcol" => columns per design
df_Snapshot_Allcol = df_merge_streams.select(
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

# 2) "Snapshot" => columns "CLCL_ID", "ATCHMT_IND"
df_Snapshot_Snapshot = df_merge_streams.select(
    F.col("CLM_ID").alias("CLCL_ID"),
    F.col("ATCHMT_IND").alias("ATCHMT_IND")
)

# 3) "Transform" => columns "SRC_SYS_CD_SK", "CLM_ID", "CLM_ATCHMT_TYP_CD"
df_Snapshot_Transform = df_merge_streams.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("ATCHMT_IND").alias("CLM_ATCHMT_TYP_CD")
)

#------------------------------------------------------------------------------
# Transformer stage "Transformer"
# Input pin => df_Snapshot_Snapshot
# Stage variable => ClmAtchmtTypCdSk = GetFkeyCodes('LUMERIS', 0, "CLAIM ATTACHMENT TYPE", Snapshot.ATCHMT_IND, 'X')
# Output => "RowCount" => B_CLM_ATCHMT

df_Transformer = df_Snapshot_Snapshot.withColumn(
    "ClmAtchmtTypCdSk",
    GetFkeyCodes("LUMERIS", 0, "CLAIM ATTACHMENT TYPE", F.col("ATCHMT_IND"), "X")
)

df_B_CLM_ATCHMT = df_Transformer.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("ClmAtchmtTypCdSk").alias("CLM_ATCHMT_TYP_CD_SK")
)

# Write to CSeqFileStage => "B_CLM_ATCHMT.LhoFACETS.dat.#RunID#"
write_files(
    df_B_CLM_ATCHMT,
    f"{adls_path}/load/B_CLM_ATCHMT.LhoFACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

#------------------------------------------------------------------------------
# Container stage "ClmAtchmtPK"
# Input pins => df_Snapshot_Allcol (Allcol), df_Snapshot_Transform (Transform)
# Output => single link => "Key"

params_ClmAtchmtPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrentDate,
    "IDSOwner": IDSOwner
}

df_ClmAtchmtPK_Key = ClmAtchmtPK(df_Snapshot_Allcol, df_Snapshot_Transform, params_ClmAtchmtPK)

#------------------------------------------------------------------------------
# Final stage "IdsClmAtchmtPkey" => CSeqFileStage
# We must preserve column order and apply rpad to char columns

df_IdsClmAtchmtPkey = df_ClmAtchmtPK_Key.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_ATCHMT_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ATCHMT_IND",
    "ATCHMT_REF_ID"
)

# rpad on char columns where length is specified
df_IdsClmAtchmtPkey_padded = (
    df_IdsClmAtchmtPkey
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("ATCHMT_IND", F.rpad(F.col("ATCHMT_IND"), 35, " "))
)

write_files(
    df_IdsClmAtchmtPkey_padded,
    f"{adls_path}/key/LhoFctsClmAtchmtExtr.LhoFctsClmAtchmt.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)