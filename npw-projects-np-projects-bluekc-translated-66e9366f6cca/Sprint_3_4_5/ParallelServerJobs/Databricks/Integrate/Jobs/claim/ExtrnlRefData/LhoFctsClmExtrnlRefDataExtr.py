# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     LhoFctsClmExtrnRefDataExtr
# MAGIC Calling Job: 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CLED_EDI_DATA to CLM_EXTRNL_REF_DATA for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	CMC_CLED_EDI_DATA
# MAGIC                 Joined with 
# MAGIC                 tempdb..TMP_IDS_CLAIM  to get claims updated within time period
# MAGIC   
# MAGIC HASH FILES:       hf_clm_fcts_reversals              -   For Reversals
# MAGIC                             hf_clm_nasco_dup_bypass      -  For duplication check
# MAGIC                             hf_clm_exxtrnl_ref_data 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   STRIP.FIELD
# MAGIC                   FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file is created
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC          
# MAGIC                 Parikshith Chada  09/24/2006   -Originally Programmed
# MAGIC                 Ralph Tucker       12/1/2006    - Changed to use hf_clm in the Primary Key step.
# MAGIC                 Sanderw  12/08/2006   Project 1756  - Reversal logix added for new status codes 89 and  and 99
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC 
# MAGIC 
# MAGIC Reddy Sanam        2020-08-14      263440                  Copied from Facets jobs 
# MAGIC                                                                                     Added LhoFacets Env variables and removed Facets
# MAGIC                                                                                      Env Variables
# MAGIC                                                                                      Added new Job parameter SrcSysCd
# MAGIC                                                                                      Replace the hard coded value "FACETS" in the column 
# MAGIC                                                                                      derivations of the transformer "BusinessRules"
# MAGIC                                                                                      Replaced target sequential file name to include LhoFcts
# MAGIC Venkatesh Babu 2020-10-12                                       changed Buffersize and Timeout to 512kb and 300sec.
# MAGIC 
# MAGIC Prabhu ES          2022-03-29            S2S                      MSSQL ODBC conn params added                                               IntegrateDev5                   Manasa Andru            2022-06-09

# MAGIC Assign primary surrogate key
# MAGIC Apply business logic
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Extract Facets Claim External Reference Data
# MAGIC Writing Sequential File to ../key
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmExtrnlRefPK
# COMMAND ----------

# PARAMETERS
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
LhoFacetsStgOwner = get_widget_value('$LhoFacetsStgOwner','')
lhofacets_secret_name = get_widget_value('lhofacets_secret_name','')
LhoFacetsStgAcct = get_widget_value('$LhoFacetsStgAcct','')
LhoFacetsStgPW = get_widget_value('$LhoFacetsStgPW','')
SrcSysCd = get_widget_value('SrcSysCd','')
LhoFacetsStgDSN = get_widget_value('$LhoFacetsStgDSN','')

# READ CHashedFileStage hf_clm_fcts_reversals (Scenario C)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# READ CHashedFileStage clm_nasco_dup_bypass (Scenario C)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# READ ODBCConnector CMC_CLED_EDI_DATA
jdbc_url, jdbc_props = get_db_config(lhofacets_secret_name)
extract_query = f"SELECT CLCL_ID,CLED_EXT_REF,CLED_TRAD_PARTNER FROM {LhoFacetsStgOwner}.CMC_CLED_EDI_DATA A, tempdb..#{DriverTable} TMP WHERE TMP.CLM_ID = A.CLCL_ID"
df_CMC_CLED_EDI_DATA = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# TRANSFORMER StripField
df_stripfield = (
    df_CMC_CLED_EDI_DATA
    .withColumn("CLCL_ID", strip_field(F.col("CLCL_ID")))
    .withColumn("CLED_EXT_REF", strip_field(F.col("CLED_EXT_REF")))
    .withColumn("CLED_TRAD_PARTNER", strip_field(F.col("CLED_TRAD_PARTNER")))
)

# TRANSFORMER BusinessRules - join lookup links
df_businessrules_joined = (
    df_stripfield.alias("Strip")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
        "left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
    .withColumn("claimId", trim(F.col("Strip.CLCL_ID")))
)

# OUTPUT LINK Trans1: IsNull(nasco_dup_lkup.CLM_ID) = @TRUE
df_Trans1 = df_businessrules_joined.filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
df_Trans1 = df_Trans1.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("claimId")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_SK"),
    F.col("claimId").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN"),
    F.when(
        (F.col("Strip.CLED_TRAD_PARTNER") != "EMPOWERED BNFTS")
        | (F.col("Strip.CLED_TRAD_PARTNER") != "PCA RUNOUT")
        | (F.col("Strip.CLED_TRAD_PARTNER") != "HRA DRUG CLAIM"),
        F.col("Strip.CLED_EXT_REF")
    ).otherwise(F.lit("NA")).alias("EXTRNL_REF_ID"),
    F.when(
        (F.col("Strip.CLED_TRAD_PARTNER") == "EMPOWERED BNFTS")
        | (F.col("Strip.CLED_TRAD_PARTNER") == "PCA RUNOUT")
        | (F.col("Strip.CLED_TRAD_PARTNER") == "HRA DRUG CLAIM"),
        F.col("Strip.CLED_EXT_REF")
    ).otherwise(F.lit("NA")).alias("PCA_EXTRNL_ID"),
    F.when(
        (F.col("Strip.CLED_TRAD_PARTNER") == "EMPOWERED BNFTS")
        | (F.col("Strip.CLED_TRAD_PARTNER") == "PCA RUNOUT")
        | (F.col("Strip.CLED_TRAD_PARTNER") == "HRA DRUG CLAIM"),
        F.col("Strip.CLED_TRAD_PARTNER")
    ).otherwise(F.lit("NA")).alias("PCA_SRC_NM"),
    F.when(
        (F.col("Strip.CLED_TRAD_PARTNER") != "EMPOWERED BNFTS")
        | (F.col("Strip.CLED_TRAD_PARTNER") != "PCA RUNOUT")
        | (F.col("Strip.CLED_TRAD_PARTNER") != "HRA DRUG CLAIM"),
        F.col("Strip.CLED_TRAD_PARTNER")
    ).otherwise(F.lit("NA")).alias("TRDNG_PRTNR_NM")
)

# OUTPUT LINK reversals: IsNull(fcts_reversals.CLCL_ID) = @FALSE AND (fcts_reversals.CLCL_CUR_STS in 89,91,99)
df_reversals = df_businessrules_joined.filter(
    (~F.col("fcts_reversals.CLCL_ID").isNull())
    & (
        (F.col("fcts_reversals.CLCL_CUR_STS") == "89")
        | (F.col("fcts_reversals.CLCL_CUR_STS") == "91")
        | (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
    )
)
df_reversals = df_reversals.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("claimId"), F.lit("R")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_SK"),
    F.concat(F.col("claimId"), F.lit("R")).alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN"),
    F.when(
        (F.col("Strip.CLED_TRAD_PARTNER") != "EMPOWERED BNFTS")
        | (F.col("Strip.CLED_TRAD_PARTNER") != "PCA RUNOUT")
        | (F.col("Strip.CLED_TRAD_PARTNER") != "HRA DRUG CLAIM"),
        F.col("Strip.CLED_EXT_REF")
    ).otherwise(F.lit("NA")).alias("EXTRNL_REF_ID"),
    F.when(
        (F.col("Strip.CLED_TRAD_PARTNER") == "EMPOWERED BNFTS")
        | (F.col("Strip.CLED_TRAD_PARTNER") == "PCA RUNOUT"),
        F.col("Strip.CLED_EXT_REF")
    ).otherwise(F.lit("NA")).alias("PCA_EXTRNL_ID"),
    F.when(
        (F.col("Strip.CLED_TRAD_PARTNER") == "EMPOWERED BNFTS")
        | (F.col("Strip.CLED_TRAD_PARTNER") == "PCA RUNOUT")
        | (F.col("Strip.CLED_TRAD_PARTNER") == "HRA DRUG CLAIM"),
        F.col("Strip.CLED_TRAD_PARTNER")
    ).otherwise(F.lit("NA")).alias("PCA_SRC_NM"),
    F.when(
        (F.col("Strip.CLED_TRAD_PARTNER") != "EMPOWERED BNFTS")
        | (F.col("Strip.CLED_TRAD_PARTNER") != "PCA RUNOUT")
        | (F.col("Strip.CLED_TRAD_PARTNER") != "HRA DRUG CLAIM"),
        F.col("Strip.CLED_TRAD_PARTNER")
    ).otherwise(F.lit("NA")).alias("TRDNG_PRTNR_NM")
)

# COLLECTOR - union
df_collector = df_Trans1.unionByName(df_reversals)
collector_cols = [
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN",
    "LAST_UPDT_RUN_CYC_EXCTN",
    "EXTRNL_REF_ID",
    "PCA_EXTRNL_ID",
    "PCA_SRC_NM",
    "TRDNG_PRTNR_NM"
]
df_collector = df_collector.select(*collector_cols)

# SHARED CONTAINER ClmExtrnlRefPK
params = {
    "CurrRunCycle": CurrRunCycle
}
df_clmextrnlrefpk_out = ClmExtrnlRefPK(df_collector, params)

# WRITE CSeqFileStage IdsClmExtrnlRef
df_final = df_clmextrnlrefpk_out.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_SK"),
    F.rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("EXTRNL_REF_ID"), 50, " ").alias("EXTRNL_REF_ID"),
    F.rpad(F.col("PCA_EXTRNL_ID"), 50, " ").alias("PCA_EXTRNL_ID"),
    F.rpad(F.col("PCA_SRC_NM"), 15, " ").alias("PCA_SRC_NM"),
    F.rpad(F.col("TRDNG_PRTNR_NM"), 15, " ").alias("TRDNG_PRTNR_NM")
)

write_files(
    df_final,
    f"{adls_path}/key/LhoFctsClmExtrnlRefDataExtr.LhoFctsClmExtrnlRefData.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)