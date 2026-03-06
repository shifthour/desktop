# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmExtrnRefDataExtr
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
# MAGIC Bhoomi Dasari         2008-08-05      3567(Primary Key)    Changed primary key process from hash file to DB2 table            devlIDS                         Steph Goddard          08/20/2008
# MAGIC Kalyan Neelam        2010-02-19      4278                        Changed length of the fields EXTR_REF_ID and 
# MAGIC                                                                                         PCA_EXTRNL_ID to 255 (before it was 20)                                 IntegrateCurDevl           Steph Goddard          03/08/2010
# MAGIC Steph Goddard        2010/06/15    4022 Facets 4.7.1   changed metadata for CLED_EXT_REF to 50                              IntegrateNewDevl
# MAGIC Prabhu ES               2022-02-28      S2S Remediation     MSSQL connection parameters added                                         IntegrateDev5                Kalyan Neelam           2022-06-10

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
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmExtrnlRefPK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','TMP_IDS_CLAIM')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','2006092623456')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrDate = get_widget_value('CurrDate','')

# Read from hashed file hf_clm_fcts_reversals (Scenario C)
df_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# Read from hashed file hf_clm_nasco_dup_bypass (Scenario C)
df_nasco_dup_lkup = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# ODBC Connector stage: CMC_CLED_EDI_DATA
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"SELECT CLCL_ID,CLED_EXT_REF,CLED_TRAD_PARTNER FROM {FacetsOwner}.CMC_CLED_EDI_DATA A, tempdb..{DriverTable} TMP WHERE TMP.CLM_ID = A.CLCL_ID"
df_CMC_CLED_EDI_DATA = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Transformer: StripField
df_Strip = df_CMC_CLED_EDI_DATA.select(
    strip_field("CLCL_ID").alias("CLCL_ID"),
    strip_field("CLED_EXT_REF").alias("CLED_EXT_REF"),
    strip_field("CLED_TRAD_PARTNER").alias("CLED_TRAD_PARTNER")
)

# BusinessRules Transformer
df_business_rules = (
    df_Strip.alias("Strip")
    .join(
        df_fcts_reversals.alias("fcts_reversals"),
        df_Strip["CLCL_ID"] == F.col("fcts_reversals.CLCL_ID"),
        "left"
    )
    .join(
        df_nasco_dup_lkup.alias("nasco_dup_lkup"),
        df_Strip["CLCL_ID"] == F.col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
    .withColumn("claimId", trim(F.col("Strip.CLCL_ID")))
)

# Trans1 output link: IsNull(nasco_dup_lkup.CLM_ID) = @TRUE
df_Trans1 = (
    df_business_rules
    .filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(F.lit("FACETS"), F.lit(";"), F.col("claimId")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_SK"),
        F.col("claimId").alias("CLM_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN"),
        F.when(
            (F.col("Strip.CLED_TRAD_PARTNER") != "EMPOWERED BNFTS")
            | (F.col("Strip.CLED_TRAD_PARTNER") != "PCA RUNOUT")
            | (F.col("Strip.CLED_TRAD_PARTNER") != "HRA DRUG CLAIM"),
            F.col("Strip.CLED_EXT_REF")
        ).otherwise("NA").alias("EXTRNL_REF_ID"),
        F.when(
            (F.col("Strip.CLED_TRAD_PARTNER") == "EMPOWERED BNFTS")
            | (F.col("Strip.CLED_TRAD_PARTNER") == "PCA RUNOUT")
            | (F.col("Strip.CLED_TRAD_PARTNER") == "HRA DRUG CLAIM"),
            F.col("Strip.CLED_EXT_REF")
        ).otherwise("NA").alias("PCA_EXTRNL_ID"),
        F.when(
            (F.col("Strip.CLED_TRAD_PARTNER") == "EMPOWERED BNFTS")
            | (F.col("Strip.CLED_TRAD_PARTNER") == "PCA RUNOUT")
            | (F.col("Strip.CLED_TRAD_PARTNER") == "HRA DRUG CLAIM"),
            F.col("Strip.CLED_TRAD_PARTNER")
        ).otherwise("NA").alias("PCA_SRC_NM"),
        F.when(
            (F.col("Strip.CLED_TRAD_PARTNER") != "EMPOWERED BNFTS")
            | (F.col("Strip.CLED_TRAD_PARTNER") != "PCA RUNOUT")
            | (F.col("Strip.CLED_TRAD_PARTNER") != "HRA DRUG CLAIM"),
            F.col("Strip.CLED_TRAD_PARTNER")
        ).otherwise("NA").alias("TRDNG_PRTNR_NM")
    )
)

# reversals output link: IsNull(fcts_reversals.CLCL_ID) = @FALSE and (CLCL_CUR_STS in 89,91,99)
df_reversals = (
    df_business_rules
    .filter(
        F.col("fcts_reversals.CLCL_ID").isNotNull()
        & (F.col("fcts_reversals.CLCL_CUR_STS").isin("89", "91", "99"))
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(F.lit("FACETS"), F.lit(";"), F.col("claimId"), F.lit("R")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_SK"),
        F.concat(F.col("claimId"), F.lit("R")).alias("CLM_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN"),
        F.when(
            (F.col("Strip.CLED_TRAD_PARTNER") != "EMPOWERED BNFTS")
            | (F.col("Strip.CLED_TRAD_PARTNER") != "PCA RUNOUT")
            | (F.col("Strip.CLED_TRAD_PARTNER") != "HRA DRUG CLAIM"),
            F.col("Strip.CLED_EXT_REF")
        ).otherwise("NA").alias("EXTRNL_REF_ID"),
        F.when(
            (F.col("Strip.CLED_TRAD_PARTNER") == "EMPOWERED BNFTS")
            | (F.col("Strip.CLED_TRAD_PARTNER") == "PCA RUNOUT"),
            F.col("Strip.CLED_EXT_REF")
        ).otherwise("NA").alias("PCA_EXTRNL_ID"),
        F.when(
            (F.col("Strip.CLED_TRAD_PARTNER") == "EMPOWERED BNFTS")
            | (F.col("Strip.CLED_TRAD_PARTNER") == "PCA RUNOUT")
            | (F.col("Strip.CLED_TRAD_PARTNER") == "HRA DRUG CLAIM"),
            F.col("Strip.CLED_TRAD_PARTNER")
        ).otherwise("NA").alias("PCA_SRC_NM"),
        F.when(
            (F.col("Strip.CLED_TRAD_PARTNER") != "EMPOWERED BNFTS")
            | (F.col("Strip.CLED_TRAD_PARTNER") != "PCA RUNOUT")
            | (F.col("Strip.CLED_TRAD_PARTNER") != "HRA DRUG CLAIM"),
            F.col("Strip.CLED_TRAD_PARTNER")
        ).otherwise("NA").alias("TRDNG_PRTNR_NM")
    )
)

# Collector stage
df_collector = df_Trans1.unionByName(df_reversals)

# Shared Container: ClmExtrnlRefPK
params = {
    "CurrRunCycle": CurrRunCycle
}
df_clm_extrnl_ref_pk = ClmExtrnlRefPK(df_collector, params)

# Final Seq File Output: IdsClmExtrnlRef
df_final = df_clm_extrnl_ref_pk.select(
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
    F.col("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("EXTRNL_REF_ID"),
    F.col("PCA_EXTRNL_ID"),
    F.col("PCA_SRC_NM"),
    F.col("TRDNG_PRTNR_NM")
)

write_files(
    df_final,
    f"{adls_path}/key/FctsClmExtrnlRefDataExtr.FctsClmExtrnlRefData.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)