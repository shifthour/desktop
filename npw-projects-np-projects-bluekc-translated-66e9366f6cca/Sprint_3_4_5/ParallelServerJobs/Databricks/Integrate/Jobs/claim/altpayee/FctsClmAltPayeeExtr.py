# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmAltPayeeExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CLAP_ALT_PAYEE to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	CMC_CLAP_ALT_PAYEE
# MAGIC                 Joined with 
# MAGIC                 tempdb..TMP_IDS_CLAIM  to get claims updated within time period
# MAGIC   
# MAGIC HASH FILES:  hf_clm_nasco_dup_bypass
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   STRIP.FIELD
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Oliver Nielsen  05/18/2004-   Originally Programmed - For ids version 1.1
# MAGIC Sharon Andrew  06/20/2004  - Standardized and corrected documentation.
# MAGIC Sharon Andrew  11/01/2004  - Facets claim reversal.  Added logic to test if claim status is '91'.  If so, write out record with ClaimId and an R on the end.   If there are any amount fields, make them negative.
# MAGIC Steph Goddard 02/08/2006   Changed for sequencer - combined extract, transform, and primary key jobs
# MAGIC BJ Luce            03/20/2006     add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC Sanderw  12/08/2006   Project 1576  - Reversal logix added for new status codes 89 and  and 99
# MAGIC Oliver Nielsen          08/15/2007                                    Added Snapshot extract for balancing                                          devlIDS30                       Steph Goddard         8/30/07                                                  
# MAGIC 
# MAGIC Bhoomi Dasari         2008-08-05      3567(Primary Key)    Changed primary key process from hash file to DB2 table            devlIDS                           Steph Goddard        08/15/2008
# MAGIC Prabhu ES               2022-02-28      S2S Remediation     MSSQL connection parameters added                                        IntegrateDev5                  Kalyan Neelam         2022-06-10

# MAGIC Assign primary surrogate key
# MAGIC Apply business logic
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Extract Facets Alternate Payee Data
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
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# Read Hashed File: hf_clm_fcts_reversals (Scenario C -> read parquet)
schema_hf_clm_fcts_reversals = StructType([
    StructField("CLCL_ID", StringType(), True),
    StructField("CLCL_CUR_STS", StringType(), True),
    StructField("CLCL_PAID_DT", TimestampType(), True),
    StructField("CLCL_ID_ADJ_TO", StringType(), True),
    StructField("CLCL_ID_ADJ_FROM", StringType(), True)
])
df_hf_clm_fcts_reversals = spark.read.schema(schema_hf_clm_fcts_reversals).parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# Read Hashed File: clm_nasco_dup_bypass (Scenario C -> read parquet)
schema_clm_nasco_dup_bypass = StructType([
    StructField("CLM_ID", StringType(), True)
])
df_clm_nasco_dup_bypass = spark.read.schema(schema_clm_nasco_dup_bypass).parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# ODBCConnector: CMC_CLAP_ALT_PAYEE
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = (
    f"SELECT CLCL_ID,CLAP_NAME,CLAP_ADDR1,CLAP_ADDR2,CLAP_ADDR3,CLAP_CITY,CLAP_STATE,CLAP_ZIP,"
    f"CLAP_COUNTY,CLAP_CTRY_CD,CLAP_PHONE,CLAP_PHONE_EXT,CLAP_FAX,CLAP_FAX_EXT,CLAP_EMAIL,"
    f"CLAP_LOCK_TOKEN,ATXR_SOURCE_ID "
    f"FROM {FacetsOwner}.CMC_CLAP_ALT_PAYEE A, tempdb..{DriverTable} TMP "
    f"WHERE TMP.CLM_ID = A.CLCL_ID"
)
df_CMC_CLAP_ALT_PAYEE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# StripField (CTransformerStage)
df_StripField = df_CMC_CLAP_ALT_PAYEE.select(
    F.col("CLCL_ID").alias("CLCL_ID"),
    strip_field(F.col("CLAP_NAME")).alias("CLAP_NAME"),
    strip_field(F.col("CLAP_ADDR1")).alias("CLAP_ADDR1"),
    strip_field(F.col("CLAP_ADDR2")).alias("CLAP_ADDR2"),
    strip_field(F.col("CLAP_ADDR3")).alias("CLAP_ADDR3"),
    strip_field(F.col("CLAP_CITY")).alias("CLAP_CITY"),
    strip_field(F.col("CLAP_STATE")).alias("CLAP_STATE"),
    strip_field(F.col("CLAP_ZIP")).alias("CLAP_ZIP"),
    strip_field(F.col("CLAP_COUNTY")).alias("CLAP_COUNTY"),
    strip_field(F.col("CLAP_CTRY_CD")).alias("CLAP_CTRY_CD"),
    strip_field(F.col("CLAP_PHONE")).alias("CLAP_PHONE"),
    strip_field(F.col("CLAP_PHONE_EXT")).alias("CLAP_PHONE_EXT"),
    strip_field(F.col("CLAP_FAX")).alias("CLAP_FAX"),
    strip_field(F.col("CLAP_FAX_EXT")).alias("CLAP_FAX_EXT"),
    strip_field(F.col("CLAP_EMAIL")).alias("CLAP_EMAIL"),
    F.col("CLAP_LOCK_TOKEN").alias("CLAP_LOCK_TOKEN"),
    F.col("ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID")
)

# BusinessRules (CTransformerStage) - Join with hashed files
df_BusinessRulesJoined = (
    df_StripField.alias("Strip")
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
    .withColumn("clclid", trim(F.col("Strip.CLCL_ID")))
)

# Trans1 constraint: IsNull(nasco_dup_lkup.CLM_ID) = True
df_Trans1 = df_BusinessRulesJoined.filter(F.isnull(F.col("nasco_dup_lkup.CLM_ID"))).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"), F.col("clclid")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_ALT_PAYE_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("clclid").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.trim(F.col("Strip.CLAP_NAME")).alias("ALT_PAYE_NM"),
    F.trim(F.col("Strip.CLAP_ADDR1")).alias("ADDR_LN_1"),
    F.trim(F.col("Strip.CLAP_ADDR2")).alias("ADDR_LN_2"),
    F.trim(F.col("Strip.CLAP_ADDR3")).alias("ADDR_LN_3"),
    F.trim(F.col("Strip.CLAP_CITY")).alias("CITY_NM"),
    F.trim(F.col("Strip.CLAP_STATE")).alias("CLM_ALT_PAYE_ST_CD"),
    F.trim(F.col("Strip.CLAP_ZIP")).alias("POSTAL_CD"),
    F.trim(F.col("Strip.CLAP_COUNTY")).alias("CNTY_NM"),
    F.lit("USA").alias("CLM_ALT_PAYE_CTRY_CD"),
    F.trim(F.col("Strip.CLAP_PHONE")).alias("PHN_NO"),
    F.trim(F.col("Strip.CLAP_PHONE_EXT")).alias("PHN_NO_EXT"),
    F.trim(F.col("Strip.CLAP_FAX")).alias("FAX_NO"),
    F.trim(F.col("Strip.CLAP_FAX_EXT")).alias("FAX_NO_EXT"),
    F.trim(F.col("Strip.CLAP_EMAIL")).alias("EMAIL_ADDR")
)

# Reversals constraint: IsNull(fcts_reversals.CLCL_ID) = False and CLCL_CUR_STS in ('89','91','99')
df_Reversals = df_BusinessRulesJoined.filter(
    (~F.isnull(F.col("fcts_reversals.CLCL_ID"))) &
    (F.col("fcts_reversals.CLCL_CUR_STS").isin(["89","91","99"]))
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"), F.col("clclid"), F.lit("R")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_ALT_PAYE_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.concat(F.col("clclid"), F.lit("R")).alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.trim(F.col("Strip.CLAP_NAME")).alias("ALT_PAYE_NM"),
    F.trim(F.col("Strip.CLAP_ADDR1")).alias("ADDR_LN_1"),
    F.trim(F.col("Strip.CLAP_ADDR2")).alias("ADDR_LN_2"),
    F.trim(F.col("Strip.CLAP_ADDR3")).alias("ADDR_LN_3"),
    F.trim(F.col("Strip.CLAP_CITY")).alias("CITY_NM"),
    F.trim(F.col("Strip.CLAP_STATE")).alias("CLM_ALT_PAYE_ST_CD"),
    F.trim(F.col("Strip.CLAP_ZIP")).alias("POSTAL_CD"),
    F.trim(F.col("Strip.CLAP_COUNTY")).alias("CNTY_NM"),
    F.lit("USA").alias("CLM_ALT_PAYE_CTRY_CD"),
    F.trim(F.col("Strip.CLAP_PHONE")).alias("PHN_NO"),
    F.trim(F.col("Strip.CLAP_PHONE_EXT")).alias("PHN_NO_EXT"),
    F.trim(F.col("Strip.CLAP_FAX")).alias("FAX_NO"),
    F.trim(F.col("Strip.CLAP_FAX_EXT")).alias("FAX_NO_EXT"),
    F.trim(F.col("Strip.CLAP_EMAIL")).alias("EMAIL_ADDR")
)

# Collector (CCollector) - Union trans1 and reversals
df_Collector = df_Trans1.unionByName(df_Reversals)

# Snapshot (CTransformerStage)
df_SnapshotPkey = df_Collector
df_SnapshotTransformer = df_Collector.select(F.col("CLM_ID").alias("CLCL_ID"))

# Transformer (CTransformerStage) -> Output to B_CLM_ALT_PAYEE
df_Transformer = df_SnapshotTransformer.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLCL_ID").alias("CLM_ID")
)

# Write to B_CLM_ALT_PAYEE (CSeqFileStage)
df_final_B_CLM_ALT_PAYEE = df_Transformer.withColumn(
    "CLM_ID", F.rpad(F.col("CLM_ID"), 18, " ")
).select("SRC_SYS_CD_SK","CLM_ID")

write_files(
    df_final_B_CLM_ALT_PAYEE,
    f"{adls_path}/load/B_CLM_ALT_PAYEE.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# Shared Container: ClmAltPayeePK
# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmAltPayeePK
params_ClmAltPayeePK = {
    "CurrRunCycle": CurrRunCycle
}
df_ClmAltPayeePK = ClmAltPayeePK(df_SnapshotPkey, params_ClmAltPayeePK)

# IdsClmAltPayee (CSeqFileStage)
df_IdsClmAltPayee = (
    df_ClmAltPayeePK
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
    .withColumn("CLM_ALT_PAYE_ST_CD", F.rpad(F.col("CLM_ALT_PAYE_ST_CD"), 2, " "))
    .withColumn("POSTAL_CD", F.rpad(F.col("POSTAL_CD"), 11, " "))
    .withColumn("CLM_ALT_PAYE_CTRY_CD", F.rpad(F.col("CLM_ALT_PAYE_CTRY_CD"), 4, " "))
    .withColumn("ALT_PAYE_NM", F.rpad(F.col("ALT_PAYE_NM"), 50, " "))
    .withColumn("ADDR_LN_1", F.rpad(F.col("ADDR_LN_1"), 40, " "))
    .withColumn("ADDR_LN_2", F.rpad(F.col("ADDR_LN_2"), 40, " "))
    .withColumn("ADDR_LN_3", F.rpad(F.col("ADDR_LN_3"), 40, " "))
    .withColumn("CITY_NM", F.rpad(F.col("CITY_NM"), 19, " "))
    .withColumn("CNTY_NM", F.rpad(F.col("CNTY_NM"), 20, " "))
    .withColumn("PHN_NO", F.rpad(F.col("PHN_NO"), 20, " "))
    .withColumn("PHN_NO_EXT", F.rpad(F.col("PHN_NO_EXT"), 4, " "))
    .withColumn("FAX_NO", F.rpad(F.col("FAX_NO"), 20, " "))
    .withColumn("FAX_NO_EXT", F.rpad(F.col("FAX_NO_EXT"), 4, " "))
    .withColumn("EMAIL_ADDR", F.rpad(F.col("EMAIL_ADDR"), 40, " "))
    .select([
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_ALT_PAYE_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "ALT_PAYE_NM",
        "ADDR_LN_1",
        "ADDR_LN_2",
        "ADDR_LN_3",
        "CITY_NM",
        "CLM_ALT_PAYE_ST_CD",
        "POSTAL_CD",
        "CNTY_NM",
        "CLM_ALT_PAYE_CTRY_CD",
        "PHN_NO",
        "PHN_NO_EXT",
        "FAX_NO",
        "FAX_NO_EXT",
        "EMAIL_ADDR"
    ])
)

write_files(
    df_IdsClmAltPayee,
    f"{adls_path}/key/FctsClmAltPayeExtr.FctsClmAltPaye.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)