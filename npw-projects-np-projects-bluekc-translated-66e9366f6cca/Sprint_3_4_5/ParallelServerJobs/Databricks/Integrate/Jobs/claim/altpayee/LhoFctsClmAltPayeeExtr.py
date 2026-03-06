# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     LhoFctsClmAltPayeeExtr
# MAGIC CALLED BY:  LhoFctsClmExtr1Seq
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
# MAGIC Prabhu ES               2022-03-29      S2S                          MSSQL ODBC conn params added                                             IntegrateDev5		Ken Bradmon	2022-06-09

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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve all parameter values
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','LUMERIS')

# Read from hf_clm_fcts_reversals (CHashedFileStage, Scenario A => drop duplicates on key CLCL_ID)
df_hf_clm_fcts_reversals = (
    spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
    .dropDuplicates(["CLCL_ID"])
)

# Read from clm_nasco_dup_bypass (CHashedFileStage, Scenario A => drop duplicates on key CLM_ID)
df_clm_nasco_dup_bypass = (
    spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
    .dropDuplicates(["CLM_ID"])
)

# ODBCConnector: CMC_CLAP_ALT_PAYEE
jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)
extract_query = (
    f"SELECT CLCL_ID,CLAP_NAME,CLAP_ADDR1,CLAP_ADDR2,CLAP_ADDR3,CLAP_CITY,CLAP_STATE,CLAP_ZIP,"
    f"CLAP_COUNTY,CLAP_CTRY_CD,CLAP_PHONE,CLAP_PHONE_EXT,CLAP_FAX,CLAP_FAX_EXT,CLAP_EMAIL,"
    f"CLAP_LOCK_TOKEN,ATXR_SOURCE_ID "
    f"FROM {LhoFacetsStgOwner}.CMC_CLAP_ALT_PAYEE A, tempdb..{DriverTable} TMP "
    f"WHERE TMP.CLM_ID = A.CLCL_ID"
)
df_CMC_CLAP_ALT_PAYEE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Transformer: StripField
df_Strip = df_CMC_CLAP_ALT_PAYEE.select(
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

# BusinessRules Transformer (with two lookup links)
df_BusinessRules_joined = (
    df_Strip.alias("Strip")
    .join(df_hf_clm_fcts_reversals.alias("fcts_reversals"), F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"), how="left")
    .join(df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"), F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"), how="left")
)

# Trans1 output link
df_BusinessRules_Trans1 = (
    df_BusinessRules_joined
    .filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
    .withColumn("clclid", trim(F.col("Strip.CLCL_ID")))
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("clclid")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_ALT_PAYE_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.rpad(F.col("clclid"), 18, " ").alias("CLM_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("CLM_SK"),
        F.rpad(trim(F.col("Strip.CLAP_NAME")), 50, " ").alias("ALT_PAYE_NM"),
        F.rpad(trim(F.col("Strip.CLAP_ADDR1")), 40, " ").alias("ADDR_LN_1"),
        F.rpad(trim(F.col("Strip.CLAP_ADDR2")), 40, " ").alias("ADDR_LN_2"),
        F.rpad(trim(F.col("Strip.CLAP_ADDR3")), 40, " ").alias("ADDR_LN_3"),
        F.rpad(trim(F.col("Strip.CLAP_CITY")), 19, " ").alias("CITY_NM"),
        F.rpad(trim(F.col("Strip.CLAP_STATE")), 2, " ").alias("CLM_ALT_PAYE_ST_CD"),
        F.rpad(trim(F.col("Strip.CLAP_ZIP")), 11, " ").alias("POSTAL_CD"),
        F.rpad(trim(F.col("Strip.CLAP_COUNTY")), 20, " ").alias("CNTY_NM"),
        F.rpad(F.lit("USA"), 4, " ").alias("CLM_ALT_PAYE_CTRY_CD"),
        F.rpad(trim(F.col("Strip.CLAP_PHONE")), 20, " ").alias("PHN_NO"),
        F.rpad(trim(F.col("Strip.CLAP_PHONE_EXT")), 4, " ").alias("PHN_NO_EXT"),
        F.rpad(trim(F.col("Strip.CLAP_FAX")), 20, " ").alias("FAX_NO"),
        F.rpad(trim(F.col("Strip.CLAP_FAX_EXT")), 4, " ").alias("FAX_NO_EXT"),
        F.rpad(trim(F.col("Strip.CLAP_EMAIL")), 40, " ").alias("EMAIL_ADDR")
    )
)

# reversals output link
df_BusinessRules_reversals = (
    df_BusinessRules_joined
    .filter(
        F.col("fcts_reversals.CLCL_ID").isNotNull() &
        (
            (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("89")) |
            (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("91")) |
            (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("99"))
        )
    )
    .withColumn("clclid", trim(F.col("Strip.CLCL_ID")))
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("clclid"), F.lit("R")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_ALT_PAYE_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.rpad(F.concat(F.col("clclid"), F.lit("R")), 18, " ").alias("CLM_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("CLM_SK"),
        F.rpad(trim(F.col("Strip.CLAP_NAME")), 50, " ").alias("ALT_PAYE_NM"),
        F.rpad(trim(F.col("Strip.CLAP_ADDR1")), 40, " ").alias("ADDR_LN_1"),
        F.rpad(trim(F.col("Strip.CLAP_ADDR2")), 40, " ").alias("ADDR_LN_2"),
        F.rpad(trim(F.col("Strip.CLAP_ADDR3")), 40, " ").alias("ADDR_LN_3"),
        F.rpad(trim(F.col("Strip.CLAP_CITY")), 19, " ").alias("CITY_NM"),
        F.rpad(trim(F.col("Strip.CLAP_STATE")), 2, " ").alias("CLM_ALT_PAYE_ST_CD"),
        F.rpad(trim(F.col("Strip.CLAP_ZIP")), 11, " ").alias("POSTAL_CD"),
        F.rpad(trim(F.col("Strip.CLAP_COUNTY")), 20, " ").alias("CNTY_NM"),
        F.rpad(F.lit("USA"), 4, " ").alias("CLM_ALT_PAYE_CTRY_CD"),
        F.rpad(trim(F.col("Strip.CLAP_PHONE")), 20, " ").alias("PHN_NO"),
        F.rpad(trim(F.col("Strip.CLAP_PHONE_EXT")), 4, " ").alias("PHN_NO_EXT"),
        F.rpad(trim(F.col("Strip.CLAP_FAX")), 20, " ").alias("FAX_NO"),
        F.rpad(trim(F.col("Strip.CLAP_FAX_EXT")), 4, " ").alias("FAX_NO_EXT"),
        F.rpad(trim(F.col("Strip.CLAP_EMAIL")), 40, " ").alias("EMAIL_ADDR")
    )
)

# Collector (CCollector) - Round-Robin => union the two DataFrames
df_Collector = df_BusinessRules_Trans1.unionByName(df_BusinessRules_reversals)

# Snapshot (CTransformerStage) has two outputs: "Pkey" and "Snapshot"

# Output Pkey -> ClmAltPayeePK (30 columns)
df_Snapshot_Pkey = df_Collector.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_ALT_PAYE_SK").alias("CLM_ALT_PAYE_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("ALT_PAYE_NM").alias("ALT_PAYE_NM"),
    F.col("ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("CITY_NM").alias("CITY_NM"),
    F.col("CLM_ALT_PAYE_ST_CD").alias("CLM_ALT_PAYE_ST_CD"),
    F.col("POSTAL_CD").alias("POSTAL_CD"),
    F.col("CNTY_NM").alias("CNTY_NM"),
    F.col("CLM_ALT_PAYE_CTRY_CD").alias("CLM_ALT_PAYE_CTRY_CD"),
    F.col("PHN_NO").alias("PHN_NO"),
    F.col("PHN_NO_EXT").alias("PHN_NO_EXT"),
    F.col("FAX_NO").alias("FAX_NO"),
    F.col("FAX_NO_EXT").alias("FAX_NO_EXT"),
    F.col("EMAIL_ADDR").alias("EMAIL_ADDR")
)

# Output Snapshot -> Transformer (1 column)
df_Snapshot_Snapshot = df_Collector.select(
    F.col("CLM_ID").alias("CLCL_ID")
)

# Transformer (CTransformerStage) -> B_CLM_ALT_PAYE
df_Transformer = df_Snapshot_Snapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLCL_ID").alias("CLM_ID")
)

write_files(
    df_Transformer,
    f"{adls_path}/load/B_CLM_ALT_PAYE.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmAltPayeePK
# COMMAND ----------

params_ClmAltPayeePK = {
    "CurrRunCycle": CurrRunCycle
}
df_ClmAltPayeePK = ClmAltPayeePK(df_Snapshot_Pkey, params_ClmAltPayeePK)

write_files(
    df_ClmAltPayeePK,
    f"{adls_path}/key/LhoFctsClmAltPayeExtr.LhoFctsClmAltPaye.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)