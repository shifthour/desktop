# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC 6;hf_clm_extrnl_prov_newITS_addr;hf_clm_extrnl_prov_oldITS_addr;hf_clm_extrnl_prov_old_ITS_tax_id;hf_clm_extrnl_prov_newITS;hf_clm_extrnl_prov_oldITS;hf_clm_extrnl_prov_all_claims
# MAGIC 
# MAGIC 
# MAGIC  Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  LhoFctsClmExtrnlProvExtr
# MAGIC Called By: LhoFctsClmExtr1Seq
# MAGIC 
# MAGIC DESCRIPTION: Runs Facets External Provider extract.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Reddy sanam           307700            Cloned from Facets and made changes 
# MAGIC                                                         needed for LUMERIS                                                                                        IntegrateDev2                                        Manasa Andru             2020-11-13
# MAGIC Prabhu ES               2022-03-29      S2S                         MSSQL ODBC conn params added                                   IntegrateDev5                        Manasa Andru             2022-06-09

# MAGIC Prov ID and Tax ID are on BLE_EXCHG table by SCCF no, however, only original SCCF no is on the table so only match first 15 characters of SCCF
# MAGIC Apply business logic
# MAGIC Extract Facets Claim External Provider Data.   
# MAGIC Mostly deals with ITS Home claims
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    DateType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmExtrnlProvPK
# COMMAND ----------

# --------------------------------------------------------------------------------
# Retrieve all job parameters
# --------------------------------------------------------------------------------
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrentDate = get_widget_value('CurrentDate','')
RunID = get_widget_value('RunID','')
DriverTable = get_widget_value('DriverTable','')
AdjTable = get_widget_value('AdjTable','')
facetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# --------------------------------------------------------------------------------
# Stage 1: hf_clm_fcts_reversals (CHashedFileStage, Scenario C)
# Read from hf_clm_fcts_reversals.parquet
# --------------------------------------------------------------------------------
hf_clm_fcts_reversals_schema = StructType([
    StructField("CLCL_ID", StringType(), nullable=False),
    StructField("CLCL_CUR_STS", StringType(), nullable=False),
    StructField("CLCL_PAID_DT", TimestampType(), nullable=False),
    StructField("CLCL_ID_ADJ_TO", StringType(), nullable=False),
    StructField("CLCL_ID_ADJ_FROM", StringType(), nullable=False)
])
df_hf_clm_fcts_reversals = (
    spark.read.schema(hf_clm_fcts_reversals_schema)
    .parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
)

# --------------------------------------------------------------------------------
# Stage 2: clm_nasco_dup_bypass (CHashedFileStage, Scenario C)
# Read from hf_clm_nasco_dup_bypass.parquet
# --------------------------------------------------------------------------------
hf_clm_nasco_dup_bypass_schema = StructType([
    StructField("CLM_ID", StringType(), nullable=False)
])
df_clm_nasco_dup_bypass = (
    spark.read.schema(hf_clm_nasco_dup_bypass_schema)
    .parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
)

# --------------------------------------------------------------------------------
# Stage 3: CMC_CLPP_ITS_PROV (ODBCConnector)
# Read from the facets DB using the provided SQL
# --------------------------------------------------------------------------------
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_CMC_CLPP_ITS_PROV = f"""
SELECT 
TMP.CLM_ID CLCL_ID, 
PROV.CLPP_BCBS_PR_ID, 
PROV.CLPP_BCBS_PR_NPI,  
PROV.CLPP_CNTRY_CD, 
PROV.CLPP_FED_TAX_ID,  
PROV.CLPP_PERF_ID,  
PROV.CLPP_PERF_ID_NPI,
PROV.CLPP_PHONE,  
PROV.CLPP_PR_ADDR1,  
PROV.CLPP_PR_ADDR2,  
PROV.CLPP_PR_CITY, 
PROV.CLPP_PR_NAME, 
PROV.CLPP_PR_STATE,  
PROV.CLPP_ZIP, 
PROV.CLPP_CLM_SUB_ORG, 
PROV.CLPP_PERF_NAME
FROM {facetsOwner}.CMC_CLPP_ITS_PROV PROV,             
     tempdb..{DriverTable} TMP ,
     {facetsOwner}.CMC_CLMI_MISC MISC
WHERE TMP.CLM_ID = PROV.CLCL_ID 
  AND TMP.CLM_ID = MISC.CLCL_ID
  AND MISC.CLMI_ITS_SUB_TYPE IN ('H','T')
"""
df_CMC_CLPP_ITS_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_CLPP_ITS_PROV)
    .load()
)

# --------------------------------------------------------------------------------
# Stage 4: hf_clm_extrnl_prov_newITS_addr (CHashedFileStage, Scenario C)
# This stage writes from df_CMC_CLPP_ITS_PROV into hf_clm_extrnl_prov_newITS_addr.parquet
# and then reads it to produce two outputs: NewITSAddr and lkupconstrnt
# Both outputs share the same columns in the job.
# --------------------------------------------------------------------------------
hf_clm_extrnl_prov_newITS_addr_schema = StructType([
    StructField("CLCL_ID", StringType(), nullable=False),
    StructField("CLPP_BCBS_PR_ID", StringType(), nullable=False),
    StructField("CLPP_BCBS_PR_NPI", StringType(), nullable=False),
    StructField("CLPP_CNTRY_CD", StringType(), nullable=False),
    StructField("CLPP_FED_TAX_ID", StringType(), nullable=False),
    StructField("CLPP_PERF_ID", StringType(), nullable=False),
    StructField("CLPP_PERF_ID_NPI", StringType(), nullable=False),
    StructField("CLPP_PHONE", StringType(), nullable=False),
    StructField("CLPP_PR_ADDR1", StringType(), nullable=False),
    StructField("CLPP_PR_ADDR2", StringType(), nullable=False),
    StructField("CLPP_PR_CITY", StringType(), nullable=False),
    StructField("CLPP_PR_NAME", StringType(), nullable=False),
    StructField("CLPP_PR_STATE", StringType(), nullable=False),
    StructField("CLPP_ZIP", StringType(), nullable=False),
    StructField("CLPP_CLM_SUB_ORG", StringType(), nullable=False),
    StructField("CLPP_PERF_NAME", StringType(), nullable=False)
])

# Select the columns from df_CMC_CLPP_ITS_PROV and write to parquet
df_write_hf_clm_extrnl_prov_newITS_addr = df_CMC_CLPP_ITS_PROV.select(
    F.col("CLCL_ID").cast(StringType()),
    F.col("CLPP_BCBS_PR_ID").cast(StringType()),
    F.col("CLPP_BCBS_PR_NPI").cast(StringType()),
    F.col("CLPP_CNTRY_CD").cast(StringType()),
    F.col("CLPP_FED_TAX_ID").cast(StringType()),
    F.col("CLPP_PERF_ID").cast(StringType()),
    F.col("CLPP_PERF_ID_NPI").cast(StringType()),
    F.col("CLPP_PHONE").cast(StringType()),
    F.col("CLPP_PR_ADDR1").cast(StringType()),
    F.col("CLPP_PR_ADDR2").cast(StringType()),
    F.col("CLPP_PR_CITY").cast(StringType()),
    F.col("CLPP_PR_NAME").cast(StringType()),
    F.col("CLPP_PR_STATE").cast(StringType()),
    F.col("CLPP_ZIP").cast(StringType()),
    F.col("CLPP_CLM_SUB_ORG").cast(StringType()),
    F.col("CLPP_PERF_NAME").cast(StringType())
)
write_files(
    df_write_hf_clm_extrnl_prov_newITS_addr,
    f"{adls_path}/hf_clm_extrnl_prov_newITS_addr.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Now read it back to produce the base DataFrame that can serve as 
# "NewITSAddr" and "lkupconstrnt" outputs:
df_hf_clm_extrnl_prov_newITS_addr = (
    spark.read.schema(hf_clm_extrnl_prov_newITS_addr_schema)
    .parquet(f"{adls_path}/hf_clm_extrnl_prov_newITS_addr.parquet")
)

# We'll define df_NewITSAddr and df_lkupconstrnt as the same data:
df_NewITSAddr = df_hf_clm_extrnl_prov_newITS_addr
df_lkupconstrnt = df_hf_clm_extrnl_prov_newITS_addr

# --------------------------------------------------------------------------------
# Stage 5: CMC_CLCL_CLAIM (ODBCConnector)
# --------------------------------------------------------------------------------
extract_query_CMC_CLCL_CLAIM = f"""
SELECT distinct 
CLM.CLCL_ID, 
CLM.CLCL_CL_SUB_TYPE, 
CLM.CLCL_CUR_STS, 
CLM.CLCL_LAST_ACT_DTM, 
MISC.CLMI_ITS_SCCF_NO,
MISC.CLMI_ITS_SUB_TYPE
FROM {facetsOwner}.CMC_CLMI_MISC MISC,
     {facetsOwner}.CMC_CLCL_CLAIM CLM, 
     tempdb..{DriverTable} TMP 
WHERE TMP.CLM_ID = CLM.CLCL_ID 
  AND CLM.CLCL_ID = MISC.CLCL_ID
  AND (
       SUBSTRING(CLM.CLCL_ID, 6,1) = 'H' 
    OR SUBSTRING(CLM.CLCL_ID,6,2) = 'RH'  
    OR SUBSTRING(CLM.CLCL_ID,6,1) = 'K'
    OR SUBSTRING(CLM.CLCL_ID,6,1) = 'G'     
    OR MISC.CLMI_ITS_SUB_TYPE IN ('H','T')
  )
"""
df_CMC_CLCL_CLAIM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_CLCL_CLAIM)
    .load()
)

# --------------------------------------------------------------------------------
# Stage 6: old_or_new_its (CTransformerStage)
# We will join df_CMC_CLCL_CLAIM (Extract1) with df_lkupconstrnt (lkupconstrnt) on CLCL_ID (left join).
# Then produce two outputs:
#   old_its -> constraint: svOldITS = 'Y'
#   new_its -> constraint: svOldITS = 'N'
# --------------------------------------------------------------------------------

df_old_or_new_its_joined = (
    df_CMC_CLCL_CLAIM.alias("Extract1")
    .join(
        df_lkupconstrnt.alias("lkupconstrnt"),
        F.col("Extract1.CLCL_ID") == F.col("lkupconstrnt.CLCL_ID"),
        how="left"
    )
)

# svOldITS expression:
# If (Len(Trim(lkupconstrnt.CLCL_ID)) = 0 or IsNull(...)) then 'Y' else 'N'
df_old_or_new_its_with_flag = df_old_or_new_its_joined.withColumn(
    "svOldITS",
    F.when(
        (F.col("lkupconstrnt.CLCL_ID").isNull()) |
        (F.trim(strip_field(F.col("lkupconstrnt.CLCL_ID"))) == ""),
        F.lit("Y")
    ).otherwise("N")
)

df_old_its = df_old_or_new_its_with_flag.filter(F.col("svOldITS") == "Y")
df_new_its = df_old_or_new_its_with_flag.filter(F.col("svOldITS") == "N")

# Build the old_its output DataFrame with the specified columns
df_old_its_output = df_old_its.select(
    strip_field(F.col("Extract1.CLCL_ID")).alias("CLCL_ID"),
    strip_field(F.col("Extract1.CLCL_CL_SUB_TYPE")).alias("CLCL_CL_SUB_TYPE"),
    strip_field(F.col("Extract1.CLMI_ITS_SCCF_NO")).alias("CLMI_ITS_SCCF_NO")
)

# Build the new_its output DataFrame with the specified columns
df_new_its_output = df_new_its.select(
    strip_field(F.col("Extract1.CLCL_ID")).alias("CLCL_ID"),
    strip_field(F.col("Extract1.CLCL_CL_SUB_TYPE")).alias("CLCL_CL_SUB_TYPE"),
    strip_field(F.col("Extract1.CLMI_ITS_SCCF_NO")).alias("CLMI_ITS_SCCF_NO")
)

# --------------------------------------------------------------------------------
# Stage 7: StripField (CTransformerStage)
# InputPins:
#   PrimaryLink = new_its
#   LookupLink = NewITSAddr
# Then produce one output => Strip1 => goes to hf_clm_extrn_prov_newITS
# --------------------------------------------------------------------------------
# Join on new_its.CLCL_ID = NewITSAddr.CLCL_ID (left join)

df_StripField_joined = (
    df_new_its_output.alias("new_its")
    .join(
        df_NewITSAddr.alias("NewITSAddr"),
        F.col("new_its.CLCL_ID") == F.col("NewITSAddr.CLCL_ID"),
        how="left"
    )
)

df_StripField_output = df_StripField_joined.select(
    strip_field(F.col("new_its.CLCL_ID")).alias("CLCL_ID"),
    strip_field(F.col("new_its.CLCL_CL_SUB_TYPE")).alias("CLCL_CL_SUB_TYPE"),
    trim(F.col("NewITSAddr.CLPP_BCBS_PR_ID")).alias("CLPP_BCBS_PR_ID"),
    trim(F.col("NewITSAddr.CLPP_BCBS_PR_NPI")).alias("CLPP_BCBS_PR_NPI"),
    trim(F.col("NewITSAddr.CLPP_CNTRY_CD")).alias("CLPP_CNTRY_CD"),
    trim(F.col("NewITSAddr.CLPP_FED_TAX_ID")).alias("CLPP_FED_TAX_ID"),
    trim(F.col("NewITSAddr.CLPP_PERF_ID")).alias("CLPP_PERF_ID"),
    trim(F.col("NewITSAddr.CLPP_PERF_ID_NPI")).alias("CLPP_PERF_ID_NPI"),
    trim(F.col("NewITSAddr.CLPP_PHONE")).alias("CLPP_PHONE"),
    trim(F.col("NewITSAddr.CLPP_PR_ADDR1")).alias("CLPP_PR_ADDR1"),
    trim(F.col("NewITSAddr.CLPP_PR_ADDR2")).alias("CLPP_PR_ADDR2"),
    trim(F.col("NewITSAddr.CLPP_PR_CITY")).alias("CLPP_PR_CITY"),
    trim(F.col("NewITSAddr.CLPP_PR_NAME")).alias("CLPP_PR_NAME"),
    trim(F.col("NewITSAddr.CLPP_PR_STATE")).alias("CLPP_PR_STATE"),
    trim(F.col("NewITSAddr.CLPP_ZIP")).alias("CLPP_ZIP"),
    F.when(
        F.col("new_its.CLCL_CL_SUB_TYPE") == "H",
        trim(F.col("NewITSAddr.CLPP_PR_NAME"))
    ).otherwise(
        F.when(
            (F.trim(F.col("new_its.CLCL_CL_SUB_TYPE")) == "M") & 
            (F.length(F.trim(F.col("NewITSAddr.CLPP_CLM_SUB_ORG"))) > 0),
            F.col("NewITSAddr.CLPP_CLM_SUB_ORG")
        ).otherwise(
            F.when(
                (F.trim(F.col("new_its.CLCL_CL_SUB_TYPE")) == "M") & 
                (F.length(F.trim(F.col("NewITSAddr.CLPP_PR_NAME"))) > 0),
                F.col("NewITSAddr.CLPP_PR_NAME")
            ).otherwise(
                F.when(
                    (F.trim(F.col("new_its.CLCL_CL_SUB_TYPE")) == "M"),
                    F.col("NewITSAddr.CLPP_PERF_NAME")
                ).otherwise(F.col("NewITSAddr.CLPP_PERF_NAME"))
            )
        )
    ).alias("PROV_NM"),
    F.when(
        (F.col("NewITSAddr.CLPP_PERF_NAME").isNull()) |
        (F.length(F.col("NewITSAddr.CLPP_PERF_NAME")) == 0),
        F.lit(None)
    ).otherwise(trim(F.col("NewITSAddr.CLPP_PERF_NAME"))).alias("SVC_PROV_NM"),
)

# --------------------------------------------------------------------------------
# Stage 8: hf_clm_extrn_prov_newITS (CHashedFileStage, Scenario C)
# Write the above output and then read it 
# Output => "new"
# --------------------------------------------------------------------------------
hf_clm_extrn_prov_newITS_schema = StructType([
    StructField("CLCL_ID", StringType(), nullable=False),
    StructField("CLCL_CL_SUB_TYPE", StringType(), nullable=False),
    StructField("CLPP_BCBS_PR_ID", StringType(), nullable=False),
    StructField("CLPP_BCBS_PR_NPI", StringType(), nullable=False),
    StructField("CLPP_CNTRY_CD", StringType(), nullable=False),
    StructField("CLPP_FED_TAX_ID", StringType(), nullable=False),
    StructField("CLPP_PERF_ID", StringType(), nullable=False),
    StructField("CLPP_PERF_ID_NPI", StringType(), nullable=False),
    StructField("CLPP_PHONE", StringType(), nullable=False),
    StructField("CLPP_PR_ADDR1", StringType(), nullable=False),
    StructField("CLPP_PR_ADDR2", StringType(), nullable=False),
    StructField("CLPP_PR_CITY", StringType(), nullable=False),
    StructField("CLPP_PR_NAME", StringType(), nullable=False),
    StructField("CLPP_PR_STATE", StringType(), nullable=False),
    StructField("CLPP_ZIP", StringType(), nullable=False),
    StructField("PROV_NM", StringType(), nullable=False),
    StructField("SVC_PROV_NM", StringType(), nullable=False)
])

df_write_hf_clm_extrn_prov_newITS = df_StripField_output.select(
    F.col("CLCL_ID"),
    F.col("CLCL_CL_SUB_TYPE"),
    F.col("CLPP_BCBS_PR_ID"),
    F.col("CLPP_BCBS_PR_NPI"),
    F.col("CLPP_CNTRY_CD"),
    F.col("CLPP_FED_TAX_ID"),
    F.col("CLPP_PERF_ID"),
    F.col("CLPP_PERF_ID_NPI"),
    F.col("CLPP_PHONE"),
    F.col("CLPP_PR_ADDR1"),
    F.col("CLPP_PR_ADDR2"),
    F.col("CLPP_PR_CITY"),
    F.col("CLPP_PR_NAME"),
    F.col("CLPP_PR_STATE"),
    F.col("CLPP_ZIP"),
    F.col("PROV_NM"),
    F.col("SVC_PROV_NM")
)
write_files(
    df_write_hf_clm_extrn_prov_newITS,
    f"{adls_path}/hf_clm_extrn_prov_newITS.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_hf_clm_extrn_prov_newITS = (
    spark.read.schema(hf_clm_extrn_prov_newITS_schema)
    .parquet(f"{adls_path}/hf_clm_extrn_prov_newITS.parquet")
)

df_new = df_hf_clm_extrn_prov_newITS

# --------------------------------------------------------------------------------
# Stage 9: CER_ATAD_ADDRESS (ODBCConnector)
# --------------------------------------------------------------------------------
extract_query_CER_ATAD_ADDRESS = f"""
SELECT CL.CLCL_ID, ADDR.ATAD_NAME,  ADDR.ATAD_ADDR1,  
ADDR.ATAD_ADDR2,  ADDR.ATAD_ADDR3,  ADDR.ATAD_CITY,  ADDR.ATAD_STATE,  ADDR.ATAD_ZIP,  ADDR.ATAD_COUNTY,  
ADDR.ATAD_CTRY_CD,  ADDR.ATAD_PHONE
FROM {facetsOwner}.CER_ATAD_ADDRESS_D ADDR, 
     {facetsOwner}.CER_ATXR_ATTACH_U ATT, 
     {facetsOwner}.CMC_CLCL_CLAIM CL, 
     tempdb..{DriverTable} TMP
WHERE CL.CLCL_ID = TMP.CLM_ID
       and (SUBSTRING(CL.CLCL_ID, 6,1) = 'H' or SUBSTRING(CL.CLCL_ID,6,2) = 'RH'
            or SUBSTRING(CL.CLCL_ID,6,1) = 'K' or SUBSTRING(CL.CLCL_ID,6,1) = 'G')
       and CL.ATXR_SOURCE_ID <> '1753-01-01 00:00:00.000'
       and CL.ATXR_SOURCE_ID = ATT.ATXR_SOURCE_ID
       and ATT.ATXR_DEST_ID = ADDR.ATXR_DEST_ID
       and ADDR.ATSY_ID = 'ATUE'
       and ATT.ATSY_ID = ADDR.ATSY_ID

UNION ALL

SELECT CL.CLCL_ID,  ADDR.ATAD_NAME,  ADDR.ATAD_ADDR1,  
ADDR.ATAD_ADDR2,  ADDR.ATAD_ADDR3,  ADDR.ATAD_CITY,  ADDR.ATAD_STATE,  ADDR.ATAD_ZIP,  ADDR.ATAD_COUNTY,  
ADDR.ATAD_CTRY_CD,  ADDR.ATAD_PHONE
FROM {facetsOwner}.CER_ATAD_ADDRESS_D ADDR, 
     {facetsOwner}.CER_ATXR_ATTACH_U ATT, 
     {facetsOwner}.CMC_CLCL_CLAIM CL, 
     tempdb..{AdjTable} TMP2
WHERE CL.CLCL_ID = (substring(TMP2.CLM_ID,1,10) + '00')
       and (SUBSTRING(CL.CLCL_ID, 6,1) = 'H' or SUBSTRING(CL.CLCL_ID,6,2) = 'RH'
            or SUBSTRING(CL.CLCL_ID,6,1) = 'K' or SUBSTRING(CL.CLCL_ID,6,1) = 'G')
       and CL.ATXR_SOURCE_ID <> '1753-01-01 00:00:00.000'
       and CL.ATXR_SOURCE_ID = ATT.ATXR_SOURCE_ID
       and ATT.ATXR_DEST_ID = ADDR.ATXR_DEST_ID
       and ADDR.ATSY_ID = 'ATUE'
       and ATT.ATSY_ID = ADDR.ATSY_ID
ORDER BY CL.CLCL_ID
"""
df_CER_ATAD_ADDRESS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CER_ATAD_ADDRESS)
    .load()
)

# --------------------------------------------------------------------------------
# Stage 10: hf_clm_extrnl_prov_oldITS_addr (CHashedFileStage, Scenario C)
# Write and read => "OldITSAddr"
# --------------------------------------------------------------------------------
hf_clm_extrnl_prov_oldITS_addr_schema = StructType([
    StructField("CLCL_ID", StringType(), nullable=False),
    StructField("ATAD_NAME", StringType(), nullable=False),
    StructField("ATAD_ADDR1", StringType(), nullable=False),
    StructField("ATAD_ADDR2", StringType(), nullable=False),
    StructField("ATAD_ADDR3", StringType(), nullable=False),
    StructField("ATAD_CITY", StringType(), nullable=False),
    StructField("ATAD_STATE", StringType(), nullable=False),
    StructField("ATAD_ZIP", StringType(), nullable=False),
    StructField("ATAD_COUNTY", StringType(), nullable=False),
    StructField("ATAD_CTRY_CD", StringType(), nullable=False),
    StructField("ATAD_PHONE", StringType(), nullable=False)
])

df_write_hf_clm_extrnl_prov_oldITS_addr = df_CER_ATAD_ADDRESS.select(
    F.col("CLCL_ID"),
    F.col("ATAD_NAME"),
    F.col("ATAD_ADDR1"),
    F.col("ATAD_ADDR2"),
    F.col("ATAD_ADDR3"),
    F.col("ATAD_CITY"),
    F.col("ATAD_STATE"),
    F.col("ATAD_ZIP"),
    F.col("ATAD_COUNTY"),
    F.col("ATAD_CTRY_CD"),
    F.col("ATAD_PHONE")
)
write_files(
    df_write_hf_clm_extrnl_prov_oldITS_addr,
    f"{adls_path}/hf_clm_extrnl_prov_oldITS_addr.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_hf_clm_extrnl_prov_oldITS_addr = (
    spark.read.schema(hf_clm_extrnl_prov_oldITS_addr_schema)
    .parquet(f"{adls_path}/hf_clm_extrnl_prov_oldITS_addr.parquet")
)
df_OldITSAddr = df_hf_clm_extrnl_prov_oldITS_addr

# --------------------------------------------------------------------------------
# Stage 11: Strip (CTransformerStage)
#   PrimaryLink = old_its_output => "old_its"
#   LookupLink = OldITSAddr => left join on ??? 
#
#   The job's JoinConditions array was empty in JSON, so we do a left join
#   on no condition, typically that might be a mismatch, but DataStage suggests
#   it's a valid scenario. In server jobs, that can sometimes represent
#   "all columns from primary plus columns from reference if matched." 
#   BUT a left join with no condition effectively is a cross join. 
#   However, guidelines forbid using cross join for left join. 
#   Usually in a server job, an empty condition might still produce a cross. 
#   We will replicate as best as possible under normal left-join logic. 
#   Because the stage user-coded references to OldITSAddr columns with 
#   "If IsNull(...) then ..." checks, it strongly indicates that 
#   the intention was to match by CLCL_ID as well, but the JSON has no JoinConditions. 
#   We'll interpret the code references:
#   "If IsNull(Convert(...(OldITSAddr.CLCL_ID))) = @FALSE then..." 
#   Possibly it was an oversight in the JSON. 
#   
#   We'll replicate the original DS expressions. We'll assume the intended join 
#   is old_its.CLCL_ID = OldITSAddr.CLCL_ID left, consistent with logic. 
# --------------------------------------------------------------------------------
df_Strip_joined = (
    df_old_its_output.alias("old_its")
    .join(
        df_OldITSAddr.alias("OldITSAddr"),
        F.col("old_its.CLCL_ID") == F.col("OldITSAddr.CLCL_ID"),
        how="left"
    )
)

df_Strip_output = df_Strip_joined.select(
    strip_field(F.col("old_its.CLCL_ID")).alias("CLCL_ID"),
    strip_field(F.col("old_its.CLCL_CL_SUB_TYPE")).alias("CLCL_CL_SUB_TYPE"),
    # BCBS_PR_ID => old_its.CLMI_ITS_SCCF_NO
    F.col("old_its.CLMI_ITS_SCCF_NO").alias("BCBS_PR_ID"),
    # BCBS_PR_NPI => ' '
    F.lit(" ").alias("BCBS_PR_NPI"),
    # CNTRY_CD
    F.when(
        F.col("OldITSAddr.CLCL_ID").isNotNull(),
        strip_field(F.col("OldITSAddr.ATAD_CTRY_CD"))
    ).otherwise(F.lit(" ")).alias("CNTRY_CD"),
    # FED_TAX_ID => ' '
    F.lit(" ").alias("FED_TAX_ID"),
    # PERF_ID => ' '
    F.lit(" ").alias("PERF_ID"),
    # PERF_ID_NPI => ' '
    F.lit(" ").alias("PERF_ID_NPI"),
    # PHONE
    F.when(
        F.col("OldITSAddr.CLCL_ID").isNotNull(),
        strip_field(F.col("OldITSAddr.ATAD_PHONE"))
    ).otherwise(F.lit(None)).alias("PHONE"),
    # PR_ADDR1
    F.when(
        F.col("OldITSAddr.CLCL_ID").isNotNull(),
        strip_field(F.col("OldITSAddr.ATAD_ADDR1"))
    ).otherwise(F.lit(None)).alias("PR_ADDR1"),
    # PR_ADDR2
    F.when(
        F.col("OldITSAddr.CLCL_ID").isNotNull(),
        strip_field(F.col("OldITSAddr.ATAD_ADDR2"))
    ).otherwise(F.lit(None)).alias("PR_ADDR2"),
    # PR_CITY
    F.when(
        F.col("OldITSAddr.CLCL_ID").isNotNull(),
        strip_field(F.col("OldITSAddr.ATAD_CITY"))
    ).otherwise(F.lit(None)).alias("PR_CITY"),
    # PR_NAME
    F.when(
        F.col("OldITSAddr.CLCL_ID").isNotNull(),
        strip_field(F.col("OldITSAddr.ATAD_NAME"))
    ).otherwise(F.lit(None)).alias("PR_NAME"),
    # PR_STATE
    F.when(
        F.col("OldITSAddr.CLCL_ID").isNotNull(),
        strip_field(F.col("OldITSAddr.ATAD_STATE"))
    ).otherwise(F.lit(" ")).alias("PR_STATE"),
    # ZIP
    F.when(
        F.col("OldITSAddr.CLCL_ID").isNotNull(),
        strip_field(F.col("OldITSAddr.ATAD_ZIP"))
    ).otherwise(F.lit(None)).alias("ZIP"),
    # PROV_NM
    F.when(
        F.col("OldITSAddr.CLCL_ID").isNotNull(),
        strip_field(F.col("OldITSAddr.ATAD_NAME"))
    ).otherwise(F.lit(None)).alias("PROV_NM"),
    # SVC_PROV_NM => ' '
    F.lit(" ").alias("SVC_PROV_NM"),
)

# --------------------------------------------------------------------------------
# Stage 12: hf_clm_extrnl_prov_oldITS (CHashedFileStage, Scenario C)
# Write df_Strip_output => read => "old"
# --------------------------------------------------------------------------------
hf_clm_extrnl_prov_oldITS_schema = StructType([
    StructField("CLCL_ID", StringType(), nullable=False),
    StructField("CLCL_CL_SUB_TYPE", StringType(), nullable=False),
    StructField("CLPP_BCBS_PR_ID", StringType(), nullable=False),
    StructField("CLPP_BCBS_PR_NPI", StringType(), nullable=False),
    StructField("CLPP_CNTRY_CD", StringType(), nullable=False),
    StructField("CLPP_FED_TAX_ID", StringType(), nullable=False),
    StructField("CLPP_PERF_ID", StringType(), nullable=False),
    StructField("CLPP_PERF_ID_NPI", StringType(), nullable=False),
    StructField("CLPP_PHONE", StringType(), nullable=False),
    StructField("CLPP_PR_ADDR1", StringType(), nullable=False),
    StructField("CLPP_PR_ADDR2", StringType(), nullable=False),
    StructField("CLPP_PR_CITY", StringType(), nullable=False),
    StructField("CLPP_PR_NAME", StringType(), nullable=False),
    StructField("CLPP_PR_STATE", StringType(), nullable=False),
    StructField("CLPP_ZIP", StringType(), nullable=False),
    StructField("PROV_NM", StringType(), nullable=False),
    StructField("SVC_PROV_NM", StringType(), nullable=False)
])

df_write_hf_clm_extrnl_prov_oldITS = df_Strip_output.select(
    F.col("CLCL_ID"),
    F.col("CLCL_CL_SUB_TYPE"),
    F.col("BCBS_PR_ID").alias("CLPP_BCBS_PR_ID"),
    F.col("BCBS_PR_NPI").alias("CLPP_BCBS_PR_NPI"),
    F.col("CNTRY_CD").alias("CLPP_CNTRY_CD"),
    F.col("FED_TAX_ID").alias("CLPP_FED_TAX_ID"),
    F.col("PERF_ID").alias("CLPP_PERF_ID"),
    F.col("PERF_ID_NPI").alias("CLPP_PERF_ID_NPI"),
    F.col("PHONE").alias("CLPP_PHONE"),
    F.col("PR_ADDR1").alias("CLPP_PR_ADDR1"),
    F.col("PR_ADDR2").alias("CLPP_PR_ADDR2"),
    F.col("PR_CITY").alias("CLPP_PR_CITY"),
    F.col("PR_NAME").alias("CLPP_PR_NAME"),
    F.col("PR_STATE").alias("CLPP_PR_STATE"),
    F.col("ZIP").alias("CLPP_ZIP"),
    F.col("PROV_NM"),
    F.col("SVC_PROV_NM")
)
write_files(
    df_write_hf_clm_extrnl_prov_oldITS,
    f"{adls_path}/hf_clm_extrnl_prov_oldITS.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_hf_clm_extrnl_prov_oldITS = (
    spark.read.schema(hf_clm_extrnl_prov_oldITS_schema)
    .parquet(f"{adls_path}/hf_clm_extrnl_prov_oldITS.parquet")
)

df_old = df_hf_clm_extrnl_prov_oldITS

# --------------------------------------------------------------------------------
# Stage 13: merge_old_new (CCollector)
# InputPins => old, new => Round-Robin => single output => all_out
# We'll simply union them preserving the same columns and order
# --------------------------------------------------------------------------------
df_merge_old_new = df_old.select(
    "CLCL_ID",
    "CLCL_CL_SUB_TYPE",
    "CLPP_BCBS_PR_ID",
    "CLPP_BCBS_PR_NPI",
    "CLPP_CNTRY_CD",
    "CLPP_FED_TAX_ID",
    "CLPP_PERF_ID",
    "CLPP_PERF_ID_NPI",
    "CLPP_PHONE",
    "CLPP_PR_ADDR1",
    "CLPP_PR_ADDR2",
    "CLPP_PR_CITY",
    "CLPP_PR_NAME",
    "CLPP_PR_STATE",
    "CLPP_ZIP",
    "PROV_NM",
    "SVC_PROV_NM"
).unionByName(
    df_new.select(
        "CLCL_ID",
        "CLCL_CL_SUB_TYPE",
        "CLPP_BCBS_PR_ID",
        "CLPP_BCBS_PR_NPI",
        "CLPP_CNTRY_CD",
        "CLPP_FED_TAX_ID",
        "CLPP_PERF_ID",
        "CLPP_PERF_ID_NPI",
        "CLPP_PHONE",
        "CLPP_PR_ADDR1",
        "CLPP_PR_ADDR2",
        "CLPP_PR_CITY",
        "CLPP_PR_NAME",
        "CLPP_PR_STATE",
        "CLPP_ZIP",
        "PROV_NM",
        "SVC_PROV_NM"
    )
)

df_all_out = df_merge_old_new

# --------------------------------------------------------------------------------
# Stage 14: hf_clm_extrn_prov_all_claims (CHashedFileStage, Scenario C)
# Writes => read => and used as input to BusinessRules
# --------------------------------------------------------------------------------
hf_clm_extrn_prov_all_claims_schema = StructType([
    StructField("CLCL_ID", StringType(), nullable=False),
    StructField("CLCL_CL_SUB_TYPE", StringType(), nullable=False),
    StructField("CLPP_BCBS_PR_ID", StringType(), nullable=False),
    StructField("CLPP_BCBS_PR_NPI", StringType(), nullable=False),
    StructField("CLPP_CNTRY_CD", StringType(), nullable=False),
    StructField("CLPP_FED_TAX_ID", StringType(), nullable=False),
    StructField("CLPP_PERF_ID", StringType(), nullable=False),
    StructField("CLPP_PERF_ID_NPI", StringType(), nullable=False),
    StructField("CLPP_PHONE", StringType(), nullable=False),
    StructField("CLPP_PR_ADDR1", StringType(), nullable=False),
    StructField("CLPP_PR_ADDR2", StringType(), nullable=False),
    StructField("CLPP_PR_CITY", StringType(), nullable=False),
    StructField("CLPP_PR_NAME", StringType(), nullable=False),
    StructField("CLPP_PR_STATE", StringType(), nullable=False),
    StructField("CLPP_ZIP", StringType(), nullable=False),
    StructField("PROV_NM", StringType(), nullable=True),
    StructField("SVC_PROV_NM", StringType(), nullable=True)
])

df_write_hf_clm_extrn_prov_all_claims = df_all_out.select(
    F.col("CLCL_ID"),
    F.col("CLCL_CL_SUB_TYPE"),
    F.col("CLPP_BCBS_PR_ID"),
    F.col("CLPP_BCBS_PR_NPI"),
    F.col("CLPP_CNTRY_CD"),
    F.col("CLPP_FED_TAX_ID"),
    F.col("CLPP_PERF_ID"),
    F.col("CLPP_PERF_ID_NPI"),
    F.col("CLPP_PHONE"),
    F.col("CLPP_PR_ADDR1"),
    F.col("CLPP_PR_ADDR2"),
    F.col("CLPP_PR_CITY"),
    F.col("CLPP_PR_NAME"),
    F.col("CLPP_PR_STATE"),
    F.col("CLPP_ZIP"),
    F.col("PROV_NM"),
    F.col("SVC_PROV_NM")
)
write_files(
    df_write_hf_clm_extrn_prov_all_claims,
    f"{adls_path}/hf_clm_extrn_prov_all_claims.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_hf_clm_extrn_prov_all_claims = (
    spark.read.schema(hf_clm_extrn_prov_all_claims_schema)
    .parquet(f"{adls_path}/hf_clm_extrn_prov_all_claims.parquet")
)

# --------------------------------------------------------------------------------
# Stage 15: BusinessRules (CTransformerStage)
# InputPins => Primary => df_hf_clm_extrn_prov_all_claims (Strip)
# Lookup => fcts_reversals, nasco_dup_lkup
# Two output links => Regular, Reversals
# --------------------------------------------------------------------------------
df_BusinessRules_joined = (
    df_hf_clm_extrn_prov_all_claims.alias("Strip")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
        how="left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        how="left"
    )
)

# Stage variable: ClmId = trim(Strip.CLCL_ID)
df_BusinessRules = df_BusinessRules_joined.withColumn(
    "ClmId",
    trim(F.col("Strip.CLCL_ID"))
)

# Constraint for "Regular": IsNull(nasco_dup_lkup.CLM_ID) = @TRUE
df_Regular = df_BusinessRules.filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
# Constraint for "Reversals": (IsNull(fcts_reversals.CLCL_ID)=@FALSE and
#  fcts_reversals.CLCL_CUR_STS in ("89","91","99"))
df_Reversals = df_BusinessRules.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    (
        (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
        (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
        (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
    )
)

df_Regular_output = df_Regular.select(
    F.lit("0").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit("0").alias("ERR_CT"),
    F.lit("0").alias("RECYCLE_CT"),
    F.lit("LUMERIS").alias("SRC_SYS_CD"),
    F.concat(F.lit("LUMERIS"),F.lit(";"),F.col("ClmId")).alias("PRI_KEY_STRING"),
    F.lit("0").alias("CLM_EXTRNL_PROV_SK"),
    F.lit("0").alias("SRC_SYS_CD_SK"),
    F.col("ClmId").alias("CLM_ID"),
    F.lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("0").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("0").alias("CLM_SK"),
    F.col("Strip.PROV_NM").alias("PROV_NM"),
    F.when(
        (F.col("Strip.CLPP_PR_ADDR1").isNull()) | (F.length(F.col("Strip.CLPP_PR_ADDR1"))==0),
        F.lit(None)
    ).otherwise(F.col("Strip.CLPP_PR_ADDR1")).alias("ADDR_LN_1"),
    F.when(
        (F.col("Strip.CLPP_PR_ADDR2").isNull()) | (F.length(F.col("Strip.CLPP_PR_ADDR2"))==0),
        F.lit(None)
    ).otherwise(F.col("Strip.CLPP_PR_ADDR2")).alias("ADDR_LN_2"),
    F.lit(None).alias("ADDR_LN_3"),
    F.when(
        (F.col("Strip.CLPP_PR_CITY").isNull()) | (F.length(F.col("Strip.CLPP_PR_CITY"))==0),
        F.lit(" ")
    ).otherwise(F.col("Strip.CLPP_PR_CITY")).alias("CITY_NM"),
    F.when(
        (F.trim(F.col("Strip.CLPP_PR_STATE")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_PR_STATE")))==0),
        F.lit("NA")
    ).otherwise(F.trim(F.col("Strip.CLPP_PR_STATE"))).alias("CLPP_PR_STATE"),
    F.lit("0").alias("CLM_EXTRNL_PROV_ST_CD_SK"),
    F.when(
        (F.trim(F.col("Strip.CLPP_ZIP")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_ZIP")))==0),
        F.lit(" ")
    ).otherwise(F.trim(F.col("Strip.CLPP_ZIP"))).alias("POSTAL_CD"),
    F.lit(None).alias("CNTY_NM"),
    F.when(
        (F.trim(F.col("Strip.CLPP_CNTRY_CD")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_CNTRY_CD")))==0),
        F.lit("NA")
    ).otherwise(F.trim(F.col("Strip.CLPP_CNTRY_CD"))).alias("CLPP_CNTRY_CD"),
    F.lit("0").alias("CLM_EXTRNL_PROV_CTRY_CD_SK"),
    F.when(
        (F.trim(F.col("Strip.CLPP_PHONE")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_PHONE")))==0),
        F.lit(None)
    ).otherwise(F.trim(F.col("Strip.CLPP_PHONE"))).alias("PHN_NO"),
    F.when(F.length(F.trim(F.col("Strip.CLPP_BCBS_PR_ID")))==0, F.lit(" ")).otherwise(F.col("Strip.CLPP_BCBS_PR_ID")).alias("PROV_ID"),
    F.when(F.length(F.trim(F.col("Strip.CLPP_BCBS_PR_NPI")))==0, F.lit(" ")).otherwise(F.trim(F.col("Strip.CLPP_BCBS_PR_NPI"))).alias("PROV_NPI"),
    F.when(F.length(F.trim(F.col("Strip.CLPP_PERF_ID")))==0, F.lit(" ")).otherwise(F.trim(F.col("Strip.CLPP_PERF_ID"))).alias("SVC_PROV_ID"),
    F.when(F.length(F.trim(F.col("Strip.CLPP_PERF_ID_NPI")))==0, F.lit(" ")).otherwise(F.trim(F.col("Strip.CLPP_PERF_ID_NPI"))).alias("SVC_PROV_NPI"),
    F.col("Strip.SVC_PROV_NM").alias("SVC_PROV_NM"),
    F.when(F.length(F.trim(F.col("Strip.CLPP_FED_TAX_ID")))==0, F.lit(" ")).otherwise(F.trim(F.col("Strip.CLPP_FED_TAX_ID"))).alias("TAX_ID")
)

df_Reversals_output = df_Reversals.select(
    F.lit("0").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit("0").alias("ERR_CT"),
    F.lit("0").alias("RECYCLE_CT"),
    F.lit("LUMERIS").alias("SRC_SYS_CD"),
    F.concat(F.lit("LUMERIS"),F.lit(";"),F.col("ClmId"),F.lit("R")).alias("PRI_KEY_STRING"),
    F.lit("0").alias("CLM_EXTRNL_PROV_SK"),
    F.lit("0").alias("SRC_SYS_CD_SK"),
    F.concat(F.col("ClmId"),F.lit("R")).alias("CLM_ID"),
    F.lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("0").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("0").alias("CLM_SK"),
    F.col("Strip.PROV_NM").alias("PROV_NM"),
    F.when(
        (F.trim(F.col("Strip.CLPP_PR_ADDR1")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_PR_ADDR1")))==0),
        F.lit(None)
    ).otherwise(F.trim(F.col("Strip.CLPP_PR_ADDR1"))).alias("ADDR_LN_1"),
    F.when(
        (F.trim(F.col("Strip.CLPP_PR_ADDR2")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_PR_ADDR2")))==0),
        F.lit(None)
    ).otherwise(F.trim(F.col("Strip.CLPP_PR_ADDR2"))).alias("ADDR_LN_2"),
    F.lit(None).alias("ADDR_LN_3"),
    F.when(
        (F.trim(F.col("Strip.CLPP_PR_CITY")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_PR_CITY")))==0),
        F.lit(" ")
    ).otherwise(F.trim(F.col("Strip.CLPP_PR_CITY"))).alias("CITY_NM"),
    F.when(
        (F.trim(F.col("Strip.CLPP_PR_STATE")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_PR_STATE")))==0),
        F.lit("NA")
    ).otherwise(F.trim(F.col("Strip.CLPP_PR_STATE"))).alias("CLPP_PR_STATE"),
    F.lit("0").alias("CLM_EXTRNL_PROV_ST_CD_SK"),
    F.when(
        (F.trim(F.col("Strip.CLPP_ZIP")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_ZIP")))==0),
        F.lit(" ")
    ).otherwise(F.trim(F.col("Strip.CLPP_ZIP"))).alias("POSTAL_CD"),
    F.lit(None).alias("CNTY_NM"),
    F.when(
        (F.trim(F.col("Strip.CLPP_CNTRY_CD")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_CNTRY_CD")))==0),
        F.lit("NA")
    ).otherwise(F.trim(F.col("Strip.CLPP_CNTRY_CD"))).alias("CLPP_CNTRY_CD"),
    F.lit("0").alias("CLM_EXTRNL_PROV_CTRY_CD_SK"),
    F.when(
        (F.trim(F.col("Strip.CLPP_PHONE")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_PHONE")))==0),
        F.lit(None)
    ).otherwise(F.trim(F.col("Strip.CLPP_PHONE"))).alias("PHN_NO"),
    F.when(F.length(F.trim(F.col("Strip.CLPP_BCBS_PR_ID")))==0, F.lit(" ")).otherwise(F.col("Strip.CLPP_BCBS_PR_ID")).alias("PROV_ID"),
    F.when(F.length(F.trim(F.col("Strip.CLPP_BCBS_PR_NPI")))==0, F.lit(" ")).otherwise(F.trim(F.col("Strip.CLPP_BCBS_PR_NPI"))).alias("PROV_NPI"),
    F.when(F.length(F.trim(F.col("Strip.CLPP_PERF_ID")))==0, F.lit(" ")).otherwise(F.trim(F.col("Strip.CLPP_PERF_ID"))).alias("SVC_PROV_ID"),
    F.when(F.length(F.trim(F.col("Strip.CLPP_PERF_ID_NPI")))==0, F.lit(" ")).otherwise(F.trim(F.col("Strip.CLPP_PERF_ID_NPI"))).alias("SVC_PROV_NPI"),
    F.col("Strip.SVC_PROV_NM").alias("SVC_PROV_NM"),
    F.when(F.length(F.trim(F.col("Strip.CLPP_FED_TAX_ID")))==0, F.lit(" ")).otherwise(F.trim(F.col("Strip.CLPP_FED_TAX_ID"))).alias("TAX_ID")
)

# --------------------------------------------------------------------------------
# Stage 16: Collector (CCollector)
# Input => Reversals, Regular => union
# Output => Transform => to "Snapshot" 
# --------------------------------------------------------------------------------
df_Collector_union = df_Reversals_output.unionByName(df_Regular_output)

# --------------------------------------------------------------------------------
# Stage 17: Snapshot (CTransformerStage)
# PrimaryLink => df_Collector_union => output => Pkey => "ClmExtrnlProvPK"
# --------------------------------------------------------------------------------
df_Snapshot = df_Collector_union.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_EXTRNL_PROV_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "PROV_NM",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "ADDR_LN_3",
    "CITY_NM",
    "CLPP_PR_STATE",
    "CLM_EXTRNL_PROV_ST_CD_SK",
    "POSTAL_CD",
    "CNTY_NM",
    "CLPP_CNTRY_CD",
    "CLM_EXTRNL_PROV_CTRY_CD_SK",
    "PHN_NO",
    "PROV_ID",
    "PROV_NPI",
    "SVC_PROV_ID",
    "SVC_PROV_NPI",
    "SVC_PROV_NM",
    "TAX_ID"
)

# --------------------------------------------------------------------------------
# Stage 18: ClmExtrnlProvPK (CContainerStage)
# Call the shared container
# --------------------------------------------------------------------------------
params_ClmExtrnlProvPK = {
    "CurrRunCycle": CurrRunCycle
}
df_ClmExtrnlProvPK_output = ClmExtrnlProvPK(df_Snapshot, params_ClmExtrnlProvPK)

# --------------------------------------------------------------------------------
# Stage 19: IdsClmExtrnlProvPkey (CSeqFileStage)
# Write the final file => LhoFctsClmExtrnlProvExtr.LhoFctsClmExtrnlProv.dat.#RunID#
# in directory "key" => use adls_path => no header => comma-delimited => overwrite
# We must rpad all char or varchar columns to their declared lengths, if known.
# The schema from that stage is:
#  1 JOB_EXCTN_RCRD_ERR_SK (no known char length => no rpad)
#  2 INSRT_UPDT_CD char(10)
#  3 DISCARD_IN char(1)
#  4 PASS_THRU_IN char(1)
#  5 FIRST_RECYC_DT (no rpad)
#  6 ERR_CT (no rpad)
#  7 RECYCLE_CT (no rpad)
#  8 SRC_SYS_CD (no known length => treat as char or varchar? If unknown, assume no length => no rpad)
#  9 PRI_KEY_STRING (no known length => no rpad)
# 10 CLM_EXTRNL_PROV_SK (no rpad)
# 11 SRC_SYS_CD_SK (no rpad)
# 12 CLM_ID char(18)
# 13 CRT_RUN_CYC_EXCTN_SK (no rpad)
# 14 LAST_UPDT_RUN_CYC_EXCTN_SK (no rpad)
# 15 CLM_SK (no rpad)
# 16 PROV_NM (varchar => must rpad? The specification says if it is "char" or "varchar" we rpad. 
#    We do not have a declared length, so no official length to rpad to. We'll skip if no length known.)
# 17 ADDR_LN_1 (no length info => skip rpad since unknown)
# 18 ADDR_LN_2 (same)
# 19 ADDR_LN_3 (same)
# 20 CITY_NM (same)
# 21 CLPP_PR_STATE char(2)
# 22 CLM_EXTRNL_PROV_ST_CD_SK (no rpad)
# 23 POSTAL_CD char(11)
# 24 CNTY_NM (no length => no rpad)
# 25 CLPP_CNTRY_CD char(4)
# 26 CLM_EXTRNL_PROv_CTRY_CD_SK (no rpad)
# 27 PHN_NO char(20)
# 28 PROV_ID (no length => skip)
# 29 PROV_NPI (no length => skip)
# 30 SVC_PROV_ID char(13)
# 31 SVC_PROV_NPI (no length => skip)
# 32 SVC_PROV_NM (no length => skip)
# 33 TAX_ID char(9)
#
# We'll rpad for the ones that do have a specified length from the JSON: 
#   INSRT_UPDT_CD (10), 
#   DISCARD_IN (1), 
#   PASS_THRU_IN (1), 
#   CLM_ID (18), 
#   CLPP_PR_STATE (2), 
#   POSTAL_CD (11), 
#   CLPP_CNTRY_CD (4), 
#   PHN_NO (20), 
#   SVC_PROV_ID (13), 
#   TAX_ID (9).
# --------------------------------------------------------------------------------
df_final = df_ClmExtrnlProvPK_output.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_EXTRNL_PROV_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK"),
    F.col("PROV_NM"),
    F.col("ADDR_LN_1"),
    F.col("ADDR_LN_2"),
    F.col("ADDR_LN_3"),
    F.col("CITY_NM"),
    F.rpad(F.col("CLPP_PR_STATE"), 2, " ").alias("CLPP_PR_STATE"),
    F.col("CLM_EXTRNL_PROV_ST_CD_SK"),
    F.rpad(F.col("POSTAL_CD"), 11, " ").alias("POSTAL_CD"),
    F.col("CNTY_NM"),
    F.rpad(F.col("CLPP_CNTRY_CD"), 4, " ").alias("CLPP_CNTRY_CD"),
    F.col("CLM_EXTRNL_PROV_CTRY_CD_SK"),
    F.rpad(F.col("PHN_NO"), 20, " ").alias("PHN_NO"),
    F.col("PROV_ID"),
    F.col("PROV_NPI"),
    F.rpad(F.col("SVC_PROV_ID"), 13, " ").alias("SVC_PROV_ID"),
    F.col("SVC_PROV_NPI"),
    F.col("SVC_PROV_NM"),
    F.rpad(F.col("TAX_ID"), 9, " ").alias("TAX_ID")
)

# Write the final file
output_file_path = f"{adls_path}/key/LhoFctsClmExtrnlProvExtr.LhoFctsClmExtrnlProv.dat.{RunID}"
write_files(
    df_final,
    output_file_path,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)