# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2006, 2007, 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsClmDriverBuild
# MAGIC Called by:
# MAGIC                     FctsClmPrereqSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Extracts Facets claims based on last activity date.  Claim IDs for claims that are already in the IDS are saved for the delete process that is done prior to loaded the updated claims.
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    What needs to happen before a special run can occur?
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                    Project/                                                                                                                                 Code                   Date
# MAGIC Developer           Date                Altiris #        Change Description                                                                                           Reviewer            Reviewed
# MAGIC -------------------------  ---------------------   ----------------   -----------------------------------------------------------------------------------------------------------------         -------------------------  -------------------
# MAGIC Oliver Nielsen     03/01/2004-                       Originally Programmed
# MAGIC  SAndrew           10/29/2004-                       Facets Claim Reversal - changed facets extract to include adusted from 
# MAGIC                                                                       claims that had no activity for adjusted to claims that did have activity.
# MAGIC                                                                       Created a new hash file to contain facets claims that are part of an adjusted
# MAGIC                                                                       claim.  hash file to be used in all facets claim transform jobs.
# MAGIC                                                                       Changed the lnkHashOut to create reversal claim ids.  
# MAGIC Brent Leland      11/04/2004                         Moved load of tmp driver table to tmp_claim.ksh script. 
# MAGIC Steph Goddard  02/28/2006                         sequencer changes
# MAGIC BJ Luce             03/27/2006                         Add logic to create hash files needed to process the Nasco 
# MAGIC                                                                       duplicate claims
# MAGIC Sanderw            12/08/2006   1756              Reversal logix added for new status codes 89 and  and 99
# MAGIC Brent Leland      08/15/2007   ProdSupp.     Add claim status hash file - hf_clm_recycle_sttus                                              Steph Goddard    09/26/07
# MAGIC                                                                       Added hf_fcts_rcrd_del for new primary key process
# MAGIC Hugh Sisson      2015-10-21    5526              Added columns GRP_TYP and CD_CLS to the Grouper Version temp table    Bhoomi Dasari     2/3/2016
# MAGIC Prabhu ES        2022-02-26     S2S               MSSQL connection parameters added         \(9)Ken Bradmon\(9)2022-06-10

# MAGIC Lookup if  claims already in IDS
# MAGIC Facets claims with status = 91 or status = 89 or claims with an adjusted-to value.  Used in all other claim transform jobs to determine if need to build reversal record.   Used in claim trns job to provide reversal record adjusted-to paid date.
# MAGIC Pulling Facets Claim Data from tempdb table
# MAGIC Loads DRG_GROUPER_VER file
# MAGIC File created in CDS job.
# MAGIC Nasco_orig hash file - list of the orig Nasco claim and the 'R' claim (Nasco duplicate claim in Facets)
# MAGIC Used in FctsClmFkey
# MAGIC Nasco_dup hash file - list of claims that will have an 'R' row created and theNasco claim it is adjusted from
# MAGIC Used in FctsClmFkey
# MAGIC Pulling Facets Claim Data from tempdb table
# MAGIC nasco_dup_bypass is list of claims that are Nasco Duplicates. Criteria:
# MAGIC Host plan = 740
# MAGIC Group id = 'BLUECARE'
# MAGIC Claim sequence number = '00'
# MAGIC text box 1 in claim attachment = blank
# MAGIC test box 2 in claim attachment not = blank
# MAGIC test box 3 in claim attachment = blank
# MAGIC All claim extract jobs will not build a row for claim if on this hash file
# MAGIC Claim status used to filter error recycle records
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, length, substring, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


SrcSysCdSK = get_widget_value('SrcSysCdSK','')
DriverTable = get_widget_value('DriverTable','')
AdjFromTable = get_widget_value('AdjFromTable','')
GrouperTable = get_widget_value('GrouperTable','')
FacetsOwner = get_widget_value('$FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
temp_secret_name = get_widget_value('temp_secret_name','')

# Read from TMP_IDS_CLAIM (ODBCConnector) - tempdb..#DriverTable#
jdbc_url, jdbc_props = get_db_config(temp_secret_name)
extract_query_TMP_IDS_CLAIM = f"SELECT * FROM tempdb..{DriverTable}"
df_TMP_IDS_CLAIM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_TMP_IDS_CLAIM)
    .load()
)

# Transformer: TrnsSK
df_lnkClms = df_TMP_IDS_CLAIM

df_lnkClmFctsReversals = df_lnkClms.filter(
    (col("CLM_STS") == '91') | (col("CLM_STS") == '89')
).select(
    trim(col("CLM_ID")).alias("CLCL_ID"),
    trim(col("CLM_STS")).alias("CLCL_CUR_STS"),
    col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    trim(col("CLCL_ID_ADJ_TO")).alias("CLCL_ID_ADJ_TO"),
    trim(col("CLCL_ID_ADJ_FROM")).alias("CLCL_ID_ADJ_FROM")
)

df_adj_from = df_lnkClms.filter(
    length(trim(col("CLCL_ID_ADJ_FROM"))) > 0
).select(
    trim(col("CLCL_ID_ADJ_FROM")).alias("CLCL_ID")
)

df_adj_to = df_lnkClms.filter(
    length(trim(col("CLCL_ID_ADJ_TO"))) > 0
).select(
    trim(col("CLCL_ID_ADJ_TO")).alias("CLCL_ID")
)

df_Claim_Status = df_lnkClms.select(
    lit("FACETS").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    trim(col("CLM_STS")).alias("CLM_STTUS")
)

# hf_clm_fcts_reversals (CHashedFileStage) - Scenario C
df_hf_clm_fcts_reversals_final = df_lnkClmFctsReversals.select(
    rpad(col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    rpad(col("CLCL_CUR_STS"), 2, " ").alias("CLCL_CUR_STS"),
    col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    rpad(col("CLCL_ID_ADJ_TO"), 12, " ").alias("CLCL_ID_ADJ_TO"),
    rpad(col("CLCL_ID_ADJ_FROM"), 12, " ").alias("CLCL_ID_ADJ_FROM")
)

write_files(
    df_hf_clm_fcts_reversals_final,
    "hf_clm_fcts_reversals.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# hf_clm_recycle_sttus (CHashedFileStage) - Scenario C
df_hf_clm_recycle_sttus_final = df_Claim_Status.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    rpad(col("CLM_STTUS"), 2, " ").alias("CLM_STTUS")
)

write_files(
    df_hf_clm_recycle_sttus_final,
    "hf_clm_recycle_sttus.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# adjusted_from_to_claim_ids (CCollector) - Round-Robin union
df_adjusted = df_adj_from.unionByName(df_adj_to).withColumnRenamed("CLCL_ID","CLML_ID")  # intermediate rename
df_adjusted = df_adjusted.select(col("CLML_ID").alias("CLM_ID"))

# hf_clm_prereq_adj_clm_id (CHashedFileStage) - Scenario A
# Deduplicate on key=CLM_ID
df_hf_clm_prereq_adj_clm_id = df_adjusted.dropDuplicates(["CLM_ID"])

# tmp_adj_from (ODBCConnector) - write to tempdb..#AdjFromTable# (Truncate+Insert)
df_tmp_adj_from = df_hf_clm_prereq_adj_clm_id.select(
    rpad(col("CLM_ID"), 12, " ").alias("CLM_ID")
)
jdbc_url_tmp_adj_from, jdbc_props_tmp_adj_from = get_db_config(temp_secret_name)
temp_table_tmp_adj_from = "tempdb.FctsClmDriverBuild_tmp_adj_from_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_tmp_adj_from}", jdbc_url_tmp_adj_from, jdbc_props_tmp_adj_from)
df_tmp_adj_from.write.format("jdbc") \
    .option("url", jdbc_url_tmp_adj_from) \
    .options(**jdbc_props_tmp_adj_from) \
    .option("dbtable", temp_table_tmp_adj_from) \
    .mode("append") \
    .save()
execute_dml(f"TRUNCATE TABLE tempdb..{AdjFromTable}", jdbc_url_tmp_adj_from, jdbc_props_tmp_adj_from)
merge_sql_tmp_adj_from = f"""
MERGE tempdb..{AdjFromTable} AS T
USING {temp_table_tmp_adj_from} AS S
ON (T.[CLM_ID] = S.[CLM_ID])
WHEN MATCHED THEN
  UPDATE SET T.[CLM_ID] = S.[CLM_ID]
WHEN NOT MATCHED THEN
  INSERT ([CLM_ID]) VALUES (S.[CLM_ID]);
"""
execute_dml(merge_sql_tmp_adj_from, jdbc_url_tmp_adj_from, jdbc_props_tmp_adj_from)

# InFile (CSeqFileStage) reading drg_grouper_ver.dat
schema_InFile = StructType([
    StructField("VER_START_DT", TimestampType(), nullable=False),
    StructField("VER_END_DT", TimestampType(), nullable=False),
    StructField("GRP_VER", StringType(), nullable=False),
    StructField("GRP_TYP", StringType(), nullable=False),
    StructField("CD_CLS", StringType(), nullable=False)
])
df_InFile = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_InFile)
    .csv(f"{adls_path_raw}/landing/drg_grouper_ver.dat")
)

df_drg_grouper_ver = df_InFile

# tmp_ver (ODBCConnector) - write to tempdb..#GrouperTable# (Truncate+Insert)
df_tmp_ver = df_drg_grouper_ver.select(
    col("VER_START_DT").alias("VER_START_DT"),
    col("VER_END_DT").alias("VER_END_DT"),
    rpad(col("GRP_VER"), 2, " ").alias("GRP_VER"),
    rpad(col("GRP_TYP"), 2, " ").alias("GRP_TYP"),
    rpad(col("CD_CLS"), 2, " ").alias("CD_CLS")
)
jdbc_url_tmp_ver, jdbc_props_tmp_ver = get_db_config(temp_secret_name)
temp_table_tmp_ver = "tempdb.FctsClmDriverBuild_tmp_ver_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_tmp_ver}", jdbc_url_tmp_ver, jdbc_props_tmp_ver)
df_tmp_ver.write.format("jdbc") \
    .option("url", jdbc_url_tmp_ver) \
    .options(**jdbc_props_tmp_ver) \
    .option("dbtable", temp_table_tmp_ver) \
    .mode("append") \
    .save()
execute_dml(f"TRUNCATE TABLE tempdb..{GrouperTable}", jdbc_url_tmp_ver, jdbc_props_tmp_ver)
merge_sql_tmp_ver = f"""
MERGE tempdb..{GrouperTable} AS T
USING {temp_table_tmp_ver} AS S
ON (
    T.[VER_START_DT] = S.[VER_START_DT]
    AND T.[VER_END_DT] = S.[VER_END_DT]
)
WHEN MATCHED THEN
  UPDATE SET
    T.[GRP_VER] = S.[GRP_VER],
    T.[GRP_TYP] = S.[GRP_TYP],
    T.[CD_CLS] = S.[CD_CLS]
WHEN NOT MATCHED THEN
  INSERT ([VER_START_DT],[VER_END_DT],[GRP_VER],[GRP_TYP],[CD_CLS])
  VALUES (S.[VER_START_DT],S.[VER_END_DT],S.[GRP_VER],S.[GRP_TYP],S.[CD_CLS]);
"""
execute_dml(merge_sql_tmp_ver, jdbc_url_tmp_ver, jdbc_props_tmp_ver)

# TMP_ID_CLMS_NASCO_DUPS (ODBCConnector) - big query referencing tempdb..#DriverTable# and $FacetsOwner
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
facets_query = f"""
SELECT 
  TMP.CLM_ID,
  TMP.ATUF_TEXT1,
  TMP.ATUF_TEXT2,
  TMP.ATUF_TEXT3,
  CLM.CLCL_ID_ADJ_TO,
  CLM.CLCL_ID_ADJ_FROM,
  CLM.CLCL_PAID_DT,
  CLM.CLCL_LAST_ACT_DTM
FROM tempdb..{DriverTable} TMP,
     {FacetsOwner}.CMC_CLCL_CLAIM CLM,
     {FacetsOwner}.CMC_GRGR_GROUP GRP,
     {FacetsOwner}.CMC_CLMI_MISC MISC,
     {FacetsOwner}.CER_ATXR_ATTACH_U U,
     {FacetsOwner}.CER_ATUF_USERFLD_D US
WHERE 
  TMP.CLM_ID = CLM.CLCL_ID
  AND CLM.CLCL_ID = MISC.CLCL_ID
  AND MISC.CLMI_HOST_PLAN_CD = '740'
  AND CLM.GRGR_CK = GRP.GRGR_CK
  AND GRP.GRGR_ID = 'BLUECARD'
  AND CLM.ATXR_SOURCE_ID = U.ATXR_SOURCE_ID
  AND U.ATSY_ID = 'HTCN'
  AND U.ATXR_DEST_ID = US.ATXR_DEST_ID
  AND US.ATSY_ID = U.ATSY_ID
"""
df_TMP_ID_CLMS_NASCO_DUPS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", facets_query)
    .load()
)

# Copy_of_TrnsSK (CTransformerStage) - has a stage variable:
svNPSSrcSysCdSk = GetFkeyCodes("IDS", 0, "SOURCE SYSTEM", 'NPS', 'X')

df_Clms = df_TMP_ID_CLMS_NASCO_DUPS

df_Nasco_dup_bypass = df_Clms.filter(
    (col("CLM_ID").substr(11,2) == '00') &
    (length(trim(col("ATUF_TEXT1"))) == 0) &
    (length(trim(col("ATUF_TEXT2"))) > 0) &
    (length(trim(col("ATUF_TEXT3"))) == 0)
).select(
    trim(col("CLM_ID")).alias("CLM_ID")
)

df_Nasco_dup = df_Clms.filter(
    (col("CLM_ID").substr(11,2) == '01') &
    (length(trim(col("ATUF_TEXT1"))) > 0) &
    (length(trim(col("ATUF_TEXT2"))) > 0) &
    (length(trim(col("ATUF_TEXT3"))) == 0)
).select(
    trim(col("CLCL_ID_ADJ_FROM")).alias("CLM_ID"),
    trim(col("ATUF_TEXT1")).alias("NASCO_CLM_ID")
)

df_Nasco_orig = df_Clms.filter(
    (col("CLM_ID").substr(11,2) == '01') &
    (length(trim(col("ATUF_TEXT1"))) > 0) &
    (length(trim(col("ATUF_TEXT2"))) > 0) &
    (length(trim(col("ATUF_TEXT3"))) == 0)
).select(
    trim(col("ATUF_TEXT1")).alias("NASCO_CLM_ID"),
    trim(col("CLCL_ID_ADJ_FROM")).alias("CLM_ID"),
    col("CLCL_LAST_ACT_DTM").substr(1,10).alias("CLCL_LAST_ACT_DTM"),
    lit(svNPSSrcSysCdSk).alias("SRC_SYS_CD_SK")
)

# clm_nasco_dup_hashfiles (CHashedFileStage) - Scenario C (3 outputs => 3 files)
# 1) hf_clm_nasco_dup
write_files(
    df_Nasco_dup,
    "hf_clm_nasco_dup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# 2) hf_clm_nasco_orig
write_files(
    df_Nasco_orig,
    "hf_clm_nasco_orig.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# 3) hf_clm_nasco_dup_bypass
write_files(
    df_Nasco_dup_bypass,
    "hf_clm_nasco_dup_bypass.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)