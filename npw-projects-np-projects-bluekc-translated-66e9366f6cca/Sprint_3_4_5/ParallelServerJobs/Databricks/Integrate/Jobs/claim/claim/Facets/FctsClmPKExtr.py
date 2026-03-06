# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsClmPKExtr
# MAGIC CALLED BY:  FctsClmPrereqSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  This job must be run after FctsClmDriverBuild
# MAGIC          *  UNIX file K_CLM.dat is removed in Before-job to remove previous keys incase of earlier abend.  File writting is set to append.  
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  Included are any adjusted to/from claims that will be foreign keyed later. 
# MAGIC          *  The primary key hash file hf_clm is the output of this job and is used by the following tables for keying
# MAGIC              CLM
# MAGIC              CLM_PCA
# MAGIC              CLM_EXTRNL_REF_DATA
# MAGIC              CLM_EXTRNL_MBRSH
# MAGIC              CLM_EXTRNL_PROV
# MAGIC              FCLTY_CLM
# MAGIC              ITS_CLM
# MAGIC              CLM_REMIT_HIST
# MAGIC              CLM_ALT_PAYE
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)              Development Project\(9)           Code Reviewer\(9)              Date Reviewed       
# MAGIC ============================================================================================================================================================== 
# MAGIC Brent Leland\(9)2008-07-25\(9)3567 Primary Key     Original Programming                                                                  devlIDS                            Steph Goddard                           10/03/2008
# MAGIC                                                                                                  
# MAGIC Manasa Andru         2014-02-19              TFS - 1321               Trimmed the CLCL_ID in the Trns1 andTrns4 stage               IntegrateCurDevl                   Bhoomi Dasari                             2/24/2014
# MAGIC Prabhu ES               2022-02-26              S2S Remediation      MSSQL connection parameters added                                  IntegrateDev5\(9)\(9)Ken Bradmon\(9)2022-06-10

# MAGIC Get SK for primary key on input record
# MAGIC hf_clm hash file used to key tables
# MAGIC CLM
# MAGIC CLM_PCA
# MAGIC CLM_EXTRNL_REF_DATA
# MAGIC CLM_EXTRNL_MBRSH
# MAGIC CLM_EXTRNL_PROV
# MAGIC FCLTY_CLM
# MAGIC ITS_CLM
# MAGIC CLM_REMIT_HIST
# MAGIC CLM_ALT_PAYE
# MAGIC Facets Claim Primary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code and claim ID
# MAGIC Check is adjusted from or to claim was keyed above
# MAGIC Key adj-to and adj-from claim IDs on claims above
# MAGIC Get SK for records with out keys
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLoadPK
# COMMAND ----------

# Retrieve job parameters
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
DriverTable = get_widget_value('DriverTable','')
AdjFromTable = get_widget_value('AdjFromTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CommitPoint = get_widget_value('CommitPoint','')
FacetsOwner = get_widget_value('FacetsOwner','')
IDSOwner = get_widget_value('IDSOwner','')

# In accordance with the rules for $<database>Owner:
facets_secret_name = get_widget_value('facets_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

# ----------------------------------------------------------------------------
# Stage: TMP_ADJ_CLM (ODBCConnector)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_TMP_ADJ_CLM = f"""
SELECT
CLM.CLCL_ID,
CLM.CLCL_CUR_STS,
CLM.CLCL_PAID_DT,
CLM.CLCL_LAST_ACT_DTM
FROM
    tempdb..{AdjFromTable} DRVR,
    {FacetsOwner}.CMC_CLCL_CLAIM CLM
WHERE CLM.CLCL_ID = DRVR.CLM_ID
"""
df_TMP_ADJ_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_TMP_ADJ_CLM)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: Trns4 (CTransformerStage) - creates two output links Radj and Adj
df_TMP_ADJ_CLM_2 = df_TMP_ADJ_CLM.withColumn(
    "svIsRvrsl",
    IS.CLM.REVERSAL(SrcSysCd, F.col("CLCL_CUR_STS"))
)

df_Radj = df_TMP_ADJ_CLM_2.filter(F.col("svIsRvrsl") == True).select(
    F.concat(trim(F.col("CLCL_ID")), F.lit("R")).alias("CLM_ID")
)

df_Adj = df_TMP_ADJ_CLM_2.select(
    trim(F.col("CLCL_ID")).alias("CLM_ID")
)

# ----------------------------------------------------------------------------
# Stage: Collector2 (CCollector) - round-robin collector of "Radj" and "Adj"
df_Collector2 = df_Radj.select("CLM_ID").unionByName(
    df_Adj.select("CLM_ID")
)
df_Adjust = df_Collector2  # Output link "Adjust" to next stage

# ----------------------------------------------------------------------------
# Stage: TMP_IDS_CLAIM (ODBCConnector)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_TMP_IDS_CLAIM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", f"tempdb..{DriverTable}")
    .load()
)

# ----------------------------------------------------------------------------
# Stage: Trans1 (CTransformerStage) - produces two outputs: Rclaim, Claims
df_TMP_IDS_CLAIM_2 = df_TMP_IDS_CLAIM.withColumn(
    "svIsRvrsl",
    IS.CLM.REVERSAL(SrcSysCd, F.col("CLM_STS"))
)

df_Rclaim = df_TMP_IDS_CLAIM_2.filter(F.col("svIsRvrsl") == True).select(
    F.concat(trim(F.col("CLM_ID")), F.lit("R")).alias("CLM_ID")
)

df_Claims = df_TMP_IDS_CLAIM_2.select(
    trim(F.col("CLM_ID")).alias("CLM_ID")
)

# ----------------------------------------------------------------------------
# Stage: Collector (CCollector) - round-robin collector for "Rclaim" & "Claims"
df_Collector = df_Rclaim.select("CLM_ID").unionByName(
    df_Claims.select("CLM_ID")
)
df_All_Clms = df_Collector  # Output link "All_Clms"

# ----------------------------------------------------------------------------
# Stage: Trans2 (CTransformerStage)
df_Trans2 = df_All_Clms.withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSK)).withColumn(
    "CLM_ID", F.col("CLM_ID")
)

# ----------------------------------------------------------------------------
# Stage: ClmLoadPK (CContainerStage)
params_ClmLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}
df_PK_Clms = ClmLoadPK(df_Trans2, params_ClmLoadPK)

# ----------------------------------------------------------------------------
# Stage: hf_clm_pk_lkup (CHashedFileStage) - scenario C => read parquet
df_hf_clm_pk_lkup = spark.read.parquet(f"{adls_path}/hf_clm_pk_lkup.parquet")

# ----------------------------------------------------------------------------
# Stage: Trans4 (CTransformerStage) - left join with reference "Lkup"
df_Adjust_2 = df_Adjust.withColumn("SrcSysCd", F.lit(SrcSysCd))

df_Trans4_join = df_Adjust_2.alias("Adjust").join(
    df_hf_clm_pk_lkup.alias("Lkup"),
    (F.col("Adjust.SrcSysCd") == F.col("Lkup.SRC_SYS_CD"))
    & (F.col("Adjust.CLM_ID") == F.col("Lkup.CLM_ID")),
    how="left"
)

df_NewAdjClms = df_Trans4_join.filter(F.col("Lkup.CLM_SK").isNull()).select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("Adjust.CLM_ID").alias("CLM_ID")
)

# ----------------------------------------------------------------------------
# Stage: ClmLoadPK2 (CContainerStage)
params_ClmLoadPK2 = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}
df_All_Clms_stage2 = ClmLoadPK(df_NewAdjClms, params_ClmLoadPK2)

# ----------------------------------------------------------------------------
# Stage: hf_clm_pk_lkup2 (CHashedFileStage) - scenario C => write parquet
# Columns: SRC_SYS_CD(varchar), CLM_ID(varchar), CRT_RUN_CYC_EXCTN_SK(int), CLM_SK(int)
# Lengths unknown => rpad uses <...> for manual remediation.
df_hf_clm_pk_lkup2 = df_All_Clms_stage2.select(
    rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK").alias("CLM_SK")
)

write_files(
    df_hf_clm_pk_lkup2,
    f"{adls_path}/hf_clm_pk_lkup.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)