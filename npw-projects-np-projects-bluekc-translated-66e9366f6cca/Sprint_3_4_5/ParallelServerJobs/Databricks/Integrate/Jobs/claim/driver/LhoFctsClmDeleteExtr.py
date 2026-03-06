# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2020 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  LhoFctsClmDeleteExtr
# MAGIC CALLED BY:  FctsClmPrereqSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Create list of record to check deleting.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Brent Leland\(9)2008-07-25\(9)3567 Primary Key     Original Programming                                               devlIDS                                   Steph Goddard        10/03/2008
# MAGIC 
# MAGIC 
# MAGIC Emran.Mohammad               2020-10-12   \(9)                                          Brought Up to Standards                                            
# MAGIC 
# MAGIC Prabhu ES                2022-03-17              S2S                          MSSQL ODBC conn params added                       IntegrateDev5\(9)Ken Bradmon\(9)2022-06-08

# MAGIC Pulling Facets Claim Data from tempdb table
# MAGIC DB2 table used to delete claim records in IDS.  Also used in FctsClmExtr to find existing claims first pass indicator.
# MAGIC Hashfile used to delete error recycle records for these claim IDs.
# MAGIC Hash file loaded in FctsClmPKExtr - ClmLoadPK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, when, row_number, sum as sum_, monotonically_increasing_id, rpad, concat
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# PARAMETERS
CommitPoint = get_widget_value('CommitPoint', '')
SrcSysCdSK = get_widget_value('SrcSysCdSK', '')
DriverTable = get_widget_value('DriverTable', '')
LhoFacetsStgAcct = get_widget_value('$LhoFacetsStgAcct', '')
LhoFacetsStgPW = get_widget_value('$LhoFacetsStgPW', '')
LhoFacetsStgOwner = get_widget_value('$LhoFacetsStgOwner', '')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name', '')
SrcSysCd = get_widget_value('SrcSysCd', '')
LhoFacetsStgDSN = get_widget_value('$LhoFacetsStgDSN', '')

# READ Hashed file "hf_fcts_rcrd_del" as parquet (Scenario C)
df_hf_fcts_rcrd_del = spark.read.parquet(f"{adls_path}/hf_fcts_rcrd_del.parquet")

# READ from ODBC "TMP_IDS_CLAIM" (tempdb..#DriverTable#)
jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)
extract_query = f"SELECT * FROM tempdb..{DriverTable}"
df_TMP_IDS_CLAIM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# STAGE: TrnsSK
# Link lnkIdsClmId (no row filter)
df_lnkIdsClmId = df_TMP_IDS_CLAIM.select(
    lit('CLM_ID').alias("SUBJ_NM"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    trim(col("CLM_ID")).alias("CLCL_ID")
)

# Link lnkClmIdsReversals (constraint = CLM_STS in ['91','89'])
df_lnkClmIdsReversals = df_TMP_IDS_CLAIM.filter(
    (col("CLM_STS") == '91') | (col("CLM_STS") == '89')
).select(
    lit('CLM_ID').alias("SUBJ_NM"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    concat(trim(col("CLM_ID")), lit("R")).alias("CLCL_ID")
)

# STAGE: Merge_IDS_claims (CCollector - Round Robin) => union
df_IDS_claims = df_lnkIdsClmId.unionByName(df_lnkClmIdsReversals)

# STAGE: TrnsFormat
# Implement the Stage Variable "CommitCnt" logic
w = Window.orderBy(monotonically_increasing_id())
df_ids_claims_with_row = df_IDS_claims.withColumn("rownum", row_number().over(w))
df_ids_claims_with_trigger = df_ids_claims_with_row.withColumn(
    "commit_trigger",
    when((col("rownum") % lit(CommitPoint).cast("int")) == 0, lit(1)).otherwise(lit(0))
)
w_cumulative = Window.orderBy("rownum").rowsBetween(Window.unboundedPreceding, 0)
df_ids_claims_with_commit = df_ids_claims_with_trigger.withColumn(
    "commitCnt",
    lit(1) + sum_("commit_trigger").over(w_cumulative)
)

# Left Join with df_hf_fcts_rcrd_del (JoinConditions)
df_join = df_ids_claims_with_commit.alias("IDS_claims").join(
    df_hf_fcts_rcrd_del.alias("lnkClmLkup"),
    (
        lit(SrcSysCdSK).cast("int") == col("lnkClmLkup.SRC_SYS_CD_SK")
    ) & (
        col("IDS_claims.CLCL_ID") == col("lnkClmLkup.CLM_ID")
    ),
    "left"
)

# Filter rows where lookup matched
df_matched = df_join.filter(col("lnkClmLkup.CLM_ID").isNotNull())

# Output link lnkDBdelete
df_lnkDBdelete = df_matched.select(
    col("IDS_claims.SUBJ_NM").alias("SUBJ_NM"),
    rpad(col("IDS_claims.CLCL_ID"), 18, " ").alias("KEY_VAL"),
    col("IDS_claims.commitCnt").alias("CMT_GRP_NO")
)

# Output link lnkErrorRecycle
df_lnkErrorRecycle = df_matched.select(
    concat(trim(col("IDS_claims.SRC_SYS_CD")), lit(";"), trim(col("IDS_claims.CLCL_ID"))).alias("PRI_NAT_KEY_STRING")
)

# STAGE: hf_recycle_delete_list (CHashedFileStage => scenario C => write parquet)
write_files(
    df_lnkErrorRecycle.select("PRI_NAT_KEY_STRING"),
    f"{adls_path}/hf_recycle_delete_list.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# STAGE: w_fcts_rcrd_del (CSeqFileStage => write delimited)
write_files(
    df_lnkDBdelete.select("SUBJ_NM", "KEY_VAL", "CMT_GRP_NO"),
    f"{adls_path}/load/W_FCTS_RCRD_DEL.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)