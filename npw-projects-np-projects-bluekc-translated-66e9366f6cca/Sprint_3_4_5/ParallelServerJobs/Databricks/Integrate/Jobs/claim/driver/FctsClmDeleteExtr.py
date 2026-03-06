# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_1 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC 
# MAGIC Â© Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsClmDeleteExtr 
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
# MAGIC Prabhu ES               2022-02-26              S2S Remediation     MSSQL connection parameters added                   IntegrateDev5\(9)Ken Bradmon\(9)2022-06-10

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
from pyspark.sql.functions import col, lit, row_number, when, sum as sum_, monotonically_increasing_id, concat, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CommitPoint = get_widget_value('CommitPoint','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
DriverTable = get_widget_value('DriverTable','TMP_IDS_CLAIM06170820')
FacetsOwner = get_widget_value('$FacetsOwner','$PROJDEF')
facets_secret_name = get_widget_value('facets_secret_name','')
tempdb_secret_name = get_widget_value('tempdb_secret_name','')

jdbc_url, jdbc_props = get_db_config(tempdb_secret_name)
extract_query = f"SELECT * FROM tempdb..{DriverTable}"
df_TMP_IDS_CLAIM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_fcts_rcrd_del = spark.read.parquet(f"{adls_path}/hf_fcts_rcrd_del.parquet")

df_lnkClmIdsReversals = df_TMP_IDS_CLAIM.filter(
    (col("CLM_STS") == '91') | (col("CLM_STS") == '89')
).select(
    lit("CLM_ID").alias("SUBJ_NM"),
    lit("FACETS").alias("SRC_SYS_CD"),
    trim(col("CLM_ID")).alias("CLCL_ID")
)
df_lnkClmIdsReversals = df_lnkClmIdsReversals.withColumn(
    "CLCL_ID",
    col("CLCL_ID") + lit("R")
)

df_lnkIdsClmId = df_TMP_IDS_CLAIM.filter(
    ~((col("CLM_STS") == '91') | (col("CLM_STS") == '89'))
).select(
    lit("CLM_ID").alias("SUBJ_NM"),
    lit("FACETS").alias("SRC_SYS_CD"),
    trim(col("CLM_ID")).alias("CLCL_ID")
)

df_IDS_claims = df_lnkIdsClmId.select("SUBJ_NM", "SRC_SYS_CD", "CLCL_ID").unionByName(
    df_lnkClmIdsReversals.select("SUBJ_NM", "SRC_SYS_CD", "CLCL_ID")
)

w0 = Window.orderBy(monotonically_increasing_id())
df_stagevars = df_IDS_claims.withColumn("_rownum", row_number().over(w0))

# Because CommitPoint is read as a string, cast to integer where needed
commitPointInt = lit(CommitPoint).cast("integer")

w = Window.orderBy("_rownum").rowsBetween(Window.unboundedPreceding, 0)
increments = when((col("_rownum") % commitPointInt) == 0, 1).otherwise(0)
df_stagevars = df_stagevars.withColumn("Commit_increments", increments)
df_stagevars = df_stagevars.withColumn("CommitCnt", lit(1) + sum_("Commit_increments").over(w))

df_joined = df_stagevars.alias("IDS_claims").join(
    df_hf_fcts_rcrd_del.alias("lnkClmLkup"),
    (
        lit(SrcSysCdSK).cast("int") == col("lnkClmLkup.SRC_SYS_CD_SK")
    ) & (
        col("IDS_claims.CLCL_ID") == col("lnkClmLkup.CLM_ID")
    ),
    how="left"
)

df_filtered = df_joined.filter(col("lnkClmLkup.CLM_ID").isNotNull())

df_lnkDBdelete = df_filtered.select(
    col("IDS_claims.SUBJ_NM").alias("SUBJ_NM"),
    col("IDS_claims.CLCL_ID").alias("KEY_VAL"),
    col("IDS_claims.CommitCnt").alias("CMT_GRP_NO")
)
df_lnkDBdelete = df_lnkDBdelete.withColumn("KEY_VAL", rpad(col("KEY_VAL"), 18, " "))

df_lnkErrorRecycle = df_filtered.select(
    concat(
        trim(col("IDS_claims.SRC_SYS_CD")), 
        lit(";"), 
        trim(col("IDS_claims.CLCL_ID"))
    ).alias("PRI_NAT_KEY_STRING")
)

write_files(
    df_lnkErrorRecycle.select("PRI_NAT_KEY_STRING"),
    f"{adls_path}/hf_recycle_delete_list.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_lnkDBdelete.select("SUBJ_NM", "KEY_VAL", "CMT_GRP_NO"),
    f"{adls_path}/load/W_FCTS_RCRD_DEL.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)