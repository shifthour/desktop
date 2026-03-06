# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2006, 2007, 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME;  LhoFctsClmDriverBuild
# MAGIC Called by:  FctsClmPrereqSeq
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
# MAGIC Venkatesh Babu 2020-10-11                         Data Elements Populated in the job
# MAGIC Prabhu ES          2022-03-17    S2S               MSSQL ODBC conn params added  - IntegrateDev5\(9)Ken Bradmon\(9)2022-06-08

# MAGIC Facets claims with status = 91 or status = 89 or claims with an adjusted-to value.  Used in all other claim transform jobs to determine if need to build reversal record.   Used in claim trns job to provide reversal record adjusted-to paid date.
# MAGIC Pulling Facets Claim Data from tempdb table
# MAGIC Loads DRG_GROUPER_VER file
# MAGIC File created in CDS job.
# MAGIC Claim status used to filter error recycle records
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
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

SrcSysCdSK = get_widget_value("SrcSysCdSK","")
DriverTable = get_widget_value("DriverTable","TMP_IDS_CLAIM")
AdjFromTable = get_widget_value("AdjFromTable","TMP_ADJ_FROM")
GrouperTable = get_widget_value("GrouperTable","TMP_GROUPER_VER")
_LhoFacetsStgOwner = get_widget_value("$LhoFacetsStgOwner","$PROJDEF")
_LhoFacetsStgAcct = get_widget_value("$LhoFacetsStgAcct","")
_LhoFacetsStgPW = get_widget_value("$LhoFacetsStgPW","")
_LhoFacetsStgDSN = get_widget_value("$LhoFacetsStgDSN","$PROJDEF")
lhofacetsstg_secret_name = get_widget_value("lhofacetsstg_secret_name","")

jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)

# TMP_IDS_CLAIM (ODBCConnector) - Reading from tempdb..#DriverTable#
extract_query_TMP_IDS_CLAIM = f"SELECT * FROM tempdb..#{DriverTable}#"
df_TMP_IDS_CLAIM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_TMP_IDS_CLAIM)
    .load()
)

# TrnsSK (CTransformerStage)
# lnkClmFctsReversals
df_lnkClmFctsReversals = (
    df_TMP_IDS_CLAIM
    .filter((F.col("CLM_STS") == '91') | (F.col("CLM_STS") == '89'))
    .withColumn("CLCL_ID", trim(F.col("CLM_ID")))
    .withColumn("CLCL_CUR_STS", trim(F.col("CLM_STS")))
    .withColumn("CLCL_PAID_DT", F.col("CLCL_PAID_DT"))
    .withColumn("CLCL_ID_ADJ_TO", trim(F.col("CLCL_ID_ADJ_TO")))
    .withColumn("CLCL_ID_ADJ_FROM", trim(F.col("CLCL_ID_ADJ_FROM")))
    .select("CLCL_ID","CLCL_CUR_STS","CLCL_PAID_DT","CLCL_ID_ADJ_TO","CLCL_ID_ADJ_FROM")
)

# adj_from
df_adj_from = (
    df_TMP_IDS_CLAIM
    .filter(F.length(trim(F.col("CLCL_ID_ADJ_FROM"))) > 0)
    .withColumn("CLCL_ID", trim(F.col("CLCL_ID_ADJ_FROM")))
    .select("CLCL_ID")
)

# adj_to
df_adj_to = (
    df_TMP_IDS_CLAIM
    .filter(F.length(trim(F.col("CLCL_ID_ADJ_TO"))) > 0)
    .withColumn("CLCL_ID", trim(F.col("CLCL_ID_ADJ_TO")))
    .select("CLCL_ID")
)

# Claim_Status
df_Claim_Status = (
    df_TMP_IDS_CLAIM
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn("CLM_ID", F.col("CLM_ID"))
    .withColumn("CLM_STTUS", trim(F.col("CLM_STS")))
    .select("SRC_SYS_CD","CLM_ID","CLM_STTUS")
)

# hf_clm_fcts_reversals (CHashedFileStage), Scenario C => write parquet
df_lnkClmFctsReversals_final = df_lnkClmFctsReversals.select(
    F.rpad(F.col("CLCL_ID"),12," ").alias("CLCL_ID"),
    F.rpad(F.col("CLCL_CUR_STS"),2," ").alias("CLCL_CUR_STS"),
    "CLCL_PAID_DT",
    F.rpad(F.col("CLCL_ID_ADJ_TO"),12," ").alias("CLCL_ID_ADJ_TO"),
    F.rpad(F.col("CLCL_ID_ADJ_FROM"),12," ").alias("CLCL_ID_ADJ_FROM")
)
write_files(
    df_lnkClmFctsReversals_final,
    "hf_clm_fcts_reversals.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# hf_clm_recycle_sttus (CHashedFileStage), Scenario C => write parquet
df_Claim_Status_final = df_Claim_Status.select(
    "SRC_SYS_CD",
    "CLM_ID",
    F.rpad(F.col("CLM_STTUS"),2," ").alias("CLM_STTUS")
)
write_files(
    df_Claim_Status_final,
    "hf_clm_recycle_sttus.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# adjusted_from_to_claim_ids (CCollector) => union of df_adj_from & df_adj_to
df_adjusted = df_adj_from.union(df_adj_to).select(F.col("CLCL_ID").alias("CLM_ID"))

# hf_clm_prereq_adj_clm_id (CHashedFileStage), Scenario A => remove hashed file
# Key columns => "CLM_ID"
df_adjusted_dedup = dedup_sort(df_adjusted, ["CLM_ID"], [])

# tmp_adj_from (ODBCConnector) => Merge into tempdb..#AdjFromTable#
drop_temp_table_sql_tmp_adj_from = "DROP TABLE IF EXISTS tempdb.LhoFctsClmDriverBuild_tmp_adj_from_temp"
execute_dml(drop_temp_table_sql_tmp_adj_from, jdbc_url, jdbc_props)

create_temp_table_sql_tmp_adj_from = """
CREATE TABLE tempdb.LhoFctsClmDriverBuild_tmp_adj_from_temp (
    CLM_ID VARCHAR(255)
)
"""
execute_dml(create_temp_table_sql_tmp_adj_from, jdbc_url, jdbc_props)

(
    df_adjusted_dedup
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "tempdb.LhoFctsClmDriverBuild_tmp_adj_from_temp")
    .mode("append")
    .save()
)

merge_sql_tmp_adj_from = f"""
MERGE tempdb..#{AdjFromTable}# AS T
USING tempdb.LhoFctsClmDriverBuild_tmp_adj_from_temp AS S
ON T.CLM_ID = S.CLM_ID
WHEN MATCHED THEN
  UPDATE SET T.CLM_ID = S.CLM_ID
WHEN NOT MATCHED THEN
  INSERT (CLM_ID) VALUES (S.CLM_ID);
"""
execute_dml(merge_sql_tmp_adj_from, jdbc_url, jdbc_props)

# InFile (CSeqFileStage) reading drg_grouper_ver.dat
schema_InFile = StructType([
    StructField("VER_START_DT", TimestampType(), True),
    StructField("VER_END_DT", TimestampType(), True),
    StructField("GRP_VER", StringType(), True),
    StructField("GRP_TYP", StringType(), True),
    StructField("CD_CLS", StringType(), True)
])

df_InFile = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .option("sep", ",")
    .schema(schema_InFile)
    .csv(f"{adls_path_raw}/landing/drg_grouper_ver.dat")
)

# tmp_ver (ODBCConnector) => Merge into tempdb..#GrouperTable#
drop_temp_table_sql_tmp_ver = "DROP TABLE IF EXISTS tempdb.LhoFctsClmDriverBuild_tmp_ver_temp"
execute_dml(drop_temp_table_sql_tmp_ver, jdbc_url, jdbc_props)

create_temp_table_sql_tmp_ver = """
CREATE TABLE tempdb.LhoFctsClmDriverBuild_tmp_ver_temp (
    VER_START_DT DATETIME,
    VER_END_DT DATETIME,
    GRP_VER VARCHAR(255),
    GRP_TYP VARCHAR(255),
    CD_CLS VARCHAR(255)
)
"""
execute_dml(create_temp_table_sql_tmp_ver, jdbc_url, jdbc_props)

(
    df_InFile
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "tempdb.LhoFctsClmDriverBuild_tmp_ver_temp")
    .mode("append")
    .save()
)

merge_sql_tmp_ver = f"""
MERGE tempdb..#{GrouperTable}# AS T
USING tempdb.LhoFctsClmDriverBuild_tmp_ver_temp AS S
ON T.VER_START_DT = S.VER_START_DT
AND T.VER_END_DT = S.VER_END_DT
WHEN MATCHED THEN
  UPDATE SET
    T.GRP_VER = S.GRP_VER,
    T.GRP_TYP = S.GRP_TYP,
    T.CD_CLS = S.CD_CLS
WHEN NOT MATCHED THEN
  INSERT (VER_START_DT, VER_END_DT, GRP_VER, GRP_TYP, CD_CLS)
  VALUES (S.VER_START_DT, S.VER_END_DT, S.GRP_VER, S.GRP_TYP, S.CD_CLS);
"""
execute_dml(merge_sql_tmp_ver, jdbc_url, jdbc_props)