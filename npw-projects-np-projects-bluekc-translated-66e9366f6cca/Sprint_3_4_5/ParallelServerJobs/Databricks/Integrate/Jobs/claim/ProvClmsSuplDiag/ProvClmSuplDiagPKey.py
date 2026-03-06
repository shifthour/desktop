# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING: Generates Primary Key for K_CLM_SUPLMT_DIAG  table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC ========================================================================================================================================
# MAGIC 									                         DATASTAGE      	CODE		DATE OF
# MAGIC DEVELOPER	                         DATE	     USER STORY	     DESCRIPTION                         ENVIRONMENT	REVIEWER	REVIEW
# MAGIC ======================================================================================================================================
# MAGIC Veerendra Punati                        2021-02-24                                             Original Programming                   IntegrateDev2	Abhiram Dasarathy	2021-03-03

# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Left Join on Natural Keys
# MAGIC Transformed Data will land into a a sequential file for the Loading job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC Inner Join primary key info with table info
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
SrcSysCd = get_widget_value('SrcSysCd','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Obtain JDBC configuration for IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# ----------------------------------------------------------------------------------------------------
# Stage: db2_k_clm_supl_diag (DB2ConnectorPX) - Read from IDS database table K_CLM_SUPLMT_DIAG
# ----------------------------------------------------------------------------------------------------
extract_query_db2_k_clm_supl_diag = (
    f"SELECT "
    f"  MBR_UNIQ_KEY, "
    f"  NTNL_PROV_ID, "
    f"  DIAG_CD, "
    f"  DIAG_CD_TYP_CD, "
    f"  SRC_SYS_CD, "
    f"  CLM_SVC_STRT_DT, "
    f"  CRT_RUN_CYC_EXCTN_SK, "
    f"  CLM_SUPLMT_DIAG_SK "
    f"FROM {IDSOwner}.K_CLM_SUPLMT_DIAG"
)
df_db2_k_clm_supl_diag = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_k_clm_supl_diag)
    .load()
)

# ----------------------------------------------------------------------------------------------------
# Stage: ProvClmSuppDiag_Extr (PxDataSet) - Read from .ds (translate to Parquet)
# ----------------------------------------------------------------------------------------------------
df_ProvClmSuppDiag_Extr = spark.read.parquet(
    f"{adls_path}/ds/Clm_Supp_Diag.{SrcSysCd}.xfrm.{RunID}.parquet"
).select(
    "MBR_UNIQ_KEY",
    "NTNL_PROV_ID",
    "CLM_SVC_STRT_DT",
    "DIAG_CD",
    "DIAG_CD_TYP_CD",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SRC_SYS_ORIG_CLM_SK",
    "DIAG_CD_SK",
    "MBR_SK",
    "PROV_SK",
    "CLM_DIAG_ORDNL_CD_SK",
    "CLM_PATN_ACCT_NO",
    "CLM_SRC_SYS_ORIG_CLM_ID"
)

# ----------------------------------------------------------------------------------------------------
# Stage: xfm_key_cols (CTransformerStage) - Split into two output links
# ----------------------------------------------------------------------------------------------------
df_Key_Cols = df_ProvClmSuppDiag_Extr.select(
    "MBR_UNIQ_KEY",
    "NTNL_PROV_ID",
    "CLM_SVC_STRT_DT",
    "DIAG_CD",
    "DIAG_CD_TYP_CD",
    "SRC_SYS_CD",
    "CLM_SRC_SYS_ORIG_CLM_SK",
    "DIAG_CD_SK",
    "MBR_SK",
    "PROV_SK",
    "CLM_DIAG_ORDNL_CD_SK",
    "CLM_PATN_ACCT_NO",
    "CLM_SRC_SYS_ORIG_CLM_ID"
)

df_lnk_xfm_out = df_ProvClmSuppDiag_Extr.select(
    "MBR_UNIQ_KEY",
    "NTNL_PROV_ID",
    "CLM_SVC_STRT_DT",
    "DIAG_CD",
    "DIAG_CD_TYP_CD",
    "SRC_SYS_CD"
)

# ----------------------------------------------------------------------------------------------------
# Stage: rdup_Natural_kys (PxRemDup) - Deduplicate on specified key columns
# ----------------------------------------------------------------------------------------------------
df_to_Join_Keys = dedup_sort(
    df_lnk_xfm_out,
    ["MBR_UNIQ_KEY","NTNL_PROV_ID","CLM_SVC_STRT_DT","DIAG_CD","DIAG_CD_TYP_CD","SRC_SYS_CD"],
    []
)

# ----------------------------------------------------------------------------------------------------
# Stage: jn_keys (PxJoin) - Left join df_to_Join_Keys with df_db2_k_clm_supl_diag
# ----------------------------------------------------------------------------------------------------
df_jn_keys_pre = df_to_Join_Keys.alias("to_Join_Keys").join(
    df_db2_k_clm_supl_diag.alias("K_CLM_SUPL_DIAG"),
    (
        (col("to_Join_Keys.MBR_UNIQ_KEY") == col("K_CLM_SUPL_DIAG.MBR_UNIQ_KEY")) &
        (col("to_Join_Keys.NTNL_PROV_ID") == col("K_CLM_SUPL_DIAG.NTNL_PROV_ID")) &
        (col("to_Join_Keys.CLM_SVC_STRT_DT") == col("K_CLM_SUPL_DIAG.CLM_SVC_STRT_DT")) &
        (col("to_Join_Keys.DIAG_CD") == col("K_CLM_SUPL_DIAG.DIAG_CD")) &
        (col("to_Join_Keys.DIAG_CD_TYP_CD") == col("K_CLM_SUPL_DIAG.DIAG_CD_TYP_CD")) &
        (col("to_Join_Keys.SRC_SYS_CD") == col("K_CLM_SUPL_DIAG.SRC_SYS_CD"))
    ),
    "left"
)

df_jn_keys = df_jn_keys_pre.select(
    col("to_Join_Keys.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("to_Join_Keys.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("to_Join_Keys.CLM_SVC_STRT_DT").alias("CLM_SVC_STRT_DT"),
    col("to_Join_Keys.DIAG_CD").alias("DIAG_CD"),
    col("to_Join_Keys.DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD"),
    col("to_Join_Keys.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("K_CLM_SUPL_DIAG.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("K_CLM_SUPL_DIAG.CLM_SUPLMT_DIAG_SK").alias("CLM_SUPLMT_DIAG_SK")
)

# ----------------------------------------------------------------------------------------------------
# Stage: xfrm_PKEY_gen (CTransformerStage) - Derive new or reuse existing Surrogate Key
# ----------------------------------------------------------------------------------------------------
df_xfrm_PKEY_gen_0 = (
    df_jn_keys
    .withColumn("CLM_SUPLMT_DIAG_SK_old", when(col("CLM_SUPLMT_DIAG_SK").isNotNull(), col("CLM_SUPLMT_DIAG_SK")).otherwise(lit(0)))
    .withColumn("CRT_RUN_CYC_EXCTN_SK_old", when(col("CRT_RUN_CYC_EXCTN_SK").isNotNull(), col("CRT_RUN_CYC_EXCTN_SK")).otherwise(lit(0)))
    # Remove or rename the original columns so we can generate new surrogate keys
    .drop("CLM_SUPLMT_DIAG_SK")
    .drop("CRT_RUN_CYC_EXCTN_SK")
)

# Prepare column to hold newly generated surrogate keys
df_xfrm_PKEY_gen_1 = df_xfrm_PKEY_gen_0.withColumn("CLM_SUPLMT_DIAG_SK", lit(None).cast("long"))

# We call SurrogateKeyGen for CLM_SUPLMT_DIAG_SK
df_enriched = SurrogateKeyGen(
    df_xfrm_PKEY_gen_1,
    <DB sequence name>,
    "CLM_SUPLMT_DIAG_SK",
    <schema>,
    <secret_name>
)

# Apply the conditional logic for partial usage of the new surrogate
df_enriched = (
    df_enriched
    .withColumn(
        "CLM_SUPLMT_DIAG_SK",
        when((col("CLM_SUPLMT_DIAG_SK_old") == 0), col("CLM_SUPLMT_DIAG_SK")).otherwise(col("CLM_SUPLMT_DIAG_SK_old"))
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        when((col("CRT_RUN_CYC_EXCTN_SK_old") == 0), lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK_old"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
)

# Create the DataFrame for lnk_Pkeyout
df_lnk_Pkeyout = df_enriched.select(
    col("MBR_UNIQ_KEY"),
    col("NTNL_PROV_ID"),
    col("DIAG_CD"),
    col("DIAG_CD_TYP_CD"),
    col("SRC_SYS_CD"),
    col("CLM_SVC_STRT_DT"),
    col("CLM_SUPLMT_DIAG_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Create the DataFrame for lnk_K_table_reload (constraint: old CLM_SUPLMT_DIAG_SK was 0)
df_lnk_K_table_reload = (
    df_enriched
    .filter((col("CLM_SUPLMT_DIAG_SK_old") == 0))
    .select(
        col("MBR_UNIQ_KEY"),
        col("NTNL_PROV_ID"),
        col("DIAG_CD"),
        col("DIAG_CD_TYP_CD"),
        col("SRC_SYS_CD"),
        col("CLM_SVC_STRT_DT"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("CLM_SUPLMT_DIAG_SK")
    )
)

# ----------------------------------------------------------------------------------------------------
# Stage: db2_K_CLM_SUPL_DIAG_ld (DB2ConnectorPX) - Insert new rows
# Using a MERGE approach with staging table in STAGING schema
# ----------------------------------------------------------------------------------------------------
drop_sql = "DROP TABLE IF EXISTS STAGING.ProvClmSuplDiagPKey_db2_K_CLM_SUPL_DIAG_ld_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

# Because DataStage does not provide explicit column types, use <...> as per instructions
create_sql = (
    "CREATE TABLE STAGING.ProvClmSuplDiagPKey_db2_K_CLM_SUPL_DIAG_ld_temp ("
    " MBR_UNIQ_KEY <...>,"
    " NTNL_PROV_ID <...>,"
    " DIAG_CD <...>,"
    " DIAG_CD_TYP_CD <...>,"
    " SRC_SYS_CD <...>,"
    " CLM_SVC_STRT_DT <...>,"
    " CRT_RUN_CYC_EXCTN_SK <...>,"
    " CLM_SUPLMT_DIAG_SK <...>"
    ")"
)
execute_dml(create_sql, jdbc_url, jdbc_props)

df_lnk_K_table_reload.write.jdbc(
    url=jdbc_url,
    table="STAGING.ProvClmSuplDiagPKey_db2_K_CLM_SUPL_DIAG_ld_temp",
    mode="append",
    properties=jdbc_props
)

merge_sql = (
    f"MERGE {IDSOwner}.K_CLM_SUPLMT_DIAG AS T "
    f"USING STAGING.ProvClmSuplDiagPKey_db2_K_CLM_SUPL_DIAG_ld_temp AS S "
    f"ON (1=0) "
    f"WHEN NOT MATCHED THEN INSERT "
    f"(MBR_UNIQ_KEY, NTNL_PROV_ID, DIAG_CD, DIAG_CD_TYP_CD, SRC_SYS_CD, CLM_SVC_STRT_DT, CRT_RUN_CYC_EXCTN_SK, CLM_SUPLMT_DIAG_SK) "
    f"VALUES "
    f"(S.MBR_UNIQ_KEY, S.NTNL_PROV_ID, S.DIAG_CD, S.DIAG_CD_TYP_CD, S.SRC_SYS_CD, S.CLM_SVC_STRT_DT, S.CRT_RUN_CYC_EXCTN_SK, S.CLM_SUPLMT_DIAG_SK);"
)
execute_dml(merge_sql, jdbc_url, jdbc_props)

# ----------------------------------------------------------------------------------------------------
# Stage: jn_with_key_cols (PxJoin) - Inner join df_Key_Cols with df_lnk_Pkeyout
# ----------------------------------------------------------------------------------------------------
df_jn_with_key_cols_pre = df_Key_Cols.alias("Key_Cols").join(
    df_lnk_Pkeyout.alias("lnk_Pkeyout"),
    (
        (col("Key_Cols.MBR_UNIQ_KEY") == col("lnk_Pkeyout.MBR_UNIQ_KEY")) &
        (col("Key_Cols.NTNL_PROV_ID") == col("lnk_Pkeyout.NTNL_PROV_ID")) &
        (col("Key_Cols.CLM_SVC_STRT_DT") == col("lnk_Pkeyout.CLM_SVC_STRT_DT")) &
        (col("Key_Cols.DIAG_CD") == col("lnk_Pkeyout.DIAG_CD")) &
        (col("Key_Cols.DIAG_CD_TYP_CD") == col("lnk_Pkeyout.DIAG_CD_TYP_CD")) &
        (col("Key_Cols.SRC_SYS_CD") == col("lnk_Pkeyout.SRC_SYS_CD"))
    ),
    "inner"
)

df_jn_with_key_cols = df_jn_with_key_cols_pre.select(
    col("Key_Cols.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("Key_Cols.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("Key_Cols.CLM_SVC_STRT_DT").alias("CLM_SVC_STRT_DT"),
    col("Key_Cols.DIAG_CD").alias("DIAG_CD"),
    col("Key_Cols.DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD"),
    col("Key_Cols.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_Pkeyout.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_Pkeyout.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Key_Cols.CLM_SRC_SYS_ORIG_CLM_SK").alias("CLM_SRC_SYS_ORIG_CLM_SK"),
    col("Key_Cols.DIAG_CD_SK").alias("DIAG_CD_SK"),
    col("Key_Cols.MBR_SK").alias("MBR_SK"),
    col("Key_Cols.PROV_SK").alias("PROV_SK"),
    col("Key_Cols.CLM_DIAG_ORDNL_CD_SK").alias("CLM_DIAG_ORDNL_CD_SK"),
    col("Key_Cols.CLM_PATN_ACCT_NO").alias("CLM_PATN_ACCT_NO"),
    col("Key_Cols.CLM_SRC_SYS_ORIG_CLM_ID").alias("CLM_SRC_SYS_ORIG_CLM_ID"),
    col("lnk_Pkeyout.CLM_SUPLMT_DIAG_SK").alias("CLM_SUPLMT_DIAG_SK")
)

# ----------------------------------------------------------------------------------------------------
# Stage: Load_Prov_Clm_Diag_Supl (PxSequentialFile) - Write to .dat file
# ----------------------------------------------------------------------------------------------------
df_final = df_jn_with_key_cols.select(
    "MBR_UNIQ_KEY",
    "NTNL_PROV_ID",
    "CLM_SVC_STRT_DT",
    "DIAG_CD",
    "DIAG_CD_TYP_CD",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SRC_SYS_ORIG_CLM_SK",
    "DIAG_CD_SK",
    "MBR_SK",
    "PROV_SK",
    "CLM_DIAG_ORDNL_CD_SK",
    "CLM_PATN_ACCT_NO",
    "CLM_SRC_SYS_ORIG_CLM_ID",
    "CLM_SUPLMT_DIAG_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/Clm_Supp_Diag.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)