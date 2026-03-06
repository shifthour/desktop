# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called by:
# MAGIC                     EdwCoccFExtr4Seq
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                           DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                                ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------             -------------------------------    ------------------------------       --------------------
# MAGIC  Balkarn Gill                 12/27/2013           5114                          generate primary key for PRVCY_TRK_SUM_F                                         EnterpriseWhseDevl     Peter Marshall               1/7/2013

# MAGIC if there is no FKEY needed, just land it to Seq File for the load job follows.
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC 
# MAGIC IMPORTANT: Make sure to change the Database SEQUENCE Name to the corresponding table name.
# MAGIC Job Name: EdwEdwPrvcyTrkSumFPkey
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Shared Container placeholders (none referenced in this job)

# ----------------------------------------------------------------------------
# Retrieve job parameter values
# ----------------------------------------------------------------------------
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# ----------------------------------------------------------------------------
# Stage: db2_K_PRVCY_TRK_SUM_F_Read (DB2ConnectorPX)
# ----------------------------------------------------------------------------
jdbc_url_db2_K_PRVCY_TRK_SUM_F_Read, jdbc_props_db2_K_PRVCY_TRK_SUM_F_Read = get_db_config(edw_secret_name)
extract_query_db2_K_PRVCY_TRK_SUM_F_Read = """
SELECT 
SRC_SYS_CD,
PRVCY_TRK_SUM_TYP_CD,
SUM_DT_SK,
CRT_RUN_CYC_EXCTN_DT_SK,
PRVCY_TRK_SUM_SK,
CRT_RUN_CYC_EXCTN_SK
FROM #$EDWOwner#.K_PRVCY_TRK_SUM_F
"""
df_db2_K_PRVCY_TRK_SUM_F_Read = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_K_PRVCY_TRK_SUM_F_Read)
    .options(**jdbc_props_db2_K_PRVCY_TRK_SUM_F_Read)
    .option("query", extract_query_db2_K_PRVCY_TRK_SUM_F_Read)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: Data_Set_100 (PxDataSet) - reading PRVCY_TRK_SUM_F.ds as parquet
# ----------------------------------------------------------------------------
df_Data_Set_100 = spark.read.parquet(f"{adls_path}/ds/PRVCY_TRK_SUM_F.parquet")

# ----------------------------------------------------------------------------
# Stage: cpy_MultiStreams (PxCopy)
# ----------------------------------------------------------------------------
df_cpy_MultiStreams_in = df_Data_Set_100

# Output pin 1 (lnkFullDataJnIn -> jn_PKEYs)
df_cpy_MultiStreams_lnkFullDataJnIn = df_cpy_MultiStreams_in.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRVCY_TRK_SUM_TYP_CD").alias("PRVCY_TRK_SUM_TYP_CD"),
    F.col("SUM_DT_SK").alias("SUM_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("ACTV_CT").alias("ACTV_CT"),
    F.col("CANC_CT").alias("CANC_CT"),
    F.col("PRVCY_TRK_SUM_TYP_NM").alias("PRVCY_TRK_SUM_TYP_NM"),
    F.col("NEW_CT").alias("NEW_CT"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PRVCY_TRK_SUM_TYP_CD_SK").alias("PRVCY_TRK_SUM_TYP_CD_SK")
)

# Output pin 2 (lnk_CpyOut -> rdp_NaturalKeys)
df_cpy_MultiStreams_lnkCpyOut = df_cpy_MultiStreams_in.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRVCY_TRK_SUM_TYP_CD").alias("PRVCY_TRK_SUM_TYP_CD"),
    F.col("SUM_DT_SK").alias("SUM_DT_SK")
)

# ----------------------------------------------------------------------------
# Stage: rdp_NaturalKeys (PxRemDup)
# ----------------------------------------------------------------------------
df_rdp_NaturalKeys_in = df_cpy_MultiStreams_lnkCpyOut
df_rdp_NaturalKeys_out = dedup_sort(
    df_rdp_NaturalKeys_in,
    ["SRC_SYS_CD","PRVCY_TRK_SUM_TYP_CD","SUM_DT_SK"],
    []
)

# The output pin (lnkRemDupDataOut -> jn_PRVCY_TRK_SUM_F)
# Same columns, no additional expression changes here, so we keep them as-is.
df_rdp_NaturalKeys = df_rdp_NaturalKeys_out

# ----------------------------------------------------------------------------
# Stage: jn_PRVCY_TRK_SUM_F (PxJoin) - leftouterjoin
# ----------------------------------------------------------------------------
df_jn_PRVCY_TRK_SUM_F = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_K_PRVCY_TRK_SUM_F_Read.alias("lnk_KEdwEdwPrvcyTrkSumFPkey_In"),
    (
        (F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("lnk_KEdwEdwPrvcyTrkSumFPkey_In.SRC_SYS_CD")) &
        (F.col("lnkRemDupDataOut.PRVCY_TRK_SUM_TYP_CD") == F.col("lnk_KEdwEdwPrvcyTrkSumFPkey_In.PRVCY_TRK_SUM_TYP_CD")) &
        (F.col("lnkRemDupDataOut.SUM_DT_SK") == F.col("lnk_KEdwEdwPrvcyTrkSumFPkey_In.SUM_DT_SK"))
    ),
    "left"
)

df_jn_PRVCY_TRK_SUM_F_out = df_jn_PRVCY_TRK_SUM_F.select(
    F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkRemDupDataOut.PRVCY_TRK_SUM_TYP_CD").alias("PRVCY_TRK_SUM_TYP_CD"),
    F.col("lnkRemDupDataOut.SUM_DT_SK").alias("SUM_DT_SK"),
    F.col("lnk_KEdwEdwPrvcyTrkSumFPkey_In.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnk_KEdwEdwPrvcyTrkSumFPkey_In.PRVCY_TRK_SUM_SK").alias("PRVCY_TRK_SUM_SK"),
    F.col("lnk_KEdwEdwPrvcyTrkSumFPkey_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

# ----------------------------------------------------------------------------
# Stage: xfm_PKEYgen (CTransformerStage)
#    Logic to replace NextSurrogateKey() with SurrogateKeyGen
#    Two outputs:
#       lnk_KEdwEdwPrvcyTrkSumFPkey_Out (constraint: IsNull(PRVCY_TRK_SUM_SK))
#       lnkPKEYxfmOut (no constraint specified, so pass all rows)
# ----------------------------------------------------------------------------
# 1) Mark which rows had PRVCY_TRK_SUM_SK as null, so we can filter them later
df_xfm_PKEYgen_in = df_jn_PRVCY_TRK_SUM_F_out.withColumn(
    "ORIGINAL_SK_ISNULL",
    F.col("PRVCY_TRK_SUM_SK").isNull()
)

# 2) SurrogateKeyGen on the entire set to fill any null PRVCY_TRK_SUM_SK
df_enriched = df_xfm_PKEYgen_in
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'PRVCY_TRK_SUM_SK',<schema>,<secret_name>)

# 3) lnkPKEYxfmOut -> includes all rows with expression logic:
df_lnkPKEYxfmOut = df_enriched.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRVCY_TRK_SUM_TYP_CD").alias("PRVCY_TRK_SUM_TYP_CD"),
    F.col("SUM_DT_SK").alias("SUM_DT_SK"),
    F.when(F.col("CRT_RUN_CYC_EXCTN_DT_SK").isNull(), F.lit(CurrRunCycleDate))
     .otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK"))
     .alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PRVCY_TRK_SUM_SK").alias("PRVCY_TRK_SUM_SK"),
    F.when(F.col("CRT_RUN_CYC_EXCTN_SK").isNull(), F.lit(CurrRunCycle))
     .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
     .alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("ORIGINAL_SK_ISNULL").alias("ORIGINAL_SK_ISNULL")
)

# 4) lnk_KEdwEdwPrvcyTrkSumFPkey_Out -> those rows originally null:
df_lnk_KEdwEdwPrvcyTrkSumFPkey_Out = df_lnkPKEYxfmOut.filter(
    F.col("ORIGINAL_SK_ISNULL") == True
).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRVCY_TRK_SUM_TYP_CD").alias("PRVCY_TRK_SUM_TYP_CD"),
    F.col("SUM_DT_SK").alias("SUM_DT_SK"),
    F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PRVCY_TRK_SUM_SK").alias("PRVCY_TRK_SUM_SK"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

# ----------------------------------------------------------------------------
# Stage: db2_K_PRVCY_TRK_SUM_F_Load (DB2ConnectorPX) - use merge upsert logic
# ----------------------------------------------------------------------------
df_db2_K_PRVCY_TRK_SUM_F_Load_in = df_lnk_KEdwEdwPrvcyTrkSumFPkey_Out

jdbc_url_db2_K_PRVCY_TRK_SUM_F_Load, jdbc_props_db2_K_PRVCY_TRK_SUM_F_Load = get_db_config(edw_secret_name)

# Create temp table in STAGING schema
temp_table_db2_K_PRVCY_TRK_SUM_F_Load = "STAGING.EdwEdwPrvcyTrkSumFPkey_db2_K_PRVCY_TRK_SUM_F_Load_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_db2_K_PRVCY_TRK_SUM_F_Load}", jdbc_url_db2_K_PRVCY_TRK_SUM_F_Load, jdbc_props_db2_K_PRVCY_TRK_SUM_F_Load)

df_db2_K_PRVCY_TRK_SUM_F_Load_in.write.jdbc(
    url=jdbc_url_db2_K_PRVCY_TRK_SUM_F_Load,
    table=temp_table_db2_K_PRVCY_TRK_SUM_F_Load,
    mode="overwrite",
    properties=jdbc_props_db2_K_PRVCY_TRK_SUM_F_Load
)

merge_sql_db2_K_PRVCY_TRK_SUM_F_Load = f"""
MERGE #${{EDWOwner}}.K_PRVCY_TRK_SUM_F AS T
USING {temp_table_db2_K_PRVCY_TRK_SUM_F_Load} AS S
ON T.PRVCY_TRK_SUM_SK = S.PRVCY_TRK_SUM_SK
WHEN MATCHED THEN 
  UPDATE SET
    T.SRC_SYS_CD = S.SRC_SYS_CD,
    T.PRVCY_TRK_SUM_TYP_CD = S.PRVCY_TRK_SUM_TYP_CD,
    T.SUM_DT_SK = S.SUM_DT_SK,
    T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (
    PRVCY_TRK_SUM_SK,
    SRC_SYS_CD,
    PRVCY_TRK_SUM_TYP_CD,
    SUM_DT_SK,
    CRT_RUN_CYC_EXCTN_DT_SK,
    CRT_RUN_CYC_EXCTN_SK
  )
  VALUES (
    S.PRVCY_TRK_SUM_SK,
    S.SRC_SYS_CD,
    S.PRVCY_TRK_SUM_TYP_CD,
    S.SUM_DT_SK,
    S.CRT_RUN_CYC_EXCTN_DT_SK,
    S.CRT_RUN_CYC_EXCTN_SK
  );
"""
execute_dml(merge_sql_db2_K_PRVCY_TRK_SUM_F_Load, jdbc_url_db2_K_PRVCY_TRK_SUM_F_Load, jdbc_props_db2_K_PRVCY_TRK_SUM_F_Load)

# ----------------------------------------------------------------------------
# Stage: jn_PKEYs (PxJoin) - leftouterjoin
# ----------------------------------------------------------------------------
df_jn_PKEYs = df_cpy_MultiStreams_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    (
        (F.col("lnkFullDataJnIn.SRC_SYS_CD") == F.col("lnkPKEYxfmOut.SRC_SYS_CD")) &
        (F.col("lnkFullDataJnIn.PRVCY_TRK_SUM_TYP_CD") == F.col("lnkPKEYxfmOut.PRVCY_TRK_SUM_TYP_CD")) &
        (F.col("lnkFullDataJnIn.SUM_DT_SK") == F.col("lnkPKEYxfmOut.SUM_DT_SK"))
    ),
    "left"
)

df_jn_PKEYs_out = df_jn_PKEYs.select(
    F.col("lnkPKEYxfmOut.PRVCY_TRK_SUM_SK").alias("PRVCY_TRK_SUM_SK"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkFullDataJnIn.PRVCY_TRK_SUM_TYP_CD").alias("PRVCY_TRK_SUM_TYP_CD"),
    F.col("lnkFullDataJnIn.SUM_DT_SK").alias("SUM_DT_SK"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnkFullDataJnIn.ACTV_CT").alias("ACTV_CT"),
    F.col("lnkFullDataJnIn.CANC_CT").alias("CANC_CT"),
    F.col("lnkFullDataJnIn.PRVCY_TRK_SUM_TYP_NM").alias("PRVCY_TRK_SUM_TYP_NM"),
    F.col("lnkFullDataJnIn.NEW_CT").alias("NEW_CT"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.PRVCY_TRK_SUM_TYP_CD_SK").alias("PRVCY_TRK_SUM_TYP_CD_SK")
)

# ----------------------------------------------------------------------------
# Stage: seq_PRVCY_TRK_SUM_F_PKey (PxSequentialFile)
# ----------------------------------------------------------------------------
# Apply rpad for char(10) columns: SUM_DT_SK, CRT_RUN_CYC_EXCTN_DT_SK, LAST_UPDT_RUN_CYC_EXCTN_DT_SK
# The final select to preserve order and apply rpad for char/varchar fields
df_seq_PRVCY_TRK_SUM_F_PKey = (
    df_jn_PKEYs_out
    .select(
        F.col("PRVCY_TRK_SUM_SK"),
        F.col("SRC_SYS_CD"),
        F.col("PRVCY_TRK_SUM_TYP_CD"),
        F.rpad(F.col("SUM_DT_SK"), 10, " ").alias("SUM_DT_SK"),
        F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("ACTV_CT"),
        F.col("CANC_CT"),
        F.col("PRVCY_TRK_SUM_TYP_NM"),
        F.col("NEW_CT"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PRVCY_TRK_SUM_TYP_CD_SK")
    )
)

write_files(
    df_seq_PRVCY_TRK_SUM_F_PKey,
    f"{adls_path}/load/PRVCY_TRK_SUM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)