# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2021 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : MAInboundClmLandSeq
# MAGIC 
# MAGIC DESCRIPTION:       MA Inbound Adjusted claims update job. Updates the Adj To information on the claims already existing on IDS if their adjusting claims are received
# MAGIC                                
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #          Change Description                                                     Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                --------------------------------              -------------------------------   ----------------------------      
# MAGIC Lokesh K                 2021-11-29         US 404552                Initial programming                                                       IntegrateDev2                     Jeyaprasanna                  2022-02-06
# MAGIC 
# MAGIC Rahul David            2024-10-04         US 629944          Replace hashfile lookup to K_CLM lookup                        IntegrateDev2                     Jeyaprasanna               2024-10-07
# MAGIC                                                                                        for Adjusted Claim ID

# MAGIC Read the Claim Adjust To Update file created from MAInboundClmExtrRvrsl
# MAGIC Update Claim table with adjusted-to information
# MAGIC Get current claim SK from primary key hash file
# MAGIC Get adjusted-to claim SK from K_CLM table
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


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunCycle = get_widget_value('RunCycle','')
InFile_MA_Rvrsl = get_widget_value('InFile_MA_Rvrsl','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')

# --------------------------------------------------------------------------------
# Stage: hf_Clm (CHashedFileStage) - Scenario C
# --------------------------------------------------------------------------------
df_hf_Clm = spark.read.parquet(f"{adls_path}/hf_clm.parquet")

# --------------------------------------------------------------------------------
# Stage: MAInboundClm_AdjTos (CSeqFileStage)
# --------------------------------------------------------------------------------
MAInboundClm_AdjTos_schema = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("ADJ_FROM_CLM_ID", StringType(), True),
    StructField("ADJ_TO_CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), True)
])

df_MAInboundClm_AdjTos = (
    spark.read
    .option("header", True)
    .option("delimiter", ",")
    .option("quote", "000")
    .schema(MAInboundClm_AdjTos_schema)
    .csv(f"{adls_path}/verified/UPDT_CLM_{InFile_MA_Rvrsl}")
)

# --------------------------------------------------------------------------------
# Stage: GetPKey (CTransformerStage)
#   Primary link: AdjClms (df_MAInboundClm_AdjTos)
#   Lookup link: refClm (df_hf_Clm) - left join
# --------------------------------------------------------------------------------
df_joinGetPKey = df_MAInboundClm_AdjTos.alias("AdjClms").join(
    df_hf_Clm.alias("refClm"),
    (F.lit(SrcSysCd) == F.col("refClm.SRC_SYS_CD")) &
    (F.col("AdjClms.ADJ_TO_CLM_ID") == F.col("refClm.CLM_ID")),
    "left"
)

df_Clm_SK = (
    df_joinGetPKey
    .filter(F.col("refClm.CLM_SK").isNotNull())
    .select(
        F.col("AdjClms.CLM_ID").alias("CLM_ID"),
        F.col("AdjClms.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
        F.col("AdjClms.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
        F.col("refClm.CLM_SK").alias("ADJ_TO_CLM_SK"),
        F.col("AdjClms.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID")
    )
)

# --------------------------------------------------------------------------------
# Stage: K_CLM (DB2Connector - IDS)
#   Reads from #$IDSOwner#.K_CLM where SRC_SYS_CD_SK = #SrcSysCdSK#
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT CAST(CLM_ID AS VARCHAR(20)) AS CLM_ID, CLM_SK FROM {IDSOwner}.K_CLM WHERE SRC_SYS_CD_SK = {SrcSysCdSK}"

df_K_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_adjclm (CHashedFileStage) - Scenario A
#   AnyStage -> CHashedFileStage -> AnyStage
#   Replace with dedup on key columns [CLM_ID] before going to next stage
# --------------------------------------------------------------------------------
df_refClmSK = dedup_sort(df_K_CLM, partition_cols=["CLM_ID"], sort_cols=[])

# --------------------------------------------------------------------------------
# Stage: GetFkeys (CTransformerStage)
#   Primary link: Clm_SK (df_Clm_SK)
#   Lookup link: refClmSK (df_refClmSK) - left join
# --------------------------------------------------------------------------------
df_GetFkeysJoin = df_Clm_SK.alias("Clm_SK").join(
    df_refClmSK.alias("refClmSK"),
    F.col("Clm_SK.CLM_ID") == F.col("refClmSK.CLM_ID"),
    "left"
)

df_Transform = (
    df_GetFkeysJoin
    .filter(F.col("refClmSK.CLM_SK").isNotNull())
    .select(
        F.col("refClmSK.CLM_SK").alias("CLM_SK"),
        GetFkeyCodes(
            F.lit(SrcSysCd),
            F.col("refClmSK.CLM_SK"),
            F.lit("CLAIM STATUS"),
            F.lit("A09"),
            F.lit("N")
        ).alias("CLM_STTUS_CD_SK"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Clm_SK.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
        F.col("Clm_SK.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
        F.col("Clm_SK.ADJ_TO_CLM_SK").alias("ADJ_TO_CLM_SK"),
        F.col("Clm_SK.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID")
    )
)

# --------------------------------------------------------------------------------
# Stage: Snapshot (CTransformerStage) -> output: Update -> CLM_Update
# --------------------------------------------------------------------------------
df_Update = (
    df_Transform
    .select(
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("NA").alias("ADJ_FROM_CLM_ID"),
        F.lit(1).alias("ADJ_FROM_CLM_SK"),
        F.col("ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
        F.col("ADJ_TO_CLM_SK").alias("ADJ_TO_CLM_SK"),
        F.col("CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK")
    )
)

# Use rpad on varchar/char columns if needed (unknown lengths replaced with a placeholder)
df_Update_rpad = (
    df_Update
    .withColumn("ADJ_FROM_CLM_ID", F.rpad(F.col("ADJ_FROM_CLM_ID"), 50, " "))
    .withColumn("ADJ_TO_CLM_ID", F.rpad(F.col("ADJ_TO_CLM_ID"), 50, " "))
)

# --------------------------------------------------------------------------------
# Stage: CLM_Update (DB2Connector - IDS)
#   Upsert into #$IDSOwner#.CLM
# --------------------------------------------------------------------------------
temp_table = "STAGING.MAInboundClmExtrRvrslToUpd_CLM_Update_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url, jdbc_props)

df_Update_rpad.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.CLM AS T
USING {temp_table} AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN UPDATE SET
  T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
  T.ADJ_FROM_CLM_ID = S.ADJ_FROM_CLM_ID,
  T.ADJ_FROM_CLM_SK = S.ADJ_FROM_CLM_SK,
  T.ADJ_TO_CLM_ID = S.ADJ_TO_CLM_ID,
  T.ADJ_TO_CLM_SK = S.ADJ_TO_CLM_SK,
  T.CLM_STTUS_CD_SK = S.CLM_STTUS_CD_SK
WHEN NOT MATCHED THEN INSERT
  (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, ADJ_FROM_CLM_ID, ADJ_FROM_CLM_SK, ADJ_TO_CLM_ID, ADJ_TO_CLM_SK, CLM_STTUS_CD_SK)
  VALUES
  (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.ADJ_FROM_CLM_ID, S.ADJ_FROM_CLM_SK, S.ADJ_TO_CLM_ID, S.ADJ_TO_CLM_SK, S.CLM_STTUS_CD_SK);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)