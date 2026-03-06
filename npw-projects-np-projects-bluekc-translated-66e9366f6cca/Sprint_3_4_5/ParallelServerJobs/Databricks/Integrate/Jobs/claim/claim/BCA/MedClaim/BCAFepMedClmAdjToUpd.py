# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : IdsBCAFEPMedClmLoadAllSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:      BCA FEP Med Claim Adjusted claims update job. Updates the Adj To information on the claims already existing on IDS if their adjusting claims are received
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #          Change Description                                                     Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------          --------------------     ------------------------      -----------------------------------------------------------------------                --------------------------------              -------------------------------   ----------------------------      
# MAGIC Kalyan Neelam      2017-11-08              5781                          Original Programming                                                IntegrateDev2                 Jag Yelavarthi               2017-11-17

# MAGIC Read the Claim Adjust To Update file created from BCAFEPMedClmPreProcExtr job
# MAGIC Update Claim table with adjusted-to information
# MAGIC Get current claim SK from primary key hash file
# MAGIC Validate that claim exists in database
# MAGIC Get adjusted-to claim SK from primary key hash file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunCycle = get_widget_value('RunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# BCAFepMedClm_AdjTos (CSeqFileStage) - Read
df_BCAFepMedClm_AdjTos_schema = StructType([
    StructField("CLM_ID", StringType(), False),
    StructField("ADJ_FROM_CLM_ID", StringType(), False),
    StructField("ADJ_TO_CLM_ID", StringType(), False),
    StructField("CLM_STTUS_CD", StringType(), False)
])
df_BCAFepMedClm_AdjTos = (
    spark.read
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(df_BCAFepMedClm_AdjTos_schema)
    .csv(f"{adls_path}/verified/BCAFEP_MedClm_AdjTos.dat.{RunID}")
)

# hf_Clm (CHashedFileStage) - Read from Parquet
df_hf_Clm_schema = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CLM_SK", IntegerType(), False)
])
df_hf_Clm = (
    spark.read
    .schema(df_hf_Clm_schema)
    .parquet(f"{adls_path}/hf_clm.parquet")
)

# GetPKey (CTransformerStage) - Primary link (df_BCAFepMedClm_AdjTos alias AdjClms), Lookup link (df_hf_Clm alias refClm)
df_GetPKey_join = (
    df_BCAFepMedClm_AdjTos.alias("AdjClms")
    .join(
        df_hf_Clm.alias("refClm"),
        (
            (F.lit("BCA") == F.col("refClm.SRC_SYS_CD")) &
            (trim(F.col("AdjClms.ADJ_FROM_CLM_ID")) == F.col("refClm.CLM_ID"))
        ),
        "left"
    )
)
df_GetPKey_filtered = df_GetPKey_join.filter(F.col("refClm.CLM_SK").isNotNull())
df_GetPKey = df_GetPKey_filtered.select(
    F.col("refClm.CLM_SK").alias("ADJ_FROM_CLM_SK"),
    F.col("AdjClms.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("AdjClms.CLM_ID").alias("CLM_ID"),
    F.col("AdjClms.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("AdjClms.CLM_STTUS_CD").alias("CLM_STTUS_CD")
)

# hf_Clm_2 (CHashedFileStage) - Read from Parquet
df_hf_Clm_2 = (
    spark.read
    .schema(df_hf_Clm_schema)
    .parquet(f"{adls_path}/hf_clm.parquet")
)

# Prepare for Clm_Ref_Link (DB2Connector with CLM_SK=?). Write primary-link data to staging table for join
execute_dml("DROP TABLE IF EXISTS STAGING.BCAFepMedClmAdjToUpd_Clm_Ref_Link_temp", jdbc_url_ids, jdbc_props_ids)
df_GetPKey.select("ADJ_FROM_CLM_SK").write.format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.BCAFepMedClmAdjToUpd_Clm_Ref_Link_temp") \
    .mode("overwrite") \
    .save()

extract_query_Clm_Ref_Link = f"""
SELECT
  CLM.CLM_SK,
  CLM.SRC_SYS_CD_SK,
  CLM.CLM_ID,
  CLM.CRT_RUN_CYC_EXCTN_SK,
  CLM.LAST_UPDT_RUN_CYC_EXCTN_SK,
  CLM.ADJ_FROM_CLM_SK,
  CLM.ADJ_TO_CLM_SK,
  CLM.CLM_STTUS_CD_SK,
  CLM.ADJ_FROM_CLM_ID,
  CLM.ADJ_TO_CLM_ID,
  CD1.TRGT_CD,
  CLM.GRP_SK,
  CLM.MBR_SK,
  CLM.FNCL_LOB_SK,
  CLM.EXPRNC_CAT_SK,
  CLM.PCA_TYP_CD_SK,
  CLM.CLM_CAT_CD_SK,
  CLM.SVC_STRT_DT_SK,
  CLM.CHRG_AMT,
  CLM.PAYBL_AMT,
  CLM.CLM_CT
FROM {IDSOwner}.CLM CLM
JOIN {IDSOwner}.CD_MPPNG CD ON CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
JOIN {IDSOwner}.CD_MPPNG CD1 ON CLM.CLM_STTUS_CD_SK = CD1.CD_MPPNG_SK
JOIN STAGING.BCAFepMedClmAdjToUpd_Clm_Ref_Link_temp stg ON CLM.CLM_SK = stg.ADJ_FROM_CLM_SK
WHERE CD.TRGT_CD = 'BCA'
"""

df_Clm_Ref_Link = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Clm_Ref_Link)
    .load()
)

# GetFkeys (CTransformerStage) - Primary link (df_GetPKey alias Clm_SK), Lookup link 1 (df_Clm_Ref_Link alias Clm_Lkup), Lookup link 2 (df_hf_Clm_2 alias ClmSK)
df_GetFkeys_join_1 = (
    df_GetPKey.alias("Clm_SK")
    .join(
        df_Clm_Ref_Link.alias("Clm_Lkup"),
        F.col("Clm_SK.ADJ_FROM_CLM_SK") == F.col("Clm_Lkup.CLM_SK"),
        "left"
    )
)
df_GetFkeys_join_2 = (
    df_GetFkeys_join_1
    .join(
        df_hf_Clm_2.alias("ClmSK"),
        (
            (F.lit("BCA") == F.col("ClmSK.SRC_SYS_CD")) &
            (trim(F.col("Clm_SK.CLM_ID")) == F.col("ClmSK.CLM_ID"))
        ),
        "left"
    )
)

df_GetFkeys_var = df_GetFkeys_join_2.withColumn(
    "ClmSttusCd",
    F.when(F.col("Clm_Lkup.TRGT_CD") == "A02", F.lit("A09")).otherwise(F.col("Clm_Lkup.TRGT_CD"))
)

df_GetFkeys_filtered = df_GetFkeys_var.filter(F.col("ClmSK.CLM_SK").isNotNull())

df_GetFkeys = df_GetFkeys_filtered.select(
    F.col("Clm_Lkup.CLM_SK").alias("CLM_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Clm_SK.CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("ClmSK.CLM_SK").alias("ADJ_TO_CLM_SK"),
    GetFkeyCodes('BCA', F.col("Clm_Lkup.CLM_SK"), "CLAIM STATUS", F.col("ClmSttusCd"), 'N').alias("CLM_STTUS_CD_SK"),
    F.col("Clm_Lkup.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Clm_Lkup.CLM_ID").alias("CLM_ID"),
    F.col("Clm_Lkup.GRP_SK").alias("GRP_SK"),
    F.col("Clm_Lkup.MBR_SK").alias("MBR_SK"),
    F.col("Clm_Lkup.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("Clm_Lkup.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    F.col("Clm_Lkup.PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK"),
    F.col("Clm_Lkup.CLM_CAT_CD_SK").alias("CLM_CAT_CD_SK"),
    F.col("Clm_Lkup.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.col("Clm_Lkup.CHRG_AMT").alias("CHRG_AMT"),
    F.col("Clm_Lkup.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("Clm_Lkup.CLM_CT").alias("CLM_CT")
)

# Snapshot (CTransformerStage) - Split to df_Update and df_B_CLM
df_Update = df_GetFkeys.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("ADJ_TO_CLM_SK").alias("ADJ_TO_CLM_SK"),
    F.col("CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK")
)

df_B_CLM_pre = df_GetFkeys.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
    F.col("CLM_CAT_CD_SK").alias("CLM_CAT_CD_SK"),
    F.col("EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    F.col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("CLM_CT").alias("CLM_CT"),
    F.col("PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK")
)

# Apply rpad for char columns as required in final output
df_B_CLM = df_B_CLM_pre.withColumn(
    "SVC_STRT_DT_SK",
    F.rpad(F.col("SVC_STRT_DT_SK"), 10, " ")
)

# B_CLM (CSeqFileStage) - Write
write_files(
    df_B_CLM,
    f"{adls_path}/load/B_CLM.BCA.dat.{RunID}",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# CLM_Update (DB2Connector) - Upsert (merge) into {IDSOwner}.CLM on CLM_SK
execute_dml(f"DROP TABLE IF EXISTS STAGING.BCAFepMedClmAdjToUpd_CLM_Update_temp", jdbc_url_ids, jdbc_props_ids)
df_Update.write.format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.BCAFepMedClmAdjToUpd_CLM_Update_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.CLM AS T
USING STAGING.BCAFepMedClmAdjToUpd_CLM_Update_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.ADJ_TO_CLM_ID = S.ADJ_TO_CLM_ID,
    T.ADJ_TO_CLM_SK = S.ADJ_TO_CLM_SK,
    T.CLM_STTUS_CD_SK = S.CLM_STTUS_CD_SK
WHEN NOT MATCHED THEN
  INSERT
  (
    CLM_SK,
    LAST_UPDT_RUN_CYC_EXCTN_SK,
    ADJ_TO_CLM_ID,
    ADJ_TO_CLM_SK,
    CLM_STTUS_CD_SK
  )
  VALUES
  (
    S.CLM_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.ADJ_TO_CLM_ID,
    S.ADJ_TO_CLM_SK,
    S.CLM_STTUS_CD_SK
  );
"""

execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)