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
# MAGIC Karthik Chintalapani     2019-08-27              5884                  Job to update the adjusted claims information         IntegrateDev1                      Kalyan Neelam             2019-09-05

# MAGIC Read the Claim Adjust To Update file created from BCBSAFEPRXClmPreProcExtr job
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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunCycle = get_widget_value('RunCycle','100')
RunID = get_widget_value('RunID','100')

# BCAFepMedClm_AdjTos (CSeqFileStage) - Reading "verified/BCAFEP_DrugClm_AdjTos.dat.#RunID#"
schema_BCAFepMedClm_AdjTos = StructType([
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("ADJ_FROM_CLM_ID", StringType(), nullable=False),
    StructField("ADJ_TO_CLM_ID", StringType(), nullable=False),
    StructField("CLM_STTUS_CD", StringType(), nullable=False)
])

df_BCAFepMedClm_AdjTos_0 = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", False)
    .schema(schema_BCAFepMedClm_AdjTos)
    .load(f"{adls_path}/verified/BCAFEP_DrugClm_AdjTos.dat.{RunID}")
)
df_AdjClms = df_BCAFepMedClm_AdjTos_0

# hf_Clm (CHashedFileStage, scenario C) - Reading from parquet
df_hf_Clm_0 = spark.read.parquet(f"{adls_path}/hf_clm.parquet")
df_hf_Clm = df_hf_Clm_0.select(
    F.lit("NPS").alias("SRC_SYS_CD"),
    trim(<...>).alias("CLM_ID"),  # Expression was trim(AdjIn.Clm_Id), unknown reference => <...>
    df_hf_Clm_0["CRT_RUN_CYC_EXCTN_SK"],
    df_hf_Clm_0["CLM_SK"]
)

# GetPKey (CTransformerStage) - Primary link: df_AdjClms (AdjClms), Lookup link: df_hf_Clm (refClm)
df_GetPKey_0 = df_AdjClms.alias("AdjClms").join(
    df_hf_Clm.alias("refClm"),
    (F.lit("BCA") == F.col("refClm.SRC_SYS_CD")) & (F.col("AdjClms.ADJ_TO_CLM_ID") == F.col("refClm.CLM_ID")),
    how="left"
)
df_GetPKey = df_GetPKey_0.filter(F.col("refClm.CLM_SK").isNotNull())
df_Clm_SK = df_GetPKey.select(
    F.col("refClm.CLM_SK").alias("ADJ_FROM_CLM_SK"),
    F.col("AdjClms.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("AdjClms.CLM_ID").alias("CLM_ID"),
    F.col("AdjClms.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("AdjClms.CLM_STTUS_CD").alias("CLM_STTUS_CD")
)

# hf_Clm_2 (CHashedFileStage, scenario C) - Reading from the same parquet
df_hf_Clm_2_0 = spark.read.parquet(f"{adls_path}/hf_clm.parquet")
df_hf_Clm_2 = df_hf_Clm_2_0.select(
    F.lit("NPS").alias("SRC_SYS_CD"),
    trim(<...>).alias("CLM_ID"),  # Expression was trim(updt_Clm_nfo.ADJ_TO_CLM_ID), unknown => <...>
    df_hf_Clm_2_0["CRT_RUN_CYC_EXCTN_SK"],
    df_hf_Clm_2_0["CLM_SK"]
)

# Clm_Ref_Link (DB2Connector) with WHERE CLM_ID=? => translate to a join using a staging table
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.BCAFepRxClmAdjToUpd_Clm_Ref_Link_temp", jdbc_url, jdbc_props)

# Create STAGING.BCAFepRxClmAdjToUpd_Clm_Ref_Link_temp from df_Clm_SK (the primary link's DataFrame)
write_files(
    df_Clm_SK.select("CLM_ID"),  # Only need the join column in the staging table
    "STAGING.BCAFepRxClmAdjToUpd_Clm_Ref_Link_temp",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

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
JOIN STAGING.BCAFepRxClmAdjToUpd_Clm_Ref_Link_temp t ON (CLM.CLM_ID = t.CLM_ID)
     ,{IDSOwner}.CD_MPPNG CD
     ,{IDSOwner}.CD_MPPNG CD1
WHERE CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
  AND CD.TRGT_CD = 'BCA'
  AND CLM.CLM_STTUS_CD_SK = CD1.CD_MPPNG_SK
"""

df_Clm_Lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Clm_Ref_Link)
    .load()
)

# GetFkeys (CTransformerStage) - Primary link: df_Clm_SK, Lookup link1: df_Clm_Lkup(Clm_Lkup), Lookup link2: df_hf_Clm_2(ClmSK)
df_GetFkeys_0 = df_Clm_SK.alias("Clm_SK") \
    .join(
        df_Clm_Lkup.alias("Clm_Lkup"),
        F.col("Clm_SK.CLM_ID") == F.col("Clm_Lkup.CLM_ID"),
        how="left"
    ) \
    .join(
        df_hf_Clm_2.alias("ClmSK"),
        (F.lit("BCA") == F.col("ClmSK.SRC_SYS_CD")) & (F.col("Clm_SK.ADJ_TO_CLM_ID") == F.col("ClmSK.CLM_ID")),
        how="left"
    )

df_GetFkeys_1 = df_GetFkeys_0.filter(F.col("ClmSK.CLM_SK").isNotNull())

df_Transform = df_GetFkeys_1.select(
    F.col("Clm_Lkup.CLM_SK").alias("CLM_SK"),
    F.lit(RunCycle).cast("int").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Clm_SK.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("ClmSK.CLM_SK").alias("ADJ_TO_CLM_SK"),
    getFkeyCodes("BCA", F.col("Clm_Lkup.CLM_SK"), "CLAIM STATUS", F.col("Clm_SK.CLM_STTUS_CD"), "N").alias("CLM_STTUS_CD_SK"),
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

# Snapshot (CTransformerStage)
df_Snapshot_0 = df_Transform

# B_CLM output link
df_B_CLM = df_Snapshot_0.select(
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

# Before writing final file for B_CLM, apply rpad where char/varchar length is known or <...> if unknown
df_B_CLM_final = df_B_CLM.withColumn(
    "CLM_ID",
    F.rpad(F.col("CLM_ID"), <...>, " ")
).withColumn(
    "SVC_STRT_DT_SK",
    F.rpad(F.col("SVC_STRT_DT_SK"), 10, " ")
)

write_files(
    df_B_CLM_final,
    f"{adls_path}/load/B_CLM.BCA.dat.{RunID}",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Update link => CLM_Update (DB2Connector) merges into #$IDSOwner#.CLM
df_Update = df_Snapshot_0.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("ADJ_TO_CLM_SK").alias("ADJ_TO_CLM_SK"),
    F.col("CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK")
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.BCAFepRxClmAdjToUpd_CLM_Update_temp", jdbc_url, jdbc_props)

write_files(
    df_Update,
    "STAGING.BCAFepRxClmAdjToUpd_CLM_Update_temp",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

merge_sql = f"""
MERGE INTO {IDSOwner}.CLM AS target
USING STAGING.BCAFepRxClmAdjToUpd_CLM_Update_temp AS source
ON target.CLM_SK = source.CLM_SK
WHEN MATCHED THEN UPDATE SET
  target.LAST_UPDT_RUN_CYC_EXCTN_SK = source.LAST_UPDT_RUN_CYC_EXCTN_SK,
  target.ADJ_TO_CLM_ID = source.ADJ_TO_CLM_ID,
  target.ADJ_TO_CLM_SK = source.ADJ_TO_CLM_SK,
  target.CLM_STTUS_CD_SK = source.CLM_STTUS_CD_SK
WHEN NOT MATCHED THEN INSERT
(
  CLM_SK,
  LAST_UPDT_RUN_CYC_EXCTN_SK,
  ADJ_TO_CLM_ID,
  ADJ_TO_CLM_SK,
  CLM_STTUS_CD_SK
)
VALUES
(
  source.CLM_SK,
  source.LAST_UPDT_RUN_CYC_EXCTN_SK,
  source.ADJ_TO_CLM_ID,
  source.ADJ_TO_CLM_SK,
  source.CLM_STTUS_CD_SK
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)