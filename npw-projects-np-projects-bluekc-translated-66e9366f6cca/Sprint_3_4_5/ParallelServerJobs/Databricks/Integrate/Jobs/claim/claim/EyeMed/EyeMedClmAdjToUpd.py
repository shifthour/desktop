# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2018 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : IdsEyeMedClmLoadAllSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:      EyeMed Claim Adjusted claims update job. Updates the Adj To information on the claims already existing on IDS if their adjusting claims are received
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #          Change Description                                                     Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------          --------------------     ------------------------      -----------------------------------------------------------------------                --------------------------------              -------------------------------   ----------------------------      
# MAGIC Kalyan Neelam      2017-11-08              5781                          Original Programming                                                IntegrateDev2

# MAGIC Read the Claim Adjust To Update file created from EyeMedClmPreProcExtr job
# MAGIC Update Claim table with adjusted-to information
# MAGIC Get current claim SK from primary key hash file
# MAGIC Validate that claim exists in database
# MAGIC Get adjusted-to claim SK from primary key hash file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, rpad, isnull, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunCycle = get_widget_value('RunCycle','')
RunID = get_widget_value('RunID','')

# Read "EyeMedMedClm_AdjTos" (CSeqFileStage)
schema_EyeMedMedClm_AdjTos = StructType([
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("ADJ_FROM_CLM_ID", StringType(), nullable=False),
    StructField("ADJ_TO_CLM_ID", StringType(), nullable=False),
    StructField("CLM_STTUS_CD", StringType(), nullable=False)
])
df_EyeMedMedClm_AdjTos = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_EyeMedMedClm_AdjTos)
    .load(f"{adls_path}/verified/EyeMedClm_AdjTos.dat.{RunID}")
)

# Read "hf_Clm" (CHashedFileStage) as parquet
df_hf_Clm = spark.read.parquet(f"{adls_path}/hf_clm.parquet")
# Apply expressions/column selection in the same order
df_hf_Clm = (
    df_hf_Clm
    .withColumn("SRC_SYS_CD", lit("NPS"))
    .withColumn("CLM_ID", trim(col("CLM_ID")))
    .select(
        rpad(lit("NPS"), 3, " ").alias("SRC_SYS_CD"),  # "varchar" with no defined length; using the literal
        rpad(trim(col("CLM_ID")), 18, " ").alias("CLM_ID"),  # char(18)
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("CLM_SK")
    )
)

# Read "hf_Clm_2" (CHashedFileStage) as parquet
df_hf_Clm_2 = spark.read.parquet(f"{adls_path}/hf_clm.parquet")
# Apply expressions/column selection in the same order
df_hf_Clm_2 = (
    df_hf_Clm_2
    .withColumn("SRC_SYS_CD", lit("NPS"))
    .withColumn("CLM_ID", trim(col("CLM_ID")))
    .select(
        rpad(lit("NPS"), 3, " ").alias("SRC_SYS_CD"),  # "varchar" with no defined length; using the literal
        rpad(trim(col("CLM_ID")), 18, " ").alias("CLM_ID"),  # char(18)
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("CLM_SK")
    )
)

# GetPKey (CTransformerStage)
# Primary link: df_EyeMedMedClm_AdjTos (alias "AdjClms")
# Lookup link: df_hf_Clm (alias "refClm"), left join on:
#   lit("EYEMED") == refClm.SRC_SYS_CD
#   trim(AdjClms.ADJ_FROM_CLM_ID) == refClm.CLM_ID
# Constraint: IsNull(refClm.CLM_SK) = @FALSE => filter out null => effectively inner join
df_GetPKey = (
    df_EyeMedMedClm_AdjTos.alias("AdjClms")
    .join(
        df_hf_Clm.alias("refClm"),
        (
            (lit("EYEMED") == col("refClm.SRC_SYS_CD")) &
            (trim(col("AdjClms.ADJ_FROM_CLM_ID")) == col("refClm.CLM_ID"))
        ),
        how="left"
    )
    .filter(col("refClm.CLM_SK").isNotNull())
    .select(
        col("refClm.CLM_SK").alias("ADJ_FROM_CLM_SK"),
        col("AdjClms.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
        col("AdjClms.CLM_ID").alias("CLM_ID"),
        col("AdjClms.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
        col("AdjClms.CLM_STTUS_CD").alias("CLM_STTUS_CD")
    )
)

# Clm_Ref_Link (DB2Connector) with "CLM_SK=?" logic.
# We create a temporary table from df_GetPKey for the join keys.
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.EyeMedClmAdjToUpd_Clm_Ref_Link_temp", jdbc_url, jdbc_props)

df_GetPKey.select("ADJ_FROM_CLM_SK").dropDuplicates().write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EyeMedClmAdjToUpd_Clm_Ref_Link_temp") \
    .mode("overwrite") \
    .save()

extract_query_clm_ref_link = f"""
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
JOIN {IDSOwner}.CD_MPPNG CD
  ON CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
JOIN {IDSOwner}.CD_MPPNG CD1
  ON CLM.CLM_STTUS_CD_SK = CD1.CD_MPPNG_SK
JOIN STAGING.EyeMedClmAdjToUpd_Clm_Ref_Link_temp T
  ON T.ADJ_FROM_CLM_SK = CLM.CLM_SK
WHERE CD.TRGT_CD = 'EYEMED'
"""
df_Clm_Ref_Link = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_clm_ref_link)
    .load()
)

# GetFkeys (CTransformerStage)
# Primary link: df_GetPKey (alias "Clm_SK")
# Lookup link 1: df_Clm_Ref_Link (alias "Clm_Lkup"), left join on Clm_SK.ADJ_FROM_CLM_SK = Clm_Lkup.CLM_SK
# Lookup link 2: df_hf_Clm_2 (alias "ClmSK"), left join on "EYEMED"=ClmSK.SRC_SYS_CD and trim(Clm_SK.CLM_ID)=ClmSK.CLM_ID
df_GetFkeys = (
    df_GetPKey.alias("Clm_SK")
    .join(
        df_Clm_Ref_Link.alias("Clm_Lkup"),
        col("Clm_SK.ADJ_FROM_CLM_SK") == col("Clm_Lkup.CLM_SK"),
        how="left"
    )
    .join(
        df_hf_Clm_2.alias("ClmSK"),
        (
            (lit("EYEMED") == col("ClmSK.SRC_SYS_CD")) &
            (trim(col("Clm_SK.CLM_ID")) == col("ClmSK.CLM_ID"))
        ),
        how="left"
    )
)

# Stage variable: ClmSttusCd = If Clm_Lkup.TRGT_CD = 'A02' Then 'A09' Else Clm_Lkup.TRGT_CD
df_GetFkeys = df_GetFkeys.withColumn(
    "ClmSttusCd",
    when(col("Clm_Lkup.TRGT_CD")=="A02", "A09").otherwise(col("Clm_Lkup.TRGT_CD"))
)

# Constraint: IsNull(ClmSK.CLM_SK) = @FALSE => filter out rows where ClmSK.CLM_SK is null
df_GetFkeys = df_GetFkeys.filter(col("ClmSK.CLM_SK").isNotNull())

# Output columns for link "Transform"
df_GetFkeys = df_GetFkeys.select(
    col("Clm_Lkup.CLM_SK").alias("CLM_SK"),
    lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Clm_SK.CLM_ID").alias("ADJ_TO_CLM_ID"),
    col("ClmSK.CLM_SK").alias("ADJ_TO_CLM_SK"),
    GetFkeyCodes("EYEMED", col("Clm_Lkup.CLM_SK"), "CLAIM STATUS", col("ClmSttusCd"), 'N').alias("CLM_STTUS_CD_SK"),
    col("Clm_Lkup.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("Clm_Lkup.CLM_ID").alias("CLM_ID"),
    col("Clm_Lkup.GRP_SK").alias("GRP_SK"),
    col("Clm_Lkup.MBR_SK").alias("MBR_SK"),
    col("Clm_Lkup.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    col("Clm_Lkup.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    col("Clm_Lkup.PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK"),
    col("Clm_Lkup.CLM_CAT_CD_SK").alias("CLM_CAT_CD_SK"),
    col("Clm_Lkup.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    col("Clm_Lkup.CHRG_AMT").alias("CHRG_AMT"),
    col("Clm_Lkup.PAYBL_AMT").alias("PAYBL_AMT"),
    col("Clm_Lkup.CLM_CT").alias("CLM_CT")
)

# Snapshot (CTransformerStage)
# Input is df_GetFkeys. We branch out to two links: "Update" -> CLM_Update, and "B_CLM" -> B_CLM

df_Snapshot = df_GetFkeys

# Link "Update" columns
df_Update = df_Snapshot.select(
    col("CLM_SK").alias("CLM_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    col("ADJ_TO_CLM_SK").alias("ADJ_TO_CLM_SK"),
    col("CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK")
)

# Link "B_CLM" columns
df_B_CLM = df_Snapshot.select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
    col("CLM_CAT_CD_SK").alias("CLM_CAT_CD_SK"),
    col("EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    col("GRP_SK").alias("GRP_SK"),
    col("MBR_SK").alias("MBR_SK"),
    col("SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    col("CHRG_AMT").alias("CHRG_AMT"),
    col("PAYBL_AMT").alias("PAYBL_AMT"),
    col("CLM_CT").alias("CLM_CT"),
    col("PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK")
)

# For the file B_CLM, "CLM_ID" is char(18) in upstream definitions, "SVC_STRT_DT_SK" is char(10).
# Apply rpad. The rest do not have explicit char lengths; leaving as is.
df_B_CLM = df_B_CLM.withColumn("CLM_ID", rpad(col("CLM_ID"), 18, " "))
df_B_CLM = df_B_CLM.withColumn("SVC_STRT_DT_SK", rpad(col("SVC_STRT_DT_SK"), 10, " "))

# B_CLM (CSeqFileStage) writing to load/B_CLM.EYEMED.dat.#RunID#
write_files(
    df_B_CLM,
    f"{adls_path}/load/B_CLM.EYEMED.dat.{RunID}",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# CLM_Update (DB2Connector) to IDS database => upsert into {IDSOwner}.CLM
execute_dml(f"DROP TABLE IF EXISTS STAGING.EyeMedClmAdjToUpd_CLM_Update_temp", jdbc_url, jdbc_props)

df_Update.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EyeMedClmAdjToUpd_CLM_Update_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.CLM AS T
USING STAGING.EyeMedClmAdjToUpd_CLM_Update_temp AS S
ON (T.CLM_SK = S.CLM_SK)
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.ADJ_TO_CLM_ID = S.ADJ_TO_CLM_ID,
    T.ADJ_TO_CLM_SK = S.ADJ_TO_CLM_SK,
    T.CLM_STTUS_CD_SK = S.CLM_STTUS_CD_SK
WHEN NOT MATCHED THEN
  INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, ADJ_TO_CLM_ID, ADJ_TO_CLM_SK, CLM_STTUS_CD_SK)
  VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.ADJ_TO_CLM_ID, S.ADJ_TO_CLM_SK, S.CLM_STTUS_CD_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)