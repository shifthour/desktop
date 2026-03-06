# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwUmDiagSetDExtr
# MAGIC 
# MAGIC DESCRIPTION:  Extracts from IDS UM_DIAG_SET to flatfile UM_DIAG_SET_D.dat
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC The following IDS table is used:
# MAGIC UM_DIAG_SET
# MAGIC 
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Flatfile: UM_DIAG_SET_D.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                                  Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------         ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Suzanne Saylor              3/3/2006          MedMgmt/                        Original Programming.                                                                               devlEDW10
# MAGIC Bhoomi Dasari                2/08/2007        MedMgmt/                        Added LAST_UPDT_RUN_CYC_EXCTN_SK  >= ExtractRunCycle        devlEDW                     Steph Goddard            01/23/2008
# MAGIC                                                                                                          rule based upon which we extract records now.  
# MAGIC                                                                                                          Checked null's for lkups
# MAGIC Ralph Tucker                  09/15/2009     TTR-583                           Added EdwRunCycle to last updt run cyc                                                   devlEDW                   Steph Goddard             09/16/2009
# MAGIC Ralph Tucker                 5/14/2012       4896 Edw Remediation     Added new Diag_Cd_Typ fields                                                              IntegrateNewDevl           Sandrew                       2012-05-17
# MAGIC Sravya Gorla                   10/02/2019     US# 140167                 Updated the datatype of DIAG_SET_CRT_DTM                               EnterpriseDev2                      Jaideep Mankala          10/07/2019
# MAGIC                                                                                                       from Timestamp(26.3) to timestamp(26.6)

# MAGIC Extract IDS Data
# MAGIC Apply business logic.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
EdwRunCycle = get_widget_value('EdwRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# --------------------------------------------------------------------------------
# Stage: IDS (DB2Connector) - First Output Pin: "Extract"
# --------------------------------------------------------------------------------
query_extract = f"""
SELECT
  um.UM_DIAG_SET_SK AS UM_DIAG_SET_SK,
  um.SRC_SYS_CD_SK AS SRC_SYS_CD_SK,
  um.UM_REF_ID AS UM_REF_ID,
  um.DIAG_SET_CRT_DTM AS DIAG_SET_CRT_DTM,
  um.SEQ_NO AS SEQ_NO,
  um.CRT_RUN_CYC_EXCTN_SK AS CRT_RUN_CYC_EXCTN_SK,
  um.LAST_UPDT_RUN_CYC_EXCTN_SK AS LAST_UPDT_RUN_CYC_EXCTN_SK,
  um.DIAG_CD_SK AS DIAG_CD_SK,
  um.UM_SK AS UM_SK,
  um.ONSET_DT_SK AS ONSET_DT_SK,
  diag.DIAG_CD AS DIAG_CD
FROM {IDSOwner}.UM_DIAG_SET um,
     {IDSOwner}.DIAG_CD diag
WHERE um.DIAG_CD_SK = diag.DIAG_CD_SK
  AND um.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""

df_IDS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_extract)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: IDS (DB2Connector) - Second Output Pin: "Extract_Diag_Cd"
# --------------------------------------------------------------------------------
query_extract_diag_cd = f"""
SELECT
  DIAG_CD.DIAG_CD_SK AS DIAG_CD_SK,
  DIAG_CD.DIAG_CD AS DIAG_CD,
  DIAG_CD_TYP_CD AS DIAG_CD_TYP_CD,
  map.TRGT_CD_NM AS DIAG_CD_TYP_CD_NM,
  DIAG_CD_TYP_CD_SK AS DIAG_CD_TYP_CD_SK
FROM {IDSOwner}.DIAG_CD DIAG_CD,
     {IDSOwner}.CD_MPPNG map
WHERE DIAG_CD_TYP_CD_SK = map.CD_MPPNG_SK
"""

df_IDS_Extract_Diag_Cd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_extract_diag_cd)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_cdma_codes (CHashedFileStage) - Scenario C (read from parquet)
# --------------------------------------------------------------------------------
df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

# --------------------------------------------------------------------------------
# Stage: hf_um_diag_set_diag_cd (CHashedFileStage) - Scenario A (intermediate file)
# --------------------------------------------------------------------------------
# Key column for dedup is DIAG_CD_SK
df_hf_um_diag_set_diag_cd = df_IDS_Extract_Diag_Cd.dropDuplicates(["DIAG_CD_SK"])

# --------------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
# Left joins: refSrcSysCd on (SRC_SYS_CD_SK = CD_MPPNG_SK), Diag_Cd_Sk_Lookup on (DIAG_CD_SK = DIAG_CD_SK)
# --------------------------------------------------------------------------------
df_businessRules = (
    df_IDS_Extract.alias("Extract")
    .join(
        df_hf_cdma_codes.alias("refSrcSysCd"),
        F.col("Extract.SRC_SYS_CD_SK") == F.col("refSrcSysCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_um_diag_set_diag_cd.alias("Diag_Cd_Sk_Lookup"),
        F.col("Extract.DIAG_CD_SK") == F.col("Diag_Cd_Sk_Lookup.DIAG_CD_SK"),
        "left"
    )
    .select(
        F.col("Extract.UM_DIAG_SET_SK").alias("UM_DIAG_SET_SK"),
        F.when(
            F.col("refSrcSysCd.TRGT_CD").isNull() | (F.length(F.trim(F.col("refSrcSysCd.TRGT_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("refSrcSysCd.TRGT_CD")).alias("SRC_SYS_CD"),
        F.col("Extract.UM_REF_ID").alias("UM_REF_ID"),
        F.col("Extract.DIAG_SET_CRT_DTM").alias("UM_DIAG_SET_CRT_DTM"),
        F.col("Extract.SEQ_NO").alias("UM_DIAG_SET_SEQ_NO"),
        F.rpad(F.lit(CurrentDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.rpad(F.lit(CurrentDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("Extract.DIAG_CD_SK").alias("DIAG_CD_SK"),
        F.col("Extract.UM_SK").alias("UM_SK"),
        F.col("Extract.ONSET_DT_SK").alias("UM_DIAG_SET_ONSET_DT"),
        F.col("Extract.DIAG_CD").alias("DIAG_CD"),
        F.col("Extract.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EdwRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(
            F.col("Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD").isNull(),
            F.lit("UNK")
        ).otherwise(F.col("Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD")).alias("DIAG_CD_TYP_CD"),
        F.when(
            F.col("Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD_NM").isNull(),
            F.lit("UNK")
        ).otherwise(F.col("Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD_NM")).alias("DIAG_CD_TYP_NM"),
        F.when(
            F.col("Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD_SK").isNull(),
            F.lit(0)
        ).otherwise(F.col("Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD_SK")).alias("DIAG_CD_TYP_CD_SK")
    )
)

# --------------------------------------------------------------------------------
# Stage: UM_DIAG_SET_D (CSeqFileStage) - Write to .dat
# --------------------------------------------------------------------------------
write_files(
    df_businessRules,
    f"{adls_path}/load/UM_DIAG_SET_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)