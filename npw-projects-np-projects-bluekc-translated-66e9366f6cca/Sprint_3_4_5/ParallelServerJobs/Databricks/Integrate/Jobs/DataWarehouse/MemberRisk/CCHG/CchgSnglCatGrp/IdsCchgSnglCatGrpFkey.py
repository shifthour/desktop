# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY:CblTalnCchgLoadSeq
# MAGIC 
# MAGIC PROCESSING:  This job is for Fkey Lookups, Process the records from Pkey Job and look up for SKs against CD_MPPNG table. Output file is Load ready.
# MAGIC                                                                 
# MAGIC      
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                   DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                         DESCRIPTION                                                                                    ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Kalyan Neelam          2015-07-28         5460                               Initial Programming                                                                                IntegrateDev2            Bhoomi Dasari              8/30/2015

# MAGIC ICD_VRSN_CD lookup
# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')

job_name = "IdsCchgSnglCatGrpFkey"

# Read seqCCHG_SNGL_CAT_GRP_Pkey (PxSequentialFile)
schema_seqCCHG_SNGL_CAT_GRP_Pkey = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), False),
    StructField("FIRST_RECYC_TS", TimestampType(), False),
    StructField("CCHG_SNGL_CAT_GRP_SK", IntegerType(), False),
    StructField("CCHG_SNGL_CAT_GRP_CD", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("ICD_VRSN_CD", StringType(), False),
    StructField("CCHG_SNGL_CAT_GRP_DESC", StringType(), False),
    StructField("ICD_DIAG_CLM_MPPNG_DESC", StringType(), False)
])
df_seqCCHG_SNGL_CAT_GRP_Pkey = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("escape", "^")
    .option("nullValue", None)
    .schema(schema_seqCCHG_SNGL_CAT_GRP_Pkey)
    .csv(f"{adls_path}/key/CCHG_SNGL_CAT_GRP.{SrcSysCd}.pkey.{RunID}.dat")
)

# Read ds_CD_MPPNG_LkpData (PxDataSet => read from parquet)
df_ds_CD_MPPNG_LkpData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

# fltr_FilterData (PxFilter)
df_fltr_FilterData = (
    df_ds_CD_MPPNG_LkpData
    .filter(
        (F.col("SRC_SYS_CD") == "BCA") &
        (F.col("SRC_CLCTN_CD") == "BCA") &
        (F.col("TRGT_CLCTN_CD") == "IDS") &
        (F.col("SRC_DOMAIN_NM") == "DIAGNOSIS CODE TYPE") &
        (F.col("TRGT_DOMAIN_NM") == "DIAGNOSIS CODE TYPE")
    )
    .select(
        F.col("SRC_CD").alias("SRC_CD"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

# CdMppngLkup (PxLookup) with two join conditions, using left join
df_CdMppngLkup = (
    df_seqCCHG_SNGL_CAT_GRP_Pkey.alias("In")
    .join(
        df_fltr_FilterData.alias("IcdVrsnCd"),
        (
            (F.col("In.ICD_VRSN_CD") == F.col("IcdVrsnCd.SRC_CD")) &
            (F.lit(None).cast("int") == F.col("IcdVrsnCd.CD_MPPNG_SK"))
        ),
        how="left"
    )
    .select(
        F.col("In.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("In.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("In.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK"),
        F.col("In.CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
        F.col("In.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("In.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("In.ICD_VRSN_CD").alias("ICD_VRSN_CD"),
        F.col("In.CCHG_SNGL_CAT_GRP_DESC").alias("CCHG_SNGL_CAT_GRP_DESC"),
        F.col("In.ICD_DIAG_CLM_MPPNG_DESC").alias("ICD_DIAG_CLM_MPPNG_DESC"),
        F.col("IcdVrsnCd.CD_MPPNG_SK").alias("ICD_VRSN_CD_MPPNG_SK")
    )
)

# xfm_CheckLkpResults (CTransformerStage)
w = Window.orderBy(F.lit(1))
df_xfm_CheckLkpResults = (
    df_CdMppngLkup
    .withColumn("row_number", F.row_number().over(w))
    .withColumn(
        "svIcdVrsnCdFkeyFail",
        F.when(F.col("ICD_VRSN_CD_MPPNG_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
)

# Lnk_Main
df_xfm_CheckLkpResults_Lnk_Main = df_xfm_CheckLkpResults.select(
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK"),
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ICD_VRSN_CD_MPPNG_SK").alias("ICD_VRSN_CD_SK"),
    F.col("CCHG_SNGL_CAT_GRP_DESC").alias("CCHG_SNGL_CAT_GRP_DESC"),
    F.col("ICD_DIAG_CLM_MPPNG_DESC").alias("ICD_DIAG_CLM_MPPNG_DESC")
)

# Lnk_NA
df_xfm_CheckLkpResults_Lnk_NA = df_xfm_CheckLkpResults.filter(F.col("row_number") == 1).select(
    F.lit(1).alias("CCHG_SNGL_CAT_GRP_SK"),
    F.lit("NA").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("ICD_VRSN_CD_SK"),
    F.lit("NA").alias("CCHG_SNGL_CAT_GRP_DESC"),
    F.lit("NA").alias("ICD_DIAG_CLM_MPPNG_DESC")
)

# Lnk_UNK
df_xfm_CheckLkpResults_Lnk_UNK = df_xfm_CheckLkpResults.filter(F.col("row_number") == 1).select(
    F.lit(0).alias("CCHG_SNGL_CAT_GRP_SK"),
    F.lit("UNK").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("ICD_VRSN_CD_SK"),
    F.lit("UNK").alias("CCHG_SNGL_CAT_GRP_DESC"),
    F.lit("UNK").alias("ICD_DIAG_CLM_MPPNG_DESC")
)

# IcdVrsnCd_LkupFail
df_xfm_CheckLkpResults_IcdVrsnCd_LkupFail = df_xfm_CheckLkpResults.filter(
    F.col("svIcdVrsnCdFkeyFail") == "Y"
).select(
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(job_name).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(
        F.lit("IDS:BCA:IDS:DIAGNOSIS CODE TYPE:DIAGNOSIS CODE TYPE:"),
        F.col("ICD_VRSN_CD")
    ).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# Write seq_FkeyFailedFile_csv
df_final_seq_FkeyFailedFile_csv = df_xfm_CheckLkpResults_IcdVrsnCd_LkupFail.select(
    F.col("PRI_SK"),
    F.rpad(F.col("PRI_NAT_KEY_STRING"), 255, " ").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("JOB_NM"), 255, " ").alias("JOB_NM"),
    F.rpad(F.col("ERROR_TYP"), 255, " ").alias("ERROR_TYP"),
    F.rpad(F.col("PHYSCL_FILE_NM"), 255, " ").alias("PHYSCL_FILE_NM"),
    F.rpad(F.col("FRGN_NAT_KEY_STRING"), 255, " ").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("JOB_EXCTN_SK")
)
write_files(
    df_final_seq_FkeyFailedFile_csv,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{job_name}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# fnl_NA_UNK_Streams (PxFunnel)
df_fnl_NA_UNK_Streams = df_xfm_CheckLkpResults_Lnk_Main.unionByName(
    df_xfm_CheckLkpResults_Lnk_NA
).unionByName(
    df_xfm_CheckLkpResults_Lnk_UNK
)

# Write seq_CCHG_SNGL_CAT_GRP_Fkey
df_final_seq_CCHG_SNGL_CAT_GRP_Fkey = df_fnl_NA_UNK_Streams.select(
    F.col("CCHG_SNGL_CAT_GRP_SK"),
    F.rpad(F.col("CCHG_SNGL_CAT_GRP_CD"), 255, " ").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ICD_VRSN_CD_SK"),
    F.rpad(F.col("CCHG_SNGL_CAT_GRP_DESC"), 255, " ").alias("CCHG_SNGL_CAT_GRP_DESC"),
    F.rpad(F.col("ICD_DIAG_CLM_MPPNG_DESC"), 255, " ").alias("ICD_DIAG_CLM_MPPNG_DESC")
)
write_files(
    df_final_seq_CCHG_SNGL_CAT_GRP_Fkey,
    f"{adls_path}/load/CCHG_SNGL_CAT_GRP.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)