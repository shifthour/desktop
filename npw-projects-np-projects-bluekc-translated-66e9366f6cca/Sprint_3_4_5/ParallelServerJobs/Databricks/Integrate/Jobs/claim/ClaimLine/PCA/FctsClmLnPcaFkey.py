# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsFctsClmLoad4Seq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is the foreign keying process for the claim line PCA table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                           Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                                2008-07-23          3567                              Added the Source System Code SK part            devlIDS                         Steph Goddard            07/25/2008
# MAGIC 
# MAGIC Reddy Sanam                 2020-00-11                                                 Brought up to standards

# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DecimalType,
    TimestampType
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile", "")
Logging = get_widget_value("Logging", "\"Y\"")
RunCycle = get_widget_value("RunCycle", "100")
RunID = get_widget_value("RunID", "155000")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "")

schema_Fcts_Clm_ln_Pca_Extr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CLM_LN_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CLM_SK", IntegerType(), False),
    StructField("DSALW_EXCD_SK", IntegerType(), False),
    StructField("CLM_LN_PCA_LOB_CD_SK", IntegerType(), False),
    StructField("CLM_LN_PCA_PRCS_CD_SK", IntegerType(), False),
    StructField("CNSD_AMT", DecimalType(38, 10), False),
    StructField("DSALW_AMT", DecimalType(38, 10), False),
    StructField("NONCNSD_AMT", DecimalType(38, 10), False),
    StructField("PROV_PD_AMT", DecimalType(38, 10), False),
    StructField("SUB_PD_AMT", DecimalType(38, 10), False),
    StructField("PD_AMT", DecimalType(38, 10), False)
])

df_Fcts_Clm_ln_Pca_Extr = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_Fcts_Clm_ln_Pca_Extr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_ForeignKey = (
    df_Fcts_Clm_ln_Pca_Extr
    .withColumn("svSrcSysCd", F.when(F.col("SRC_SYS_CD") == F.lit("LUMERIS"), F.lit("FACETS")).otherwise(F.col("SRC_SYS_CD")))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ClmLnPcaLobCdSk", GetFkeyCodes(F.col("svSrcSysCd"), F.col("CLM_LN_SK"), F.lit("CLAIM LINE LOB"), F.col("CLM_LN_PCA_LOB_CD_SK"), Logging))
    .withColumn("ClmLnPcaPrcsCdSk", GetFkeyCodes(F.col("svSrcSysCd"), F.col("CLM_LN_SK"), F.lit("PERSONAL CARE ACCOUNT PROCESSING"), F.col("CLM_LN_PCA_PRCS_CD_SK"), Logging))
    .withColumn("DsalwExcdSk", GetFkeyExcd(F.col("SRC_SYS_CD"), F.col("CLM_LN_SK"), F.col("DSALW_EXCD_SK"), Logging))
    .withColumn("ClmSk", GetFkeyClm(F.col("SRC_SYS_CD"), F.col("CLM_LN_SK"), F.col("CLM_SK"), Logging))
    .withColumn("DiscardIn", F.lit("N"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CLM_LN_SK")))
)

df_Recycle = (
    df_ForeignKey
    .filter(F.col("ErrCount") > F.lit(0))
    .select(
        GetRecycleKey(F.col("CLM_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DiscardIn").alias("DISCARD_IN"),
        F.col("PassThru").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT").cast("long") + F.lit(1)).cast("string").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CLM_LN_SK").alias("CLM_LN_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("DSALW_EXCD_SK").alias("DSALW_EXCD_SK"),
        F.col("CLM_LN_PCA_LOB_CD_SK").alias("CLM_LN_PCA_LOB_CD_SK"),
        F.col("CLM_LN_PCA_PRCS_CD_SK").alias("CLM_LN_PCA_PRCS_CD_SK"),
        F.col("CNSD_AMT").alias("CNSD_AMT"),
        F.col("DSALW_AMT").alias("DSALW_AMT"),
        F.col("NONCNSD_AMT").alias("NONCNSD_AMT"),
        F.col("PROV_PD_AMT").alias("PROV_PD_AMT"),
        F.col("SUB_PD_AMT").alias("SUB_PD_AMT"),
        F.col("PD_AMT").alias("PD_AMT")
    )
)

df_Recycle = (
    df_Recycle
    .withColumn("INSRT_UPDT_CD", rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("RECYCLE_CT", rpad(F.col("RECYCLE_CT"), 6, " "))
    .withColumn("SRC_SYS_CD", rpad(F.col("SRC_SYS_CD"), 20, " "))
    .withColumn("PRI_KEY_STRING", rpad(F.col("PRI_KEY_STRING"), 1023, " "))
    .withColumn("CLM_ID", rpad(F.col("CLM_ID"), <...>, " "))
)

write_files(
    df_Recycle,
    f"{adls_path}/Recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_Recycle_Clms = (
    df_ForeignKey
    .filter(F.col("ErrCount") > F.lit(0))
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID")
    )
)

df_Recycle_Clms = (
    df_Recycle_Clms
    .withColumn("SRC_SYS_CD", rpad(F.col("SRC_SYS_CD"), 20, " "))
    .withColumn("CLM_ID", rpad(F.col("CLM_ID"), <...>, " "))
)

write_files(
    df_Recycle_Clms,
    f"{adls_path}/hf_claim_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_FKey = (
    df_ForeignKey
    .filter((F.col("ErrCount") == F.lit(0)) | (F.col("PassThru") == F.lit("Y")))
    .select(
        F.col("CLM_LN_SK").alias("CLM_LN_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ClmSk").alias("CLM_SK"),
        F.col("DsalwExcdSk").alias("DSALW_EXCD_SK"),
        F.col("ClmLnPcaLobCdSk").alias("CLM_LN_PCA_LOB_CD_SK"),
        F.col("ClmLnPcaPrcsCdSk").alias("CLM_LN_PCA_PRCS_CD_SK"),
        F.col("CNSD_AMT").alias("CNSD_AMT"),
        F.col("DSALW_AMT").alias("DSALW_AMT"),
        F.col("NONCNSD_AMT").alias("NONCNSD_AMT"),
        F.col("PROV_PD_AMT").alias("PROV_PD_AMT"),
        F.col("SUB_PD_AMT").alias("SUB_PD_AMT"),
        F.col("PD_AMT").alias("PD_AMT")
    )
)

df_DefaultUNK_source = df_ForeignKey.limit(1)
df_DefaultUNK = df_DefaultUNK_source.select(
    F.lit("0").alias("CLM_LN_SK"),
    F.lit("0").alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit("0").alias("CLM_LN_SEQ_NO"),
    F.lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("0").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("0").alias("CLM_SK"),
    F.lit("0").alias("DSALW_EXCD_SK"),
    F.lit("0").alias("CLM_LN_PCA_LOB_CD_SK"),
    F.lit("0").alias("CLM_LN_PCA_PRCS_CD_SK"),
    F.lit("0").alias("CNSD_AMT"),
    F.lit("0").alias("DSALW_AMT"),
    F.lit("0").alias("NONCNSD_AMT"),
    F.lit("0").alias("PROV_PD_AMT"),
    F.lit("0").alias("SUB_PD_AMT"),
    F.lit("0").alias("PD_AMT")
)

df_DefaultNA_source = df_ForeignKey.limit(1)
df_DefaultNA = df_DefaultNA_source.select(
    F.lit("1").alias("CLM_LN_SK"),
    F.lit("1").alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("CLM_ID"),
    F.lit("1").alias("CLM_LN_SEQ_NO"),
    F.lit("1").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("1").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("1").alias("CLM_SK"),
    F.lit("1").alias("DSALW_EXCD_SK"),
    F.lit("1").alias("CLM_LN_PCA_LOB_CD_SK"),
    F.lit("1").alias("CLM_LN_PCA_PRCS_CD_SK"),
    F.lit("0").alias("CNSD_AMT"),
    F.lit("0").alias("DSALW_AMT"),
    F.lit("0").alias("NONCNSD_AMT"),
    F.lit("0").alias("PROV_PD_AMT"),
    F.lit("0").alias("SUB_PD_AMT"),
    F.lit("0").alias("PD_AMT")
)

df_Collector_34 = df_FKey.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

df_Collector_34 = df_Collector_34.select(
    F.col("CLM_LN_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK"),
    F.col("DSALW_EXCD_SK"),
    F.col("CLM_LN_PCA_LOB_CD_SK"),
    F.col("CLM_LN_PCA_PRCS_CD_SK"),
    F.col("CNSD_AMT"),
    F.col("DSALW_AMT"),
    F.col("NONCNSD_AMT"),
    F.col("PROV_PD_AMT"),
    F.col("SUB_PD_AMT"),
    F.col("PD_AMT")
)

df_Collector_34 = df_Collector_34.withColumn("CLM_ID", rpad(F.col("CLM_ID"), <...>, " "))

write_files(
    df_Collector_34,
    f"{adls_path}/load/CLM_LN_PCA.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)