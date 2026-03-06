# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_3 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 08/30/07 07:19:35 Batch  14487_26382 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 10:06:05 Batch  14484_36371 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/26/07 12:34:37 Batch  14330_45286 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 03/07/07 15:24:05 Batch  14311_55447 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 12/18/06 15:24:00 Batch  14232_55459 PROMOTE bckcetl ids20 dsadm Keith for Ralph install 12/18/2006
# MAGIC ^1_1 12/18/06 14:39:27 Batch  14232_52797 INIT bckcett testIDS30 dsadm Keith for Ralph CDHP Install 12182006
# MAGIC ^1_1 11/17/06 13:46:03 Batch  14201_49568 PROMOTE bckcett testIDS30 u06640 Ralph
# MAGIC ^1_1 11/17/06 11:12:19 Batch  14201_40344 INIT bckcett devlIDS30 u06640 Ralph

# MAGIC FctsClmPca
# MAGIC 
# MAGIC IDS Claim Cond Foreign Keying
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","\\\"Y\\\"")
RunCycle = get_widget_value("RunCycle","100")
RunID = get_widget_value("RunID","15405")

schema_Fcts_Clm_Pca_Extr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", StringType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("CLM_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("TOT_CNSD_AMT", DecimalType(), nullable=False),
    StructField("TOT_PD_AMT", DecimalType(), nullable=False),
    StructField("SUB_TOT_PCA_AVLBL_TO_DT_AMT", DecimalType(), nullable=False),
    StructField("SUB_TOT_PCA_PD_TO_DT_AMT", DecimalType(), nullable=False)
])

df_Fcts_Clm_Pca_Extr = (
    spark.read
    .option("header", "false")
    .option("inferSchema", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_Fcts_Clm_Pca_Extr)
    .csv(f"{adls_path}/key/FctsClmPcaExtr.ClmPca.uniq")
)

df_stageVars = (
    df_Fcts_Clm_Pca_Extr
    .withColumn("SrcSysCdSk", GetFkeyCodes("IDS", F.col("CLM_SK"), "SOURCE SYSTEM", F.col("SRC_SYS_CD"), Logging))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCt", F.col("ERR_CT"))
    .withColumn("DiscardIn", F.lit("N"))
)

df_Recycle = (
    df_stageVars
    .filter(F.col("ErrCt") > 0)
    .select(
        GetRecycleKey(F.col("CLM_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DiscardIn").alias("DISCARD_IN"),
        F.col("PassThru").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCt").alias("ERR_CT"),
        (F.col("RECYCLE_CT").cast("int") + F.lit(1)).cast("string").alias("RECYCLE_CT"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("TOT_CNSD_AMT").alias("TOT_CNSD_AMT"),
        F.col("TOT_PD_AMT").alias("TOT_PD_AMT"),
        F.col("SUB_TOT_PCA_AVLBL_TO_DT_AMT").alias("SUB_TOT_PCA_AVLBL_TO_DT_AMT"),
        F.col("SUB_TOT_PCA_PD_TO_DT_AMT").alias("SUB_TOT_PCA_PD_TO_DT_AMT")
    )
)

write_files(
    df_Recycle,
    f"Recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_FKey = (
    df_stageVars
    .filter((F.col("ErrCt") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("TOT_CNSD_AMT").alias("TOT_CNSD_AMT"),
        F.col("TOT_PD_AMT").alias("TOT_PD_AMT"),
        F.col("SUB_TOT_PCA_AVLBL_TO_DT_AMT").alias("SUB_TOT_PCA_AVLBL_TO_DT_AMT"),
        F.col("SUB_TOT_PCA_PD_TO_DT_AMT").alias("SUB_TOT_PCA_PD_TO_DT_AMT")
    )
)

df_DefaultUNK = (
    df_stageVars
    .orderBy(F.lit(1))
    .limit(1)
    .select(
        F.lit(0).alias("CLM_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("CLM_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("TOT_CNSD_AMT"),
        F.lit(0).alias("TOT_PD_AMT"),
        F.lit(0).alias("SUB_TOT_PCA_AVLBL_TO_DT_AMT"),
        F.lit(0).alias("SUB_TOT_PCA_PD_TO_DT_AMT")
    )
)

df_DefaultNA = (
    df_stageVars
    .orderBy(F.lit(1))
    .limit(1)
    .select(
        F.lit(1).alias("CLM_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("CLM_ID"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("TOT_CNSD_AMT"),
        F.lit(1).alias("TOT_PD_AMT"),
        F.lit(1).alias("SUB_TOT_PCA_AVLBL_TO_DT_AMT"),
        F.lit(1).alias("SUB_TOT_PCA_PD_TO_DT_AMT")
    )
)

df_Link_Collector_34 = (
    df_FKey.unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
)

df_final = (
    df_Link_Collector_34
    .withColumn("SRC_SYS_CD_SK", F.rpad(F.col("SRC_SYS_CD_SK"), 20, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), <...>, " "))
    .select(
        "CLM_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "TOT_CNSD_AMT",
        "TOT_PD_AMT",
        "SUB_TOT_PCA_AVLBL_TO_DT_AMT",
        "SUB_TOT_PCA_PD_TO_DT_AMT"
    )
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_PCA.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)