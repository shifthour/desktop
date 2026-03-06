# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_3 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 08/30/07 07:19:35 Batch  14487_26382 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 10:06:05 Batch  14484_36371 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 06/21/07 14:03:38 Batch  14417_50622 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 05/25/07 11:20:32 Batch  14390_40859 PROMOTE bckcetl ids20 dsadm rc for steph 
# MAGIC ^1_1 05/25/07 11:11:59 Batch  14390_40332 INIT bckcett testIDS30 dsadm rc for steph 
# MAGIC ^1_1 05/16/07 11:48:45 Batch  14381_42530 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 05/16/07 11:43:58 Batch  14381_42243 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC Â© Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  Fcts claim line savings sequencer
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Apply foreign keys for claim line savings table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Steph Goddard         4/25/2007       HDMS                   New Program                                                                              devlIDS30                       Brent Leland              05/15/2007

# MAGIC Writing Sequential File to /load
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC Recycle records with ErrCount > 0
# MAGIC Read common record format file from extract job.
# MAGIC Merge source data with default rows
# MAGIC Set all foreign surrogate keys
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','N')
RunCycle = get_widget_value('RunCycle','100')
RunID = get_widget_value('RunID','')

schema_ClmLnSavExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38, 10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("CLM_LN_SAV_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), nullable=False),
    StructField("CLM_LN_SAV_TYP_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CLM_LN_SK", IntegerType(), nullable=False),
    StructField("SAV_AMT", DecimalType(38, 10), nullable=False)
])

df_ClmLnSavExtr = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_ClmLnSavExtr)
    .csv(f"{adls_path}/key/IdsClmLnSavExtr.ClmLnSav.dat.{RunID}")
)

w = Window.orderBy(lit(1))

df_ForeignKey_stagevars = (
    df_ClmLnSavExtr
    .withColumn("SrcSysCdSk", GetFkeyCodes(lit("IDS"), col("CLM_LN_SAV_SK"), lit("SOURCE SYSTEM"), col("SRC_SYS_CD"), lit(Logging)))
    .withColumn("ClmLnSavTypCd", GetFkeyCodes(lit("IDS"), col("CLM_LN_SAV_SK"), lit("CLAIM LINE SAVINGS TYPE"), col("CLM_LN_SAV_TYP_CD"), lit(Logging)))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("CLM_LN_SAV_SK")))
    .withColumn("row_number", row_number().over(w))
)

df_Fkey = (
    df_ForeignKey_stagevars
    .filter((col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y")))
    .select(
        col("CLM_LN_SAV_SK").alias("CLM_LN_SAV_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("CLM_ID").alias("CLM_ID"),
        col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("ClmLnSavTypCd").alias("CLM_LN_SAV_TYP_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CLM_LN_SK").alias("CLM_LN_SK"),
        col("SAV_AMT").alias("SAV_AMT")
    )
)

df_DefaultUNK = (
    df_ForeignKey_stagevars
    .filter(col("row_number") == lit(1))
    .select(
        lit(0).alias("CLM_LN_SAV_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("CLM_ID"),
        lit(0).alias("CLM_LN_SEQ_NO"),
        lit(0).alias("CLM_LN_SAV_TYP_CD_SK"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("CLM_LN_SK"),
        lit(0.00).alias("SAV_AMT")
    )
)

df_DefaultNA = (
    df_ForeignKey_stagevars
    .filter(col("row_number") == lit(1))
    .select(
        lit(1).alias("CLM_LN_SAV_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("CLM_ID"),
        lit(0).alias("CLM_LN_SEQ_NO"),
        lit(1).alias("CLM_LN_SAV_TYP_CD_SK"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("CLM_LN_SK"),
        lit(0.00).alias("SAV_AMT")
    )
)

df_Recycle = (
    df_ForeignKey_stagevars
    .filter(col("ErrCount") > lit(0))
    .select(
        GetRecycleKey(col("CLM_LN_SAV_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("CLM_LN_SAV_SK").alias("CLM_LN_SAV_SK"),
        col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("CLM_ID").alias("CLM_ID"),
        col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("CLM_LN_SAV_TYP_CD").alias("CLM_LN_SAV_TYP_CD"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CLM_LN_SK").alias("CLM_LN_SK"),
        col("SAV_AMT").alias("SAV_AMT")
    )
)

df_Recycle_Clms = (
    df_ForeignKey_stagevars
    .filter(col("ErrCount") > lit(0))
    .select(
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("CLM_ID").alias("CLM_ID")
    )
)

df_Recycle_rpad = (
    df_Recycle
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " "))
    .withColumn("PRI_KEY_STRING", rpad(col("PRI_KEY_STRING"), <...>, " "))
    .withColumn("CLM_ID", rpad(col("CLM_ID"), <...>, " "))
    .withColumn("CLM_LN_SAV_TYP_CD", rpad(col("CLM_LN_SAV_TYP_CD"), <...>, " "))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_LN_SAV_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_SAV_TYP_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_SK",
        "SAV_AMT"
    )
)

write_files(
    df_Recycle_rpad,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Recycle_Clms_rpad = (
    df_Recycle_Clms
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " "))
    .withColumn("CLM_ID", rpad(col("CLM_ID"), <...>, " "))
    .select("SRC_SYS_CD", "CLM_ID")
)

write_files(
    df_Recycle_Clms_rpad,
    "hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_collRows = df_Fkey.union(df_DefaultUNK).union(df_DefaultNA)

df_collRows_rpad = (
    df_collRows
    .withColumn("CLM_ID", rpad(col("CLM_ID"), <...>, " "))
    .select(
        "CLM_LN_SAV_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_SAV_TYP_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_SK",
        "SAV_AMT"
    )
)

write_files(
    df_collRows_rpad,
    f"{adls_path}/load/CLM_LN_SAV.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)