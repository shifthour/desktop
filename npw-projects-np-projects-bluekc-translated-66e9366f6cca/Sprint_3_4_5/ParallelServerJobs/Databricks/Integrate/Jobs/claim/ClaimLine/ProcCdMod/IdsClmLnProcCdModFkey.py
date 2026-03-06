# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmLnProcCdModFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     ProvHsh    - this is both read from and written to.   
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Tom Harrocks  04/12/2004  -   Originally Programmed
# MAGIC             Brent Leland    08/31/2004  -   Added default rows for NA and UNK
# MAGIC                                                          -   Added interprocess stage.
# MAGIC             Suzanne Saylor 03/01/2006 - Removed used parameters and renamed links
# MAGIC 
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------    
# MAGIC Parik                        2008-08-08      3567(Primary Key)   bring source system SK from parameter                                       devlIDS                          Steph Goddard           08/13/2008
# MAGIC 
# MAGIC Parik                       2008-08-13      3567(Primary Key)    Changed it back to original since pseudo claim is involved         devlIDS
# MAGIC                                                                                         and it has various source system codes
# MAGIC 
# MAGIC Kalyan Neelam       2014-12-17           5212                  Added If SRC_SYS_CD = 'BCBSA' Then 'BCA' Else SRC_SYS_CD    IntegrateCurDevl     Bhoomi Dasari             02/04/2015
# MAGIC                                                                                      in the stage variables and pass it to GetFkeyCodes because code sets are created under BCA for BCBSA
# MAGIC Reddy Sanam        2020-10-11                                    Changed buffer size and timeout to bring the job up to standards
# MAGIC                                                                                      Mapped FACETS when source is LUMERIS source the cdoe so
# MAGIC                                                                                      type code are retrieved from FACETS

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC SrcSysCdSk lookup done here because of pseudo claim's multiple sources per batch
# MAGIC Changed source system name BCBSA to LUMERIS
# MAGIC Merge source data with default rows
# MAGIC Recycle records with ErrCount > 0
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import Window
from pyspark.sql.functions import col, lit, when, row_number, rpad, trim
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value('Source', '')
InFile = get_widget_value('InFile', 'IdsClmLnProcCdMod.ProcCdMod.RUNID')
Logging = get_widget_value('Logging', 'Y')

schema_PCModIn = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CLM_LN_PROC_CD_MOD_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("CLM_LN_PROC_CD_MOD_ORDNL_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("PROC_CD_MOD_CD", StringType(), False)
])

df_PCModIn = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("escape", "\"")
    .schema(schema_PCModIn)
    .load(f"{adls_path}/key/{InFile}")
)

w = Window.orderBy(lit(1))
df_enriched = (
    df_PCModIn
    .withColumn("rownum", row_number().over(w))
    .withColumn(
        "svCdMpngSrcSysCd",
        when(col("SRC_SYS_CD") == 'BCBSA', 'BCA')
        .when(col("SRC_SYS_CD") == 'LUMERIS', 'FACETS')
        .otherwise(col("SRC_SYS_CD"))
    )
    .withColumn(
        "ClmLnSk",
        GetFkeyClmLn(
            col("SRC_SYS_CD"),
            col("CLM_LN_PROC_CD_MOD_SK"),
            trim(col("CLM_ID")),
            trim(col("CLM_LN_SEQ_NO")),
            Logging
        )
    )
    .withColumn(
        "ProcCdOrdCd",
        GetFkeyCodes(
            col("svCdMpngSrcSysCd"),
            col("CLM_LN_PROC_CD_MOD_SK"),
            lit("PROCEDURE ORDINAL"),
            trim(col("CLM_LN_PROC_CD_MOD_ORDNL_CD")),
            Logging
        )
    )
    .withColumn(
        "SrcSysCdSk",
        GetFkeyCodes(
            lit("IDS"),
            col("CLM_LN_PROC_CD_MOD_SK"),
            lit("SOURCE SYSTEM"),
            col("SRC_SYS_CD"),
            Logging
        )
    )
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("CLM_LN_PROC_CD_MOD_SK")))
)

df_Fkey1 = df_enriched.filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
df_Recycle1 = df_enriched.filter(col("ErrCount") > 0)
df_DefaultUNK = df_enriched.filter(col("rownum") == 1)
df_DefaultNA = df_enriched.filter(col("rownum") == 1)
df_Recycle_Clms = df_enriched.filter(col("ErrCount") > 0)

df_Fkey1_sel = df_Fkey1.select(
    col("CLM_LN_PROC_CD_MOD_SK").alias("CLM_LN_PROC_CD_MOD_SK"),
    col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("ProcCdOrdCd").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ClmLnSk").alias("CLM_LN_SK"),
    col("PROC_CD_MOD_CD").alias("PROC_CD_MOD_TX")
)

df_Recycle1_sel = df_Recycle1.select(
    GetRecycleKey(col("CLM_LN_PROC_CD_MOD_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("CLM_LN_PROC_CD_MOD_SK").alias("CLM_LN_PROC_CD_MOD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PROC_CD_MOD_CD").alias("PROC_CD_MOD")
)

write_files(
    df_Recycle1_sel,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_Recycle_Clms_sel = df_Recycle_Clms.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID")
)

write_files(
    df_Recycle_Clms_sel,
    f"{adls_path}/hf_claim_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_DefaultUNK_sel = df_DefaultUNK.select(
    lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("CLM_ID"),
    lit(0).alias("CLM_LN_SEQ_NO"),
    lit(0).alias("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLM_LN_SK"),
    lit("UN").alias("PROC_CD_MOD_TX")
)

df_DefaultNA_sel = df_DefaultNA.select(
    lit(1).alias("CLM_LN_PROC_CD_MOD_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("CLM_ID"),
    lit(1).alias("CLM_LN_SEQ_NO"),
    lit(1).alias("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("CLM_LN_SK"),
    lit("NA").alias("PROC_CD_MOD_TX")
)

df_Collector = (
    df_Fkey1_sel
    .union(df_DefaultUNK_sel)
    .union(df_DefaultNA_sel)
)

df_Collector_final = (
    df_Collector
    .withColumn("PROC_CD_MOD_TX", rpad("PROC_CD_MOD_TX", 2, " "))
    .select(
        "CLM_LN_PROC_CD_MOD_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_PROC_CD_MOD_ORDNL_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_SK",
        "PROC_CD_MOD_TX"
    )
)

write_files(
    df_Collector_final,
    f"{adls_path}/load/CLM_LN_PROC_CD_MOD.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)