# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmAtchmntFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.  Output is final table format for CLM_ATCHMT
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  File from IdsClmAtchmtPkey in common record format.
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle 
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:  Assigns foreign keys to claim attachment record.
# MAGIC 
# MAGIC OUTPUTS: CLM_ATCHMT record to be loaded to IDS.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Steph Goddard  06/2004        -   Originally Programmed
# MAGIC             Brent Leland      09/09/2004  - Added Default rows for UNK and NA
# MAGIC                                                            - Split processing into 4 streams.
# MAGIC             Steph Goddard  02/09/2006   Sequencer changes
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi Dasari         2008-07-28      3567(Primary Key)    Added new parameter SrcSysCdSk and passing it directly from source                  devlIDS                         Steph Goddard          07/29/2008
# MAGIC 
# MAGIC Reddy Sanam         2020-10-09                                      change derivation for this stage variable -ClmAtchmtTypCdSk to map to
# MAGIC                                                                                         FACETS when the source is LUMERIS
# MAGIC Sunitha Ganta         2020-10-12                                      Brought up to standards

# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, when, row_number, monotonically_increasing_id, rpad
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','Y')
InFile = get_widget_value('InFile','FctsClmAtchmtExtr.FctsClmAtchmt.dat.20080715')
Source = get_widget_value('Source','FACETS')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_IdsClmAtchmtExtr = (
    StructType()
    .add(StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False))
    .add(StructField("INSRT_UPDT_CD", StringType(), nullable=False))
    .add(StructField("DISCARD_IN", StringType(), nullable=False))
    .add(StructField("PASS_THRU_IN", StringType(), nullable=False))
    .add(StructField("FIRST_RECYC_DT", TimestampType(), nullable=False))
    .add(StructField("ERR_CT", IntegerType(), nullable=False))
    .add(StructField("RECYCLE_CT", DecimalType(38,10), nullable=False))
    .add(StructField("SRC_SYS_CD", StringType(), nullable=False))
    .add(StructField("PRI_KEY_STRING", StringType(), nullable=False))
    .add(StructField("CLM_ATCHMT_SK", IntegerType(), nullable=False))
    .add(StructField("CLM_ID", StringType(), nullable=False))
    .add(StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False))
    .add(StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False))
    .add(StructField("ATCHMT_IND", StringType(), nullable=False))
    .add(StructField("ATCHMT_REF_ID", StringType(), nullable=False))
)

df_IdsClmAtchmtExtr = (
    spark.read
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_IdsClmAtchmtExtr)
    .csv(f"{adls_path}/key/{InFile}", header=False)
)

df_enriched_base = (
    df_IdsClmAtchmtExtr
    .withColumn(
        "ClmSk",
        GetFkeyClm(
            col("SRC_SYS_CD"),
            col("CLM_ATCHMT_SK"),
            col("CLM_ID"),
            Logging
        )
    )
    .withColumn(
        "ClmAtchmtTypCdSk",
        GetFkeyCodes(
            when(col("SRC_SYS_CD")=="LUMERIS","FACETS").otherwise(col("SRC_SYS_CD")),
            col("CLM_ATCHMT_SK"),
            lit("CLAIM ATTACHMENT TYPE"),
            col("ATCHMT_IND"),
            Logging
        )
    )
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(
            col("CLM_ATCHMT_SK")
        )
    )
)

window_all = Window.orderBy(F.monotonically_increasing_id())
df_enriched = df_enriched_base.withColumn("rownum", F.row_number().over(window_all))

df_enriched_fkey1 = df_enriched.filter((col("ErrCount")==0) | (col("PassThru")==lit("Y")))
df_enriched_recycle1 = df_enriched.filter(col("ErrCount")>0)
df_enriched_defaultunk = df_enriched.filter(col("rownum")==1)
df_enriched_defaultna = df_enriched.filter(col("rownum")==1)
df_enriched_recycle_clms = df_enriched.filter(col("ErrCount")>0)

df_fkey1 = df_enriched_fkey1.select(
    col("CLM_ATCHMT_SK").alias("CLM_ATCHMT_SK"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("ClmAtchmtTypCdSk").alias("CLM_ATCHMT_TYP_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ClmSk").alias("CLM_SK"),
    col("ATCHMT_REF_ID").alias("ATCHMT_REF_ID")
)

df_recycle1 = df_enriched_recycle1.select(
    GetRecycleKey(col("CLM_ATCHMT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    (col("RECYCLE_CT")+lit(1)).alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("CLM_ATCHMT_SK").alias("CLM_ATCHMT_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ATCHMT_IND").alias("ATCHMT_IND"),
    col("ATCHMT_REF_ID").alias("ATCHMT_REF_ID")
)

df_defaultunk = df_enriched_defaultunk.select(
    lit(0).alias("CLM_ATCHMT_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("CLM_ID"),
    lit(0).alias("CLM_ATCHMT_TYP_CD_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLM_SK"),
    lit("UNK").alias("ATCHMT_REF_ID")
)

df_defaultna = df_enriched_defaultna.select(
    lit(1).alias("CLM_ATCHMT_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("CLM_ID"),
    lit(1).alias("CLM_ATCHMT_TYP_CD_SK"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("CLM_SK"),
    lit("NA").alias("ATCHMT_REF_ID")
)

df_recycle_clms = df_enriched_recycle_clms.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID")
)

write_files(
    df_recycle1,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

write_files(
    df_recycle_clms,
    "hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_collector = (
    df_fkey1.select(
        "CLM_ATCHMT_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_ATCHMT_TYP_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "ATCHMT_REF_ID"
    )
    .unionByName(
        df_defaultunk.select(
            "CLM_ATCHMT_SK",
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "CLM_ATCHMT_TYP_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "CLM_SK",
            "ATCHMT_REF_ID"
        )
    )
    .unionByName(
        df_defaultna.select(
            "CLM_ATCHMT_SK",
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "CLM_ATCHMT_TYP_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "CLM_SK",
            "ATCHMT_REF_ID"
        )
    )
)

df_collector_rpad = (
    df_collector
    .withColumn("CLM_ID", rpad(col("CLM_ID"), 18, " "))
    .withColumn("ATCHMT_REF_ID", rpad(col("ATCHMT_REF_ID"), 100, " "))
)

df_collector_final = df_collector_rpad.select(
    "CLM_ATCHMT_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_ATCHMT_TYP_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "ATCHMT_REF_ID"
)

write_files(
    df_collector_final,
    f"{adls_path}/load/CLM_ATCHMT.{Source}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)