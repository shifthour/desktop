# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsFcltyValFkey
# MAGIC Calling Job(s) : IdsFctsClmLoad1Seq,IdsLhoFctsClmLoad1Seq
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     CDMA - Read only for codes lookup
# MAGIC 
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
# MAGIC             Tom Harrocks  08/2004       -   Originally Programmed
# MAGIC             Brent Leland    09/09/2004  -  Fixed default values for UNK and NA
# MAGIC             Steph Goddard 02/14/2006    Changes for sequencer
# MAGIC             Brent Leland     03/24/2006   Changed key lookup from GetFkeyFcltyClm to GetFkeyClm to match the Fclty_clm table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi Dasari         2008-08-01      3567(Primary Key)    Added new parameter SrcSysCdSk and passing it directly from source                  devlIDS                         Steph Goddard           08/07/2008
# MAGIC 
# MAGIC Reddy Sanam         2020-10-06      263447                     Changed derivation for stage variable 'FcltyClmValCdSk' to map source                IntegrateDev2
# MAGIC                                                                                         "FACETS" even if the source is LUMERIS so that the column
# MAGIC                                                                                           will be populated properly FCLTY_CLM_VAL_CD_SK
# MAGIC 
# MAGIC Sunitha Ganta         10-12-2020                               Brought up to standards

# MAGIC Lookup all the codes and ID references. If any fail send the record to the recycle bin.
# MAGIC Read common record format file.
# MAGIC Remove recycle records when a new source record with the same natural key from the  is found.
# MAGIC Writing Sequential File to /load
# MAGIC Create default rows for UNK and NA
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value("Source","")
InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")

schema_ClmFcltyCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("FCLTY_CLM_VAL_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("FCLTY_CLM_VAL_ORD", IntegerType(), False),
    StructField("CLVC_NUMBER", StringType(), False),
    StructField("CLVC_LETTER", StringType(), False),
    StructField("FCLTY_CLM_VAL_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("FCLTY_CLM_SK", IntegerType(), False),
    StructField("VAL_AMT", DecimalType(38,10), False),
    StructField("VAL_UNIT_CT", IntegerType(), False)
])

df_clmfcltycrf = (
    spark.read
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_ClmFcltyCrf)
    .csv(f"{adls_path}/key/{InFile}", header=False)
)

df_foreignKey = (
    df_clmfcltycrf
    .withColumn("ErrCount", GetFkeyErrorCnt(col("FCLTY_CLM_VAL_SK")))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("DiscardInd", col("DISCARD_IN"))
    .withColumn("FcltyClmValCdSk",
        GetFkeyCodes(
            when(col("SRC_SYS_CD") == lit("LUMERIS"), lit("FACETS")).otherwise(col("SRC_SYS_CD")),
            col("FCLTY_CLM_VAL_SK"),
            lit("FACILITY CLAIM PROCESSING VALUE"),
            col("FCLTY_CLM_VAL_CD"),
            Logging
        )
    )
    .withColumn("FcltyClmSk", GetFkeyClm(col("SRC_SYS_CD"), col("FCLTY_CLM_VAL_SK"), col("CLM_ID"), Logging))
    .withColumn("JobExecRecSK",
        when(col("ErrCount") > lit(0), GetRecycleKey(col("FCLTY_CLM_VAL_SK"))).otherwise(lit(0))
    )
    .withColumn("SrcSysCdSk", lit(SrcSysCdSk))
)

df_pkey = (
    df_foreignKey
    .filter((col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y")))
    .select(
        col("FCLTY_CLM_VAL_SK").alias("FCLTY_CLM_VAL_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("CLM_ID").alias("CLM_ID"),
        col("FCLTY_CLM_VAL_ORD").alias("FCLTY_CLM_VAL_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("FcltyClmSk").alias("FCLTY_CLM_SK"),
        col("FcltyClmValCdSk").alias("FCLTY_CLM_VAL_CD_SK"),
        col("VAL_AMT").alias("VAL_AMT"),
        col("VAL_UNIT_CT").alias("VAL_UNIT_CT")
    )
)

df_recycle = (
    df_foreignKey
    .filter(col("ErrCount") > lit(0))
    .select(
        col("JobExecRecSK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DiscardInd").alias("DISCARD_IN"),
        col("PassThru").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("FCLTY_CLM_VAL_SK").alias("FCLTY_CLM_VAL_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("CLM_ID").alias("CLM_ID"),
        col("FCLTY_CLM_VAL_ORD").alias("FCLTY_CLM_VAL_ORD"),
        col("CLVC_NUMBER").alias("CLVC_NUMBER"),
        col("CLVC_LETTER").alias("CLVC_LETTER"),
        col("FCLTY_CLM_VAL_CD").alias("FCLTY_CLM_VAL_CD"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("FcltyClmSk").alias("FCLTY_CLM_SK"),
        col("VAL_AMT").alias("VAL_AMT"),
        col("VAL_UNIT_CT").alias("VAL_UNIT_CT")
    )
)

df_defaultUNK = (
    df_foreignKey
    .limit(1)
    .select(
        lit(0).alias("FCLTY_CLM_VAL_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("CLM_ID"),
        lit(0).alias("FCLTY_CLM_VAL_SEQ_NO"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("FCLTY_CLM_SK"),
        lit(0).alias("FCLTY_CLM_VAL_CD_SK"),
        lit(0).alias("VAL_AMT"),
        lit(0).alias("VAL_UNIT_CT")
    )
)

df_defaultNA = (
    df_foreignKey
    .limit(1)
    .select(
        lit(1).alias("FCLTY_CLM_VAL_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("CLM_ID"),
        lit(1).alias("FCLTY_CLM_VAL_SEQ_NO"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("FCLTY_CLM_SK"),
        lit(1).alias("FCLTY_CLM_VAL_CD_SK"),
        lit(0).alias("VAL_AMT"),
        lit(0).alias("VAL_UNIT_CT")
    )
)

df_recycle_clms = (
    df_foreignKey
    .filter(col("ErrCount") > lit(0))
    .select(
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("CLM_ID").alias("CLM_ID")
    )
)

df_recycle_write = (
    df_recycle
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
        "FCLTY_CLM_VAL_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "FCLTY_CLM_VAL_ORD",
        "CLVC_NUMBER",
        "CLVC_LETTER",
        "FCLTY_CLM_VAL_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FCLTY_CLM_SK",
        "VAL_AMT",
        "VAL_UNIT_CT"
    )
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " "))
    .withColumn("PRI_KEY_STRING", rpad(col("PRI_KEY_STRING"), <...>, " "))
    .withColumn("CLVC_NUMBER", rpad(col("CLVC_NUMBER"), 2, " "))
    .withColumn("CLVC_LETTER", rpad(col("CLVC_LETTER"), 1, " "))
    .withColumn("FCLTY_CLM_VAL_CD", rpad(col("FCLTY_CLM_VAL_CD"), <...>, " "))
    .withColumn("CLM_ID", rpad(col("CLM_ID"), <...>, " "))
)

write_files(
    df_recycle_write,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_recycle_clms_write = (
    df_recycle_clms
    .select("SRC_SYS_CD", "CLM_ID")
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " "))
    .withColumn("CLM_ID", rpad(col("CLM_ID"), <...>, " "))
)

write_files(
    df_recycle_clms_write,
    "hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_lnkcoll = df_pkey.unionByName(df_defaultUNK).unionByName(df_defaultNA)

df_lnkcoll_write = (
    df_lnkcoll
    .select(
        "FCLTY_CLM_VAL_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "FCLTY_CLM_VAL_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FCLTY_CLM_SK",
        "FCLTY_CLM_VAL_CD_SK",
        "VAL_AMT",
        "VAL_UNIT_CT"
    )
    .withColumn("CLM_ID", rpad(col("CLM_ID"), <...>, " "))
)

write_files(
    df_lnkcoll_write,
    f"{adls_path}/load/FCLTY_CLM_VAL.{Source}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)