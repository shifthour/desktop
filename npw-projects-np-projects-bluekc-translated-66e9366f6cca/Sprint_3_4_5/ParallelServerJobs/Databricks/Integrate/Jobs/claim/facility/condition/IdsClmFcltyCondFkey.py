# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsFcltyCondFkey
# MAGIC Calling Job: This is a foreign Key facility condition Job. This gets called for Fkey assignment for both FACETS and LHOFACETSSTAGE database (IdsFctsClmLoad1Seq,IdsLhoFctsClmLoad1Seq)
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
# MAGIC             Tom Harrocks  08/2004  -   Originally Programmed
# MAGIC             Brent Leland    09/09/2004  - Corrected default row values.
# MAGIC             Steph Goddard 02/13/2006   Changed for sequencer
# MAGIC             Brent Leland     03/24/2006   Changed key lookup from GetFkeyFcltyClm to GetFkeyClm to match the Fclty_clm table.
# MAGIC             Steph Goddard 06/26/2006   Changed lookup for recycle key - production support issue
# MAGIC                                                             primary key string was blank on recycle reports 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi Dasari         2008-07-28      3567(Primary Key)    Added new parameter SrcSysCdSk and passing it directly from source                  devlIDS                          Steph Goddard          07/29/2008
# MAGIC 
# MAGIC 
# MAGIC Reddy Sanam         2020-09-28                                      Changed mapping for this stage variable -FcltyClmCondCdSk to Map 
# MAGIC                                                                                        "FACETS" as the source when calling the routine "GetFkeyCodes"                       IntegrateDev2
# MAGIC 
# MAGIC Sunitha Ganta         10-11-2020                                      Brought up to standards

# MAGIC Lookup all the codes and ID references. If any fail send the record to the recycle bin.
# MAGIC Read common record format file.
# MAGIC Remove recycle records when a new source record with the same natural key from the  is found.
# MAGIC Writing Sequential File to /load
# MAGIC Add default rows for UNK and NA
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

Source = get_widget_value('Source','')
InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_IdsFcltyClmExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(15, 2), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("FCLTY_CLM_COND_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("FCLTY_CLM_COND_SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("FCLTY_CLM_SK", IntegerType(), False),
    StructField("FCLTY_CLM_COND_CD", StringType(), False)
])

df_IdsFcltyClmExtr = spark.read.csv(
    path=f"{adls_path}/key/{InFile}",
    schema=schema_IdsFcltyClmExtr,
    sep=",",
    quote="\"",
    header=False
)

df_foreignKey = (
    df_IdsFcltyClmExtr
    .withColumn(
        "FcltyClmCondCdSk",
        GetFkeyCodes(
            F.when(F.col("SRC_SYS_CD") == "LUMERIS", "FACETS").otherwise(F.col("SRC_SYS_CD")),
            F.col("FCLTY_CLM_COND_SK"),
            F.lit("FACILITY CLAIM CONDITION"),
            F.col("FCLTY_CLM_COND_CD"),
            Logging
        )
    )
    .withColumn(
        "FcltyClmSk",
        GetFkeyClm(
            F.col("SRC_SYS_CD"),
            F.col("FCLTY_CLM_COND_SK"),
            F.col("CLM_ID"),
            Logging
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("FCLTY_CLM_COND_SK")))
    .withColumn("DiscardInd", F.lit("N"))
)

df_Fkey = (
    df_foreignKey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("FCLTY_CLM_COND_SK").alias("FCLTY_CLM_COND_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("FCLTY_CLM_COND_SEQ_NO").alias("FCLTY_CLM_COND_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("FcltyClmSk").alias("FCLTY_CLM_SK"),
        F.col("FcltyClmCondCdSk").alias("FCLTY_CLM_COND_CD_SK")
    )
)

df_recycle = (
    df_foreignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("FCLTY_CLM_COND_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DiscardInd").alias("DISCARD_IN"),
        F.col("PassThru").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("FCLTY_CLM_COND_SK").alias("FCLTY_CLM_COND_SK"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("FCLTY_CLM_COND_SEQ_NO").alias("FCLTY_CLM_COND_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("FCLTY_CLM_SK").alias("FCLTY_CLM_SK"),
        F.col("FCLTY_CLM_COND_CD").alias("FCLTY_CLM_COND_CD")
    )
)

df_rownum = df_foreignKey.withColumn("__row_num", F.row_number().over(Window.orderBy(F.lit(1))))
df_firstRow = df_rownum.filter(F.col("__row_num") == 1)

df_DefaultUNK = df_firstRow.select(
    F.lit(0).alias("FCLTY_CLM_COND_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit(0).alias("FCLTY_CLM_COND_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("FCLTY_CLM_SK"),
    F.lit(0).alias("FCLTY_CLM_COND_CD_SK")
)

df_DefaultNA = df_firstRow.select(
    F.lit(1).alias("FCLTY_CLM_COND_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("CLM_ID"),
    F.lit(1).alias("FCLTY_CLM_COND_SEQ_NO"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("FCLTY_CLM_SK"),
    F.lit(1).alias("FCLTY_CLM_COND_CD_SK")
)

df_Recycle_Clms = (
    df_foreignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID")
    )
)

df_recycle_rpad = (
    df_recycle
    .withColumn("INSRT_UPDT_CD", rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("SRC_SYS_CD", rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("PRI_KEY_STRING", rpad(F.col("PRI_KEY_STRING"), <...>, " "))
    .withColumn("CLM_ID", rpad(F.col("CLM_ID"), <...>, " "))
    .withColumn("FCLTY_CLM_COND_CD", rpad(F.col("FCLTY_CLM_COND_CD"), <...>, " "))
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
        "FCLTY_CLM_COND_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "FCLTY_CLM_COND_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FCLTY_CLM_SK",
        "FCLTY_CLM_COND_CD"
    )
)

write_files(
    df_recycle_rpad,
    "hf_recycle.parquet",
    ',',
    'overwrite',
    True,
    True,
    '"',
    None
)

df_Recycle_Clms_rpad = (
    df_Recycle_Clms
    .withColumn("SRC_SYS_CD", rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("CLM_ID", rpad(F.col("CLM_ID"), <...>, " "))
    .select("SRC_SYS_CD", "CLM_ID")
)

write_files(
    df_Recycle_Clms_rpad,
    "hf_claim_recycle_keys.parquet",
    ',',
    'overwrite',
    True,
    True,
    '"',
    None
)

df_Collector = df_Fkey.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)
df_Collector = df_Collector.select(
    "FCLTY_CLM_COND_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "FCLTY_CLM_COND_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "FCLTY_CLM_SK",
    "FCLTY_CLM_COND_CD_SK"
)

df_Collector_rpad = df_Collector.withColumn("CLM_ID", rpad(F.col("CLM_ID"), <...>, " "))

write_files(
    df_Collector_rpad,
    f"FCLTY_CLM_COND.{Source}.dat",
    ',',
    'overwrite',
    False,
    True,
    '"',
    None
)