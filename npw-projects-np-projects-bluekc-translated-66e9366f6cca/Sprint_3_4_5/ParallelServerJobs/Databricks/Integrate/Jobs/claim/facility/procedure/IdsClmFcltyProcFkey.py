# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC   
# MAGIC HASH FILES:     ProdLkupHsh    - this is both read from and written to.   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Steph Goddard  04/2004        -   Originally Programmed
# MAGIC             Ralph Tucker    09/01/2004  - Added Proc_Type_Cd   "M" / "D" from trns.  2.0 change.  
# MAGIC             Brent Leland      09/10/2004  -  Added default rows for UNK and NA
# MAGIC             Steph Goddard  02/14/2006    Sequencer changes
# MAGIC             Brent Leland      03/24/2006   Changed key lookup from GetFkeyFcltyClm to GetFkeyClm to match the Fclty_clm table.
# MAGIC             Bhoomi Dasari   04/10/2007   Added new field PROC_CD_MOD_TX, direct mapping from primary key process.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi Dasari         2008-07-31      3567(Primary Key)    Added new parameter SrcSysCdSk and passing it directly from source                  devlIDS                          Steph Goddard          08/07/2008
# MAGIC Rick Henry              2012-04-27       4896                       Cleaned up Display and Data Element & Added PROC_CD_CAT_CD                    NewDevl                       Sandrew                      2012-05-20
# MAGIC                                                                                         PROC_CD_SK lookup
# MAGIC SA removed -  If Key.SRC_SYS_CD='BCBSSC' Then GetFkeyProcCd("FACETS", Key.CLM_PROC_SK, Key.PROC_TYPE_CD, Key.PROC_TYPE_CD, Key.PROC_CD_CAT_CD,  Logging) Else GetFkeyProcCd("FACETS", Key.CLM_PROC_SK, sProcCd, Key.PROC_TYPE_CD, Key.PROC_CD_CAT_CD,  Logging)
# MAGIC Kalyan Neelam       2014-12-17           5212                  Added If SRC_SYS_CD = 'BCBSA' Then 'BCA' Else SRC_SYS_CD                         IntegrateCurDevl               Bhoomi Dasari          02/04/2015
# MAGIC                                                                                      in the stage variables and pass it to GetFkeyCodes because code sets are created under BCA for BCBSA
# MAGIC Reddy Sanam        2020-10-09                                    changed "svCdMpngSrcSysCd" stage variable derivation to pass
# MAGIC                                                                                     'FACETS' for 'LUMERIS'
# MAGIC 
# MAGIC Sunitha Ganta         10-12-2020                                Brought up to standards

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC check for foreign keys - write out record to recycle file if errors
# MAGIC Create default rows for UNK and NA
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value('Source','')
InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_clmproc_crf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(10,0), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CLM_PROC_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("FCLTY_CLM_PROC_ORDNL_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXTCN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXTCN_SK", IntegerType(), False),
    StructField("PROC_CD", StringType(), False),
    StructField("PROC_DT", StringType(), False),
    StructField("PROC_TYPE_CD", StringType(), False),
    StructField("PROC_CD_MOD_TX", StringType(), False),
    StructField("PROC_CD_CAT_CD", StringType(), False)
])

df_clmproc_crf = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_clmproc_crf)
    .load(f"{adls_path}/key/{InFile}")
)

df_key = df_clmproc_crf.withColumn(
    "svCdMpngSrcSysCd",
    F.when(F.col("SRC_SYS_CD") == 'BCBSA', F.lit('BCA'))
     .when(F.col("SRC_SYS_CD") == 'LUMERIS', F.lit('FACETS'))
     .otherwise(F.col("SRC_SYS_CD"))
).withColumn(
    "ProcOrdnlCdSk",
    GetFkeyCodes(
        F.col("svCdMpngSrcSysCd"),
        F.col("CLM_PROC_SK"),
        F.lit("PROCEDURE ORDINAL"),
        F.col("FCLTY_CLM_PROC_ORDNL_CD"),
        F.lit(Logging)
    )
).withColumn(
    "FcltyClmSk",
    GetFkeyClm(
        F.col("SRC_SYS_CD"),
        F.col("CLM_PROC_SK"),
        F.col("CLM_ID"),
        F.lit(Logging)
    )
).withColumn(
    "sProcCd",
    F.when(F.col("PROC_TYPE_CD") != 'ICD10', F.substring(F.col("PROC_CD"), 1, 5))
     .otherwise(F.col("PROC_CD"))
).withColumn(
    "ProcCdSk",
    GetFkeyProcCd(
        F.lit("FACETS"),
        F.col("CLM_PROC_SK"),
        F.col("sProcCd"),
        F.col("PROC_TYPE_CD"),
        F.col("PROC_CD_CAT_CD"),
        F.lit(Logging)
    )
).withColumn(
    "ProcDtSk",
    GetFkeyDate(
        F.lit("IDS"),
        F.col("CLM_PROC_SK"),
        F.col("PROC_DT"),
        F.lit(Logging)
    )
).withColumn(
    "PassThru",
    F.col("PASS_THRU_IN")
).withColumn(
    "ErrCount",
    GetFkeyErrorCnt(F.col("CLM_PROC_SK"))
).withColumn(
    "JobExecRecSK",
    F.when(F.col("ErrCount") > 0, GetRecycleKey(F.col("CLM_PROC_SK"))).otherwise(F.lit(0))
)

df_FcltyClmProcOut1 = df_key.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y')
).select(
    F.col("CLM_PROC_SK").alias("FCLTY_CLM_PROC_SK"),
    F.lit(SrcSysCdSk).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("ProcOrdnlCdSk").alias("FCLTY_CLM_PROC_ORDNL_CD_SK"),
    F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("FcltyClmSk").alias("FCLTY_CLM_SK"),
    F.col("ProcCdSk").alias("PROC_CD_SK"),
    F.col("ProcDtSk").alias("PROC_DT_SK"),
    F.col("PROC_CD_MOD_TX").alias("PROC_CD_MOD_TX")
)

df_recycle = df_key.filter(F.col("ErrCount") > 0).select(
    F.col("JobExecRecSK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("PRI_KEY_STRING"), <...>, " ").alias("PRI_KEY_STRING"),
    F.col("CLM_PROC_SK").alias("CLM_PROC_SK"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.rpad(F.col("FCLTY_CLM_PROC_ORDNL_CD"), 2, " ").alias("FCLTY_CLM_PROC_ORDNL_CD"),
    F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.rpad(F.col("PROC_CD"), <...>, " ").alias("PROC_CD"),
    F.rpad(F.col("PROC_DT"), 10, " ").alias("PROC_DT"),
    F.rpad(F.col("PROC_TYPE_CD"), 1, " ").alias("PROC_TYPE_CD"),
    F.rpad(F.col("PROC_CD_MOD_TX"), 2, " ").alias("PROC_CD_MOD_TX")
)

write_files(
    df_recycle,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_recycle_clms = df_key.filter(F.col("ErrCount") > 0).select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID")
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

df_defaultUNK = df_key.limit(1).select(
    F.lit(0).alias("FCLTY_CLM_PROC_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.rpad(F.lit("UNK"), <...>, " ").alias("CLM_ID"),
    F.lit(0).alias("FCLTY_CLM_PROC_ORDNL_CD_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.lit(0).alias("FCLTY_CLM_SK"),
    F.lit(0).alias("PROC_CD_SK"),
    F.rpad(F.lit("UNK"), 10, " ").alias("PROC_DT_SK"),
    F.rpad(F.lit("U"), 2, " ").alias("PROC_CD_MOD_TX")
)

df_defaultNA = df_key.limit(1).select(
    F.lit(1).alias("FCLTY_CLM_PROC_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.rpad(F.lit("NA"), <...>, " ").alias("CLM_ID"),
    F.lit(1).alias("FCLTY_CLM_PROC_ORDNL_CD_SK"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.lit(1).alias("FCLTY_CLM_SK"),
    F.lit(1).alias("PROC_CD_SK"),
    F.rpad(F.lit("NA"), 10, " ").alias("PROC_DT_SK"),
    F.rpad(F.lit("NA"), 2, " ").alias("PROC_CD_MOD_TX")
)

df_collector = df_FcltyClmProcOut1.unionByName(df_defaultUNK).unionByName(df_defaultNA)

df_final = df_collector.select(
    F.col("FCLTY_CLM_PROC_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.col("FCLTY_CLM_PROC_ORDNL_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("FCLTY_CLM_SK"),
    F.col("PROC_CD_SK"),
    F.rpad(F.col("PROC_DT_SK"), 10, " ").alias("PROC_DT_SK"),
    F.rpad(F.col("PROC_CD_MOD_TX"), 2, " ").alias("PROC_CD_MOD_TX")
)

write_files(
    df_final,
    f"{adls_path}/load/FCLTY_CLM_PROC.{Source}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)