# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsFmlyAccumFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  Primary key job output
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes
# MAGIC                             GetFkeyErrorCnt
# MAGIC                             GetFkeySub
# MAGIC                             GetFKeyGrp
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  FMLY_ACCUM table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC            Parikshith Chada  09/14/2006      Originally Programmed
# MAGIC            Parikshith Chada  10/03/2006      Code Changes for Extraction process
# MAGIC  
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                               2008-08-27       3567(Primary key)           Added Source System Code SK as parameter   devlIDS                        Steph Goddard             09/02/2008
# MAGIC Ralph Tucker                  2008-12-16        3648(Labor Accts)      Added two field (Carovr_amt)                               devlIDSnew                  Steph Goddard            12/23/2008
# MAGIC    
# MAGIC 
# MAGIC Karthik Chintalapani        2016-11-11               5634      Added new columns PLN_YR_EFF_DT and  PLN_YR_END_DT   IntegrateDev2  Kalyan Neelam            2016-11-28

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Write out the SUB_UNIQ_KEY to a sequential file that will be read in on the next batch run and added to the driver table.
# MAGIC #$FilePath#/landing/FctsMbrRecycleList.MbrRecycle.dat.#RunID#
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
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','Y')
Infile = get_widget_value('Infile','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_FmlyAccumCrf = T.StructType([
    T.StructField("JOB_EXCTN_RCRD_ERR_SK", T.IntegerType(), False),
    T.StructField("INSRT_UPDT_CD", T.StringType(), False),
    T.StructField("DISCARD_IN", T.StringType(), False),
    T.StructField("PASS_THRU_IN", T.StringType(), False),
    T.StructField("FIRST_RECYC_DT", T.TimestampType(), False),
    T.StructField("ERR_CT", T.IntegerType(), False),
    T.StructField("RECYCLE_CT", T.DecimalType(38,10), False),
    T.StructField("SRC_SYS_CD", T.StringType(), False),
    T.StructField("PRI_KEY_STRING", T.StringType(), False),
    T.StructField("FMLY_ACCUM_SK", T.IntegerType(), False),
    T.StructField("SUB_UNIQ_KEY", T.IntegerType(), False),
    T.StructField("PROD_ACCUM_ID", T.StringType(), False),
    T.StructField("FMLY_ACCUM_TYP_CD", T.StringType(), False),
    T.StructField("ACCUM_NO", T.IntegerType(), False),
    T.StructField("YR_NO", T.IntegerType(), False),
    T.StructField("CRT_RUN_CYC_EXCTN", T.IntegerType(), False),
    T.StructField("LAST_UPDT_RUN_CYC_EXCTN", T.IntegerType(), False),
    T.StructField("GRGR_ID", T.StringType(), False),
    T.StructField("SUB_SK", T.IntegerType(), False),
    T.StructField("ACCUM_AMT", T.DecimalType(38,10), False),
    T.StructField("CAROVR_AMT", T.DecimalType(38,10), False),
    T.StructField("PLN_YR_EFF_DT", T.DateType(), True),
    T.StructField("PLN_YR_END_DT", T.DateType(), True)
])

df_FmlyAccumCrf = (
    spark.read.csv(
        path=f"{adls_path}/key/{Infile}",
        schema=schema_FmlyAccumCrf,
        sep=",",
        quote="\"",
        header=False
    )
)

df_PurgeTrnFmlyAccum_enriched = (
    df_FmlyAccumCrf
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("FMLY_ACCUM_SK")))
    .withColumn("svAccumType", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("FMLY_ACCUM_SK"), F.lit("ACCUMULATOR TYPE"), trim(F.col("FMLY_ACCUM_TYP_CD")), Logging))
    .withColumn("svSubSk", GetFkeySub(F.col("SRC_SYS_CD"), F.col("FMLY_ACCUM_SK"), F.col("SUB_UNIQ_KEY"), Logging))
    .withColumn("svGrpSk", GetFkeyGrp(F.col("SRC_SYS_CD"), F.col("FMLY_ACCUM_SK"), F.col("GRGR_ID"), Logging))
)

df_lnkRecycle_pre = (
    df_PurgeTrnFmlyAccum_enriched
    .filter(F.col("ErrCount") > 0)
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(F.col("FMLY_ACCUM_SK")))
    .withColumn("ERR_CT", F.col("ERR_CT") + F.lit(1))
)

df_lnkRecycle = (
    df_lnkRecycle_pre
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ERR_CT").alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("FMLY_ACCUM_SK").alias("FMLY_ACCUM_SK"),
        F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
        F.col("FMLY_ACCUM_TYP_CD").alias("FMLY_ACCUM_TYP_CD"),
        F.col("ACCUM_NO").alias("ACCUM_NO"),
        F.col("YR_NO").alias("YR_NO"),
        F.col("CRT_RUN_CYC_EXCTN").alias("CRT_RUN_CYC_EXCTN"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN").alias("LAST_UPDT_RUN_CYC_EXCTN"),
        F.col("GRGR_ID").alias("GRGR_ID"),
        F.col("SUB_SK").alias("SUB_SK"),
        F.col("ACCUM_AMT").alias("ACCUM_AMT"),
        F.col("CAROVR_AMT").alias("CAROVR_AMT"),
        F.col("PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
        F.col("PLN_YR_END_DT").alias("PLN_YR_END_DT")
    )
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("GRGR_ID", F.rpad(F.col("GRGR_ID"), 10, " "))
)

write_files(
    df_lnkRecycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_mbrRecycle = (
    df_PurgeTrnFmlyAccum_enriched
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.lit("FMLY_ACCUM").alias("TABLE")
    )
)

write_files(
    df_mbrRecycle,
    f"{adls_path_raw}/landing/FctsMbrRecycleList.MbrRecycle.dat.#RunID#",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_FmlyAccumOut = (
    df_PurgeTrnFmlyAccum_enriched
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("FMLY_ACCUM_SK").alias("FMLY_ACCUM_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
        F.col("svAccumType").alias("FMLY_ACCUM_TYP_CD_SK"),
        F.col("ACCUM_NO").alias("ACCUM_NO"),
        F.col("YR_NO").alias("YR_NO"),
        F.col("CRT_RUN_CYC_EXCTN").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svGrpSk").alias("GRP_SK"),
        F.col("svSubSk").alias("SUB_SK"),
        F.col("ACCUM_AMT").alias("ACCUM_AMT"),
        F.col("CAROVR_AMT").alias("CAROVR_AMT"),
        F.col("PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
        F.col("PLN_YR_END_DT").alias("PLN_YR_END_DT")
    )
)

df_DefaultUNK_pre = df_PurgeTrnFmlyAccum_enriched.limit(1)
df_DefaultUNK = (
    df_DefaultUNK_pre
    .select(
        F.lit(0).alias("FMLY_ACCUM_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(0).alias("SUB_UNIQ_KEY"),
        F.lit("UNK").alias("PROD_ACCUM_ID"),
        F.lit(0).alias("FMLY_ACCUM_TYP_CD_SK"),
        F.lit(0).alias("ACCUM_NO"),
        F.lit(0).alias("YR_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("GRP_SK"),
        F.lit(0).alias("SUB_SK"),
        F.lit(0).alias("ACCUM_AMT"),
        F.lit(0).alias("CAROVR_AMT"),
        F.lit("1753-01-01").alias("PLN_YR_EFF_DT"),
        F.lit("1753-01-01").alias("PLN_YR_END_DT")
    )
)

df_DefaultNA_pre = df_PurgeTrnFmlyAccum_enriched.limit(1)
df_DefaultNA = (
    df_DefaultNA_pre
    .select(
        F.lit(1).alias("FMLY_ACCUM_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(1).alias("SUB_UNIQ_KEY"),
        F.lit("NA").alias("PROD_ACCUM_ID"),
        F.lit(1).alias("FMLY_ACCUM_TYP_CD_SK"),
        F.lit(1).alias("ACCUM_NO"),
        F.lit(1).alias("YR_NO"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("GRP_SK"),
        F.lit(1).alias("SUB_SK"),
        F.lit(0).alias("ACCUM_AMT"),
        F.lit(0).alias("CAROVR_AMT"),
        F.lit("1753-01-01").alias("PLN_YR_EFF_DT"),
        F.lit("1753-01-01").alias("PLN_YR_END_DT")
    )
)

df_Collector = df_FmlyAccumOut.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

final_cols = [
    "FMLY_ACCUM_SK",
    "SRC_SYS_CD_SK",
    "SUB_UNIQ_KEY",
    "PROD_ACCUM_ID",
    "FMLY_ACCUM_TYP_CD_SK",
    "ACCUM_NO",
    "YR_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "GRP_SK",
    "SUB_SK",
    "ACCUM_AMT",
    "CAROVR_AMT",
    "PLN_YR_EFF_DT",
    "PLN_YR_END_DT"
]

df_Collector_final = df_Collector.select(final_cols)

write_files(
    df_Collector_final,
    f"{adls_path}/load/FMLY_ACCUM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)