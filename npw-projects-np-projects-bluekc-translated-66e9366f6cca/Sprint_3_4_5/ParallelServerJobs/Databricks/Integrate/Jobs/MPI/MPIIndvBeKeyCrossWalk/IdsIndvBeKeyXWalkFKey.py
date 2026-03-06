# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME: IdsMpiIndvBeKeyXWalkExtrFKey
# MAGIC 
# MAGIC CALLED BY:  MpiBcbsloadSeq
# MAGIC 
# MAGIC PROCESSING:  This is a daily process. These records are written to the MPI BCBS Extension Database from the MPI tool and provides any existing BE_KEY's that have received a new BE_KEY.
# MAGIC 
# MAGIC DEPENDENCIES: MPI Tool process complete and files outputted. MPI FILE ---> MPI BCBS Extension : Membership : INDV_BE_CRSWALK job completed.
# MAGIC 
# MAGIC MODIFICATIONS:             
# MAGIC                                                                                                                                                                                    
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Abhiram Dasarathy            07-19-2012      4426 - MPI                     Original Programming                                                                            IntegrateWrhsDevl       Bhoomi Dasari             08/16/2012 
# MAGIC 
# MAGIC Abhiram Dasarathy	         11-24-2012       4426 - MPI	      Updated NA and UNK rows to process Current Date		     IntegrateNewDevl
# MAGIC 
# MAGIC Abhiram Dasarathy	         2012-12-27      4426 - MPI 	      Added to fields MBR_SK and MBR_UNIQ_KEY to the 		     IntegrateNewDevl         Bhoomi Dasari             01/14/2013 
# MAGIC 				          		      Transformer, Link Collector and sequential file
# MAGIC Abhiram Dasarathy           2013-01-01        4426                           Changed MBR_SK on NA to be 1                                                          IntegrateNewDevl         Sharon Andrew            2013-01-18
# MAGIC 
# MAGIC Manasa Andru                  2014-01-13        TFS - 2646                 Updated the CRT_RUN_CYC_EXCTN_SK and                                    IntegrateNewDevl          Kalyan Neelam          2014-01-15
# MAGIC                                                                                                    LAST_UPDT_RUN_CYC_EXCTN_SK in the default rows

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC MPI INDIVIDUAL CROSSWALK FKEY JOB
# MAGIC Read common record file form extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DecimalType
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile", "IdsMpiIndvBeCrswalk.MpiIndvBeCrswalk.dat")

schema_MPI_INDV_BE_CRSWALK_Extr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38, 10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("MPI_INDV_BE_CRSWALK_SK", IntegerType(), nullable=False),
    StructField("MPI_MBR_ID", StringType(), nullable=False),
    StructField("PRCS_OWNER_ID", StringType(), nullable=False),
    StructField("SRC_SYS_ID", StringType(), nullable=False),
    StructField("PRCS_RUN_DTM", TimestampType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("PRCS_RUN_DT_SK", StringType(), nullable=False),
    StructField("NEW_INDV_BE_KEY", DecimalType(38, 10), nullable=False),
    StructField("PREV_INDV_BE_KEY", DecimalType(38, 10), nullable=False),
    StructField("PRCS_OWNER_SRC_SYS_ENVRN_ID", StringType(), nullable=False),
    StructField("MBR_SK", IntegerType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False)
])

df_MPI_INDV_BE_CRSWALK_Extr = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_MPI_INDV_BE_CRSWALK_Extr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_ForeignKey_base = (
    df_MPI_INDV_BE_CRSWALK_Extr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("MPI_INDV_BE_CRSWALK_SK")))
    .withColumn("PrcsRunDtSk", GetFkeyDate(F.lit("IDS"), F.col("MPI_INDV_BE_CRSWALK_SK"), F.col("PRCS_RUN_DT_SK"), F.lit("Y")))
    .withColumn("row_num", F.row_number().over(Window.orderBy(F.lit(1))))
)

df_ForeignKey_Fkey = (
    df_ForeignKey_base
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("MPI_INDV_BE_CRSWALK_SK").alias("MPI_INDV_BE_CRSWALK_SK"),
        F.col("MPI_MBR_ID").alias("MPI_MBR_ID"),
        F.col("PRCS_OWNER_ID").alias("PRCS_OWNER_ID"),
        F.col("SRC_SYS_ID").alias("SRC_SYS_ID"),
        F.col("PRCS_RUN_DTM").alias("PRCS_RUN_DTM"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PrcsRunDtSk").alias("PRCS_RUN_DT_SK"),
        F.col("NEW_INDV_BE_KEY").alias("NEW_INDV_BE_KEY"),
        F.col("PREV_INDV_BE_KEY").alias("PREV_INDV_BE_KEY"),
        F.col("PRCS_OWNER_SRC_SYS_ENVRN_ID").alias("PRCS_OWNER_SRC_SYS_ENVRN_ID"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
    )
)

df_ForeignKey_Recycle = (
    df_ForeignKey_base
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("MPI_INDV_BE_CRSWALK_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_ID").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("MPI_INDV_BE_CRSWALK_SK").alias("MPI_INDV_BE_CRSWALK_SK"),
        F.col("MPI_MBR_ID").alias("MPI_MBR_ID"),
        F.col("PRCS_OWNER_ID").alias("PRCS_OWNER_ID"),
        F.col("SRC_SYS_ID").alias("SRC_SYS_ID"),
        F.col("PRCS_RUN_DTM").alias("PRCS_RUN_DTM"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PRCS_RUN_DT_SK").alias("PRCS_RUN_DT_SK"),
        F.col("NEW_INDV_BE_KEY").alias("NEW_INDV_BE_KEY"),
        F.col("PREV_INDV_BE_KEY").alias("PREV_INDV_BE_KEY"),
        F.col("PRCS_OWNER_SRC_SYS_ENVRN_ID").alias("PRCS_OWNER_SRC_SYS_ENVRN_ID"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
    )
)

df_ForeignKey_DefaultNA = (
    df_ForeignKey_base
    .filter(F.col("row_num") == 1)
    .select(
        F.lit(1).alias("MPI_INDV_BE_CRSWALK_SK"),
        F.lit("NA").alias("MPI_MBR_ID"),
        F.lit("NA").alias("PRCS_OWNER_ID"),
        F.lit("NA").alias("SRC_SYS_ID"),
        F.lit("1753-01-01 00:00:00.000000").alias("PRCS_RUN_DTM"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("1753-01-01").alias("PRCS_RUN_DT_SK"),
        F.lit(1).alias("NEW_INDV_BE_KEY"),
        F.lit(1).alias("PREV_INDV_BE_KEY"),
        F.lit("NA").alias("PRCS_OWNER_SRC_SYS_ENVRN_ID"),
        F.lit(1).alias("MBR_SK"),
        F.lit(1).alias("MBR_UNIQ_KEY")
    )
)

df_ForeignKey_DefaultUNK = (
    df_ForeignKey_base
    .filter(F.col("row_num") == 1)
    .select(
        F.lit(0).alias("MPI_INDV_BE_CRSWALK_SK"),
        F.lit("UNK").alias("MPI_MBR_ID"),
        F.lit("UNK").alias("PRCS_OWNER_ID"),
        F.lit("UNK").alias("SRC_SYS_ID"),
        F.lit("1753-01-01 00:00:00.000000").alias("PRCS_RUN_DTM"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("1753-01-01").alias("PRCS_RUN_DT_SK"),
        F.lit(0).alias("NEW_INDV_BE_KEY"),
        F.lit(0).alias("PREV_INDV_BE_KEY"),
        F.lit("UNK").alias("PRCS_OWNER_SRC_SYS_ENVRN_ID"),
        F.lit(0).alias("MBR_SK"),
        F.lit(0).alias("MBR_UNIQ_KEY")
    )
)

df_hf_recycle = df_ForeignKey_Recycle.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "MPI_INDV_BE_CRSWALK_SK",
    "MPI_MBR_ID",
    "PRCS_OWNER_ID",
    "SRC_SYS_ID",
    "PRCS_RUN_DTM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRCS_RUN_DT_SK",
    "NEW_INDV_BE_KEY",
    "PREV_INDV_BE_KEY",
    "PRCS_OWNER_SRC_SYS_ENVRN_ID",
    "MBR_SK",
    "MBR_UNIQ_KEY"
)

write_files(
    df_hf_recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_Collector_Fkey = df_ForeignKey_Fkey.select(
    "MPI_INDV_BE_CRSWALK_SK",
    "MPI_MBR_ID",
    "PRCS_OWNER_ID",
    "SRC_SYS_ID",
    "PRCS_RUN_DTM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRCS_RUN_DT_SK",
    "NEW_INDV_BE_KEY",
    "PREV_INDV_BE_KEY",
    "PRCS_OWNER_SRC_SYS_ENVRN_ID",
    "MBR_SK",
    "MBR_UNIQ_KEY"
)

df_Collector_DefaultNA = df_ForeignKey_DefaultNA.select(
    "MPI_INDV_BE_CRSWALK_SK",
    "MPI_MBR_ID",
    "PRCS_OWNER_ID",
    "SRC_SYS_ID",
    "PRCS_RUN_DTM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRCS_RUN_DT_SK",
    "NEW_INDV_BE_KEY",
    "PREV_INDV_BE_KEY",
    "PRCS_OWNER_SRC_SYS_ENVRN_ID",
    "MBR_SK",
    "MBR_UNIQ_KEY"
)

df_Collector_DefaultUNK = df_ForeignKey_DefaultUNK.select(
    "MPI_INDV_BE_CRSWALK_SK",
    "MPI_MBR_ID",
    "PRCS_OWNER_ID",
    "SRC_SYS_ID",
    "PRCS_RUN_DTM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRCS_RUN_DT_SK",
    "NEW_INDV_BE_KEY",
    "PREV_INDV_BE_KEY",
    "PRCS_OWNER_SRC_SYS_ENVRN_ID",
    "MBR_SK",
    "MBR_UNIQ_KEY"
)

df_Collector = (
    df_Collector_Fkey
    .unionByName(df_Collector_DefaultNA)
    .unionByName(df_Collector_DefaultUNK)
)

df_MPI_INDV_BE_CRSWALK_LoadFile = df_Collector.withColumn(
    "PRCS_RUN_DT_SK",
    F.rpad(F.col("PRCS_RUN_DT_SK"), 10, " ")
).select(
    "MPI_INDV_BE_CRSWALK_SK",
    "MPI_MBR_ID",
    "PRCS_OWNER_ID",
    "SRC_SYS_ID",
    "PRCS_RUN_DTM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRCS_RUN_DT_SK",
    "NEW_INDV_BE_KEY",
    "PREV_INDV_BE_KEY",
    "PRCS_OWNER_SRC_SYS_ENVRN_ID",
    "MBR_SK",
    "MBR_UNIQ_KEY"
)

write_files(
    df_MPI_INDV_BE_CRSWALK_LoadFile,
    f"{adls_path}/load/MPI_INDV_BE_CRSWALK.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)