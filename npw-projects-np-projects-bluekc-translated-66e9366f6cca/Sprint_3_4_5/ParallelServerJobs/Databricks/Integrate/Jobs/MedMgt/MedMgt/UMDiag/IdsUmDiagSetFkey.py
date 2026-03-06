# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsUmDiagSetFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  Primary Key output file
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes
# MAGIC                             
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Suzanne Saylor  -  02/16/2006  -  Originally programmed
# MAGIC             Suzanne Saylor  -  03/27/2006  -  Corrected default values for integer fields from 0 to 1
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC       
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                  Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                            ----------------------------------              ---------------------------------               -------------------------
# MAGIC Bhoomi Dasari                 03/25/2009              3808                                Added SrcSysCdSk                                                           devlIDS                             Steph Goddard                      03/30/2009
# MAGIC   
# MAGIC Rick Henry                      2012-05-10        4896                                       Added Diag_Cd_Typ_Cd to SK lookup                       IntegrateNewDebl                     SAndrew                              2012-05-20

# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
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
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile", "IdsUmDiagSetExtr.dat.pkey")
Logging = get_widget_value("Logging", "X")
OutFile = get_widget_value("OutFile", "UM_DIAG_SET.dat")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "")

schema_idsumdiagsetextr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("UM_DIAG_SET_SK", IntegerType(), nullable=False),
    StructField("UM_REF_ID", StringType(), nullable=False),
    StructField("DIAG_SET_CRT_DTM", TimestampType(), nullable=False),
    StructField("SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("DIAG_CD_SK", IntegerType(), nullable=False),
    StructField("UM_SK", IntegerType(), nullable=False),
    StructField("ONSET_DT_SK", StringType(), nullable=False),
    StructField("DIAG_CD_TYP_CD", StringType(), nullable=False)
])

df_idsumdiagsetextr = (
    spark.read
    .option("sep", ",")
    .option("quote", '"')
    .schema(schema_idsumdiagsetextr)
    .csv(f"{adls_path}/key/{InFile}", header=False)
)

df_foreignkey = (
    df_idsumdiagsetextr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svDiagCdSk", GetFkeyDiagCd(F.col("SRC_SYS_CD"), F.col("UM_DIAG_SET_SK"), F.col("DIAG_CD_SK"), F.col("DIAG_CD_TYP_CD"), F.lit(Logging)))
    .withColumn("svUmSk", GetFkeyUm(F.col("SRC_SYS_CD"), F.col("UM_DIAG_SET_SK"), F.col("UM_SK"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("UM_DIAG_SET_SK")))
)

df_fkey = (
    df_foreignkey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("UM_DIAG_SET_SK").alias("UM_DIAG_SET_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("UM_REF_ID").alias("UM_REF_ID"),
        F.col("DIAG_SET_CRT_DTM").alias("DIAG_SET_CRT_DTM"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svDiagCdSk").alias("DIAG_CD_SK"),
        F.col("svUmSk").alias("UM_SK"),
        FORMAT.DATE(F.col("ONSET_DT_SK"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("CCYY-MM-DD")).alias("ONSET_DT_SK")
    )
)

df_recycle = (
    df_foreignkey
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("UM_DIAG_SET_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("UM_DIAG_SET_SK").alias("UM_DIAG_SET_SK"),
        F.col("UM_REF_ID").alias("UM_REF_ID"),
        F.col("DIAG_SET_CRT_DTM").alias("DIAG_SET_CRT_DTM"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("DIAG_CD_SK").alias("DIAG_CD_SK")
    )
)

window_spec = Window.orderBy(F.lit(1))
df_foreignkey_rownum = df_foreignkey.withColumn("_rownum", F.row_number().over(window_spec))

df_defaultunk = (
    df_foreignkey_rownum
    .filter(F.col("_rownum") == 1)
    .select(
        F.lit(0).alias("UM_DIAG_SET_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("UM_REF_ID"),
        F.lit("1753-01-01 00:00:00.000000").alias("DIAG_SET_CRT_DTM"),
        F.lit(0).alias("SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("DIAG_CD_SK"),
        F.lit(0).alias("UM_SK"),
        F.lit("1753-01-01").alias("ONSET_DT_SK")
    )
)

df_defaultna = (
    df_foreignkey_rownum
    .filter(F.col("_rownum") == 1)
    .select(
        F.lit(1).alias("UM_DIAG_SET_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("UM_REF_ID"),
        F.lit("1753-01-01 00:00:00.000000").alias("DIAG_SET_CRT_DTM"),
        F.lit(1).alias("SEQ_NO"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("DIAG_CD_SK"),
        F.lit(1).alias("UM_SK"),
        F.lit("1753-01-01").alias("ONSET_DT_SK")
    )
)

df_collector = df_fkey.union(df_defaultunk).union(df_defaultna)

write_files(
    df_recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_collector_final = df_collector.select(
    F.col("UM_DIAG_SET_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("UM_REF_ID"),
    F.col("DIAG_SET_CRT_DTM"),
    F.col("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DIAG_CD_SK"),
    F.col("UM_SK"),
    F.rpad(F.col("ONSET_DT_SK"), 10, " ").alias("ONSET_DT_SK")
)

write_files(
    df_collector_final,
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)