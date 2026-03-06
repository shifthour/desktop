# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsMbrMcareRetireeFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  Primary Key job output
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_mbr_rds
# MAGIC                            hf_recyle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes
# MAGIC                             GetFkeyDate
# MAGIC                             GetFkeyErrorCnt
# MAGIC                             GetFkeyMbr
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC OUTPUTS: MBR RDS     table load file
# MAGIC                    hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             SAndrew   03/04/2006        Originally Programmed
# MAGIC 
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Developer                  Date                       Project/Altiris #                                 Change Description                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Manasa Andru      2014-01-27                   TFS- 6710                                     Updated the default NA record                    IntegrateNewDevl         Kalyan Neelam          2014-01-29
# MAGIC                                                                                                                      to 1 for the MBR_UNIQ_KEY field.

# MAGIC Set all foreign surrogate keys
# MAGIC Read common record format file from extract job.  Created in FctsMbrMcareRetireeExtr
# MAGIC Writing Sequential File to /load
# MAGIC Reads in extract file for from the EDW generated Retiree Listing file.
# MAGIC Preps for load to MBR_RDS. 
# MAGIC Job will run month, quarterly or annually.
# MAGIC Quaterly sometime on the 10th of the first month following the end of a quarter.  
# MAGIC Since from EDW, already has MBR_SK and GRP_SK and foreign lookups not needed in Fkey job.
# MAGIC Merge source data with default rows
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, lit, trim, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


TmpOutFile = get_widget_value("TmpOutFile","MBR_RDS.dat")
InFile = get_widget_value("InFile","IdsMbrRetireeExtr.MbrRetiree.dat")
Logging = get_widget_value("Logging","Y")

schema_MbrMcareRetiree = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", StringType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("MBR_RDS_SK", IntegerType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("SUBMT_DT", StringType(), False),
    StructField("PLN_YR_STRT_DT", StringType(), False),
    StructField("SUBSIDY_PERD_EFF_DT", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("GRP_SK", IntegerType(), False),
    StructField("MBR_SK", IntegerType(), False),
    StructField("PLN_YR_END_DT", StringType(), False),
    StructField("SUBSIDY_PERD_TERM_DT", StringType(), False),
])

df_MbrMcareRetiree = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_MbrMcareRetiree)
    .load(f"{adls_path}/key/{InFile}")
)

df_PurgeTrn = (
    df_MbrMcareRetiree
    .withColumn("SrcSysCdSk", GetFkeyCodes("IDS", col("MBR_RDS_SK"), trim(lit("SOURCE SYSTEM")), trim(col("SRC_SYS_CD")), lit(Logging)))
    .withColumn("SubmitDateSk", GetFkeyDate("IDS", col("MBR_RDS_SK"), col("SUBMT_DT"), lit(Logging)))
    .withColumn("SubsidEffDtSk", GetFkeyDate("IDS", col("MBR_RDS_SK"), col("SUBSIDY_PERD_EFF_DT"), lit(Logging)))
    .withColumn("SubsidTermDtSk", GetFkeyDate("IDS", col("MBR_RDS_SK"), col("SUBSIDY_PERD_TERM_DT"), lit(Logging)))
    .withColumn("PlanYearStartDtSk", GetFkeyDate("IDS", col("MBR_RDS_SK"), col("PLN_YR_STRT_DT"), lit(Logging)))
    .withColumn("PlanYearEndDtSk", GetFkeyDate("IDS", col("MBR_RDS_SK"), col("PLN_YR_END_DT"), lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("MBR_RDS_SK")))
)

df_Retiree1 = (
    df_PurgeTrn
    .filter((col("ErrCount") == 0) | (trim(col("PASS_THRU_IN")) == "Y"))
    .select(
        col("MBR_RDS_SK").alias("MBR_RDS_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("SubmitDateSk").alias("SUBMT_DT_SK"),
        col("PlanYearStartDtSk").alias("PLN_YR_STRT_DT_SK"),
        col("SubsidEffDtSk").alias("SUBSIDY_PERD_EFF_DT_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("GRP_SK").alias("GRP_SK"),
        col("MBR_SK").alias("MBR_SK"),
        col("PlanYearEndDtSk").alias("PLN_YR_END_DT_SK"),
        col("SubsidTermDtSk").alias("SUBSIDY_PERD_TERM_DT_SK")
    )
)

df_Recycle1 = (
    df_PurgeTrn
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("MBR_RDS_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("MBR_RDS_SK").alias("MBR_RDS_SK"),
        col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("SUBMT_DT").alias("SUBMT_DT"),
        col("PLN_YR_STRT_DT").alias("PLN_YR_STRT_DT"),
        col("SUBSIDY_PERD_EFF_DT").alias("SUBSIDY_PERD_EFF_DT"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("GRP_SK").alias("GRP_SK"),
        col("MBR_SK").alias("MBR_SK"),
        col("PLN_YR_END_DT").alias("PLN_YR_END_DT"),
        col("SUBSIDY_PERD_TERM_DT").alias("SUBSIDY_PERD_TERM_DT")
    )
)

write_files(
    df_Recycle1,
    f"{adls_path}/hf_recycle.parquet",
    mode="overwrite",
    is_pqruet=True
)

df_DefaultUNK = spark.createDataFrame(
    [(0, 0, 0, "UNK", "UNK", "UNK", 0, 0, 0, 0, "UNK", "UNK")],
    [
        "MBR_RDS_SK",
        "SRC_SYS_CD_SK",
        "MBR_UNIQ_KEY",
        "SUBMT_DT_SK",
        "PLN_YR_STRT_DT_SK",
        "SUBSIDY_PERD_EFF_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "GRP_SK",
        "MBR_SK",
        "PLN_YR_END_DT_SK",
        "SUBSIDY_PERD_TERM_DT_SK"
    ]
)

df_DefaultNA = spark.createDataFrame(
    [(1, 1, 1, "NA", "NA", "NA", 1, 1, 1, 1, "NA", "NA")],
    [
        "MBR_RDS_SK",
        "SRC_SYS_CD_SK",
        "MBR_UNIQ_KEY",
        "SUBMT_DT_SK",
        "PLN_YR_STRT_DT_SK",
        "SUBSIDY_PERD_EFF_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "GRP_SK",
        "MBR_SK",
        "PLN_YR_END_DT_SK",
        "SUBSIDY_PERD_TERM_DT_SK"
    ]
)

df_Collector = df_Retiree1.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

df_final = df_Collector.select(
    col("MBR_RDS_SK"),
    col("SRC_SYS_CD_SK"),
    col("MBR_UNIQ_KEY"),
    rpad(col("SUBMT_DT_SK"), 10, " ").alias("SUBMT_DT_SK"),
    rpad(col("PLN_YR_STRT_DT_SK"), 10, " ").alias("PLN_YR_STRT_DT_SK"),
    rpad(col("SUBSIDY_PERD_EFF_DT_SK"), 10, " ").alias("SUBSIDY_PERD_EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_SK"),
    col("MBR_SK"),
    rpad(col("PLN_YR_END_DT_SK"), 10, " ").alias("PLN_YR_END_DT_SK"),
    rpad(col("SUBSIDY_PERD_TERM_DT_SK"), 10, " ").alias("SUBSIDY_PERD_TERM_DT_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)