# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsMbrAccumFkey
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
# MAGIC                             GetFkeyMbr
# MAGIC                             GetFkeyGrp
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  MBR_ACCUM table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC           
# MAGIC                     Parikshith Chada  09/12/2006   Originally programmed
# MAGIC                     Parikshith Chada 10/02/2006    Code Changes to the Extraction process
# MAGIC  
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                                2008-08-27      3567(Primary Key)          Added Source System Code SK parameter        devlIDS                         Steph Goddard            09/02/2008
# MAGIC Ralph Tucker                  2008-12-16      3648(Labor Accts)          Added two fields (Carovr_amt, Cob_oop_amt)   devlIDSnew                   Steph Goddard            12/23/2008
# MAGIC 
# MAGIC Karthik Chintalapani        2016-11-11               5634      Added new columns PLN_YR_EFF_DT and  PLN_YR_END_DT   IntegrateDev2   Kalyan Neelam           2016-11-28

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Write out the MBR_UNIQ_KEY to a sequential file that will be read in on the next batch run and added to the driver table.
# MAGIC #$FilePath#/landing/FctsMbrRecycleList.MbrRecycle.dat.#RunID#
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DateType,
    DecimalType
)
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameters
InFile = get_widget_value('InFile','IdsMbrAccumExtr.MbrAccum.dat.2006092112345')
Logging = get_widget_value('Logging','Y')
SrcSysCdSk = get_widget_value('SrcSysCdSk','103764')

# ----------------------------------------------------------------------------
# Read from "MbrAccumCrf" (CSeqFileStage)
# ----------------------------------------------------------------------------
schema_MbrAccumCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("MBR_ACCUM_SK", IntegerType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("PROD_ACCUM_ID", StringType(), nullable=False),
    StructField("MBR_ACCUM_TYP_CD", StringType(), nullable=False),
    StructField("ACCUM_NO", IntegerType(), nullable=False),
    StructField("YR_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN", IntegerType(), nullable=False),
    StructField("GRGR_ID", StringType(), nullable=False),
    StructField("MBR_SK", IntegerType(), nullable=False),
    StructField("ACCUM_AMT", DecimalType(38,10), nullable=False),
    StructField("CAROVR_AMT", DecimalType(38,10), nullable=False),
    StructField("COB_OOP_AMT", DecimalType(38,10), nullable=False),
    StructField("PLN_YR_EFF_DT", DateType(), nullable=True),
    StructField("PLN_YR_END_DT", DateType(), nullable=True)
])

df_MbrAccumCrf = (
    spark.read.format("csv")
    .option("header", False)
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_MbrAccumCrf)
    .load(f"{adls_path}/key/{InFile}")
)

# ----------------------------------------------------------------------------
# Transformer: PurgeTrnAccum (CTransformerStage)
# ----------------------------------------------------------------------------
df_purgeTrnAccum = (
    df_MbrAccumCrf
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svAccumType",
                GetFkeyCodes(
                    F.col("SRC_SYS_CD"),
                    F.col("MBR_ACCUM_SK"),
                    F.lit("ACCUMULATOR TYPE"),
                    trim(F.col("MBR_ACCUM_TYP_CD")),
                    Logging
                )
               )
    .withColumn("svMbrSk",
                GetFkeyMbr(
                    F.col("SRC_SYS_CD"),
                    F.col("MBR_ACCUM_SK"),
                    F.col("MBR_UNIQ_KEY"),
                    Logging
                )
               )
    .withColumn("svGrpSk",
                GetFkeyGrp(
                    F.col("SRC_SYS_CD"),
                    F.col("MBR_ACCUM_SK"),
                    F.col("GRGR_ID"),
                    Logging
                )
               )
    .withColumn("ErrCount",
                GetFkeyErrorCnt(F.col("MBR_ACCUM_SK"))
               )
)

# ----------------------------------------------------------------------------
# Outputs from PurgeTrnAccum
#  1) MbrAccumOut => Constraint: ErrCount = 0 Or PassThru = 'Y'
#  2) lnkRecycle => Constraint: ErrCount > 0
#  3) recycle_mbr_list => Constraint: ErrCount > 0
#  4) DefaultUNK => Constraint: @INROWNUM = 1
#  5) DefaultNA => Constraint: @INROWNUM = 1
# ----------------------------------------------------------------------------

df_MbrAccumOut = df_purgeTrnAccum.filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))

df_lnkRecycle = (
    df_purgeTrnAccum
    .filter(F.col("ErrCount") > 0)
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(F.col("MBR_ACCUM_SK")))
    .withColumn("ERR_CT", F.col("ERR_CT") + F.lit(1))
)

df_recycle_mbr_list = (
    df_purgeTrnAccum
    .filter(F.col("ErrCount") > 0)
    .withColumn("TABLE", F.lit("MBR_ACCUM"))
    .select(
        F.col("MBR_UNIQ_KEY"),
        F.col("TABLE")
    )
)

# DefaultUNK: produce a single row with specified literal values
collectorSchema = StructType([
    StructField("MBR_ACCUM_SK", IntegerType()),
    StructField("SRC_SYS_CD_SK", IntegerType()),
    StructField("MBR_UNIQ_KEY", IntegerType()),
    StructField("PROD_ACCUM_ID", StringType()),
    StructField("MBR_ACCUM_TYP_CD_SK", IntegerType()),
    StructField("ACCUM_NO", IntegerType()),
    StructField("YR_NO", IntegerType()),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType()),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType()),
    StructField("GRP_SK", IntegerType()),
    StructField("MBR_SK", IntegerType()),
    StructField("ACCUM_AMT", DecimalType(38,10)),
    StructField("CAROVR_AMT", DecimalType(38,10)),
    StructField("COB_OOP_AMT", DecimalType(38,10)),
    StructField("PLN_YR_EFF_DT", DateType()),
    StructField("PLN_YR_END_DT", DateType())
])

df_DefaultUNK = spark.createDataFrame(
    [
        (
            0, 0, 0, "UNK", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "1753-01-01", "1753-01-01"
        )
    ],
    collectorSchema
).withColumn("PLN_YR_EFF_DT", F.to_date(F.col("PLN_YR_EFF_DT"), "yyyy-MM-dd")) \
 .withColumn("PLN_YR_END_DT", F.to_date(F.col("PLN_YR_END_DT"), "yyyy-MM-dd"))

# DefaultNA: produce a single row with specified literal values
df_DefaultNA = spark.createDataFrame(
    [
        (
            1, 1, 1, "NA", 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, "1753-01-01", "1753-01-01"
        )
    ],
    collectorSchema
).withColumn("PLN_YR_EFF_DT", F.to_date(F.col("PLN_YR_EFF_DT"), "yyyy-MM-dd")) \
 .withColumn("PLN_YR_END_DT", F.to_date(F.col("PLN_YR_END_DT"), "yyyy-MM-dd"))

# ---------------------------------------------------------------------------
# Collector: union MbrAccumOut, DefaultUNK, DefaultNA => output to "LoadFile"
# ---------------------------------------------------------------------------
df_MbrAccumOut_sel = df_MbrAccumOut.select(
    F.col("MBR_ACCUM_SK").alias("MBR_ACCUM_SK"),
    F.lit(SrcSysCdSk).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    F.col("svAccumType").alias("MBR_ACCUM_TYP_CD_SK"),
    F.col("ACCUM_NO").alias("ACCUM_NO"),
    F.col("YR_NO").alias("YR_NO"),
    F.col("CRT_RUN_CYC_EXCTN").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svGrpSk").alias("GRP_SK"),
    F.col("svMbrSk").alias("MBR_SK"),
    F.col("ACCUM_AMT").alias("ACCUM_AMT"),
    F.col("CAROVR_AMT").alias("CAROVR_AMT"),
    F.col("COB_OOP_AMT").alias("COB_OOP_AMT"),
    F.col("PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
    F.col("PLN_YR_END_DT").alias("PLN_YR_END_DT")
)

df_Collector = (
    df_MbrAccumOut_sel
    .union(
        df_DefaultUNK.select(
            "MBR_ACCUM_SK",
            "SRC_SYS_CD_SK",
            "MBR_UNIQ_KEY",
            "PROD_ACCUM_ID",
            "MBR_ACCUM_TYP_CD_SK",
            "ACCUM_NO",
            "YR_NO",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "GRP_SK",
            "MBR_SK",
            "ACCUM_AMT",
            "CAROVR_AMT",
            "COB_OOP_AMT",
            "PLN_YR_EFF_DT",
            "PLN_YR_END_DT"
        )
    )
    .union(
        df_DefaultNA.select(
            "MBR_ACCUM_SK",
            "SRC_SYS_CD_SK",
            "MBR_UNIQ_KEY",
            "PROD_ACCUM_ID",
            "MBR_ACCUM_TYP_CD_SK",
            "ACCUM_NO",
            "YR_NO",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "GRP_SK",
            "MBR_SK",
            "ACCUM_AMT",
            "CAROVR_AMT",
            "COB_OOP_AMT",
            "PLN_YR_EFF_DT",
            "PLN_YR_END_DT"
        )
    )
)

# ----------------------------------------------------------------------------
# Write the collector output to "MbrAccum" (CSeqFileStage) => MBR_ACCUM.dat
# Before writing, rpad any char/varchar columns
# Only "PROD_ACCUM_ID" is varchar here (length unknown).
# ----------------------------------------------------------------------------
df_MbrAccum_final = df_Collector.withColumn(
    "PROD_ACCUM_ID",
    rpad(F.col("PROD_ACCUM_ID"), <...>, " ")
).select(
    "MBR_ACCUM_SK",
    "SRC_SYS_CD_SK",
    "MBR_UNIQ_KEY",
    "PROD_ACCUM_ID",
    "MBR_ACCUM_TYP_CD_SK",
    "ACCUM_NO",
    "YR_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "GRP_SK",
    "MBR_SK",
    "ACCUM_AMT",
    "CAROVR_AMT",
    "COB_OOP_AMT",
    "PLN_YR_EFF_DT",
    "PLN_YR_END_DT"
)

write_files(
    df_MbrAccum_final,
    f"{adls_path}/load/MBR_ACCUM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# Write the hashed-file output "hf_recycle" => scenario C => parquet
# Must rpad char/varchar columns from link "lnkRecycle"
# ----------------------------------------------------------------------------
df_lnkRecycle_out = (
    df_lnkRecycle
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("GRGR_ID", F.rpad(F.col("GRGR_ID"), 10, " "))
    .withColumn("SRC_SYS_CD", rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("PRI_KEY_STRING", rpad(F.col("PRI_KEY_STRING"), <...>, " "))
    .withColumn("PROD_ACCUM_ID", rpad(F.col("PROD_ACCUM_ID"), <...>, " "))
    .withColumn("MBR_ACCUM_TYP_CD", rpad(F.col("MBR_ACCUM_TYP_CD"), <...>, " "))
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
        "MBR_ACCUM_SK",
        "MBR_UNIQ_KEY",
        "PROD_ACCUM_ID",
        "MBR_ACCUM_TYP_CD",
        "ACCUM_NO",
        "YR_NO",
        "CRT_RUN_CYC_EXCTN",
        "LAST_UPDT_RUN_CYC_EXCTN",
        "GRGR_ID",
        "MBR_SK",
        "ACCUM_AMT",
        "CAROVR_AMT",
        "COB_OOP_AMT",
        "PLN_YR_EFF_DT",
        "PLN_YR_END_DT"
    )
)

write_files(
    df_lnkRecycle_out,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# Write "MbrRecycle" => "FctsMbrRecycleList.MbrRecycle.dat.#RunID#" (CSeqFileStage)
# columns: MBR_UNIQ_KEY (int), TABLE (varchar => use rpad with unknown length)
# ----------------------------------------------------------------------------
df_recycle_mbr_list_out = (
    df_recycle_mbr_list
    .withColumn("TABLE", rpad(F.col("TABLE"), <...>, " "))
    .select("MBR_UNIQ_KEY", "TABLE")
)

write_files(
    df_recycle_mbr_list_out,
    f"{adls_path_raw}/landing/FctsMbrRecycleList.MbrRecycle.dat.#RunID#",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)