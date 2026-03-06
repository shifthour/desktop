# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : IdsBCBSSCClmLoad2Seq
# MAGIC 
# MAGIC DESCRIPTION:      FKey job for MBR_PCP_ATTRBTN table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                     Date                 Project/Altiris #          Change Description                                                     Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                --------------------------------              -------------------------------   ----------------------------   
# MAGIC Santosh Bokka         2013-06-02            4917                        Original Programming                                                   IntegrateNewDevl               Kalyan Neelam        2013-06-27
# MAGIC Santosh Bokka         2013-02-27            TFS -  8255             Added SrcSysCdSK                                                     IntegrateNewDevl              Bhoomi Dasari             3/5/2014
# MAGIC Kalyan Neelam           2015-05-27      5212 - PI             Added 2 new columns ATTRBTN_BCBS_PLN_CD and       IntegrateCurDevl               Bhoomi Dasari           5/28/2015
# MAGIC                                                                                      ATTRBTN_BCBS_PLN_CD_SK

# MAGIC IdsMbrPcpAttrbtnFKey
# MAGIC 
# MAGIC IDS MBR_PCP_ATTRBTN Foreign Keying
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType
)
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
InFile = get_widget_value("InFile", "\\\"TreoMbrPCPAttrbtnExtr.TreoMbrPCPAttrbtn.uniq\\\"")
Logging = get_widget_value("Logging", "Y")
RunCycle = get_widget_value("RunCycle", "")
SrcSysCdSK = get_widget_value("SrcSysCdSK", "")

# Schema definition for input file (CSeqFileStage: MbrPcpAttrbtnKey)
schema_MbrPcpAttrbtnKey = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38, 10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("MBR_PCP_ATTRBTN_SK", IntegerType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("ATTRBTN_BCBS_PLN_CD", StringType(), nullable=False),
    StructField("ROW_EFF_DT_SK", StringType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("MBR_SK", IntegerType(), nullable=False),
    StructField("PROV_SK", IntegerType(), nullable=False),
    StructField("REL_GRP_PROV_SK", IntegerType(), nullable=False),
    StructField("COB_IN", StringType(), nullable=False),
    StructField("LAST_EVAL_AND_MNG_SVC_DT_SK", StringType(), nullable=False),
    StructField("INDV_BE_KEY", DecimalType(38, 10), nullable=False),
    StructField("MBR_PCP_MO_NO", DecimalType(38, 10), nullable=False),
    StructField("MED_HOME_ID", StringType(), nullable=False),
    StructField("MED_HOME_DESC", StringType(), nullable=False),
    StructField("MED_HOME_GRP_ID", StringType(), nullable=False),
    StructField("MED_HOME_GRP_DESC", StringType(), nullable=False),
    StructField("MED_HOME_LOC_ID", StringType(), nullable=False),
    StructField("MED_HOME_LOC_DESC", StringType(), nullable=False),
    StructField("ATTRBTN_BCBS_PLN_CD_SRC_CD", StringType(), nullable=False)
])

# Read from the input file path (since "key" is not "landing" or "external", use adls_path)
df_MbrPcpAttrbtnKey = (
    spark.read
    .format("csv")
    .option("header", False)
    .option("quote", "\"")
    .option("escape", "\"")
    .schema(schema_MbrPcpAttrbtnKey)
    .load(f"{adls_path}/key/{InFile}")
)

# Apply Transformer logic (Transformer_1)
df_transformer_1 = (
    df_MbrPcpAttrbtnKey
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("DiscardIn", lit("N"))
    .withColumn(
        "svAttrbtnBcbsPlnCdSk",
        GetFkeyClctnDomainCodes(
            "FACETS",
            col("MBR_PCP_ATTRBTN_SK"),
            lit("ACTIVATING BCBS PLAN"),
            lit("FACETS DBO"),
            lit("ACTIVATING BCBS PLAN"),
            lit("IDS"),
            col("ATTRBTN_BCBS_PLN_CD_SRC_CD"),
            Logging
        )
    )
    .withColumn(
        "svEffDtSk",
        GetFkeyDate(
            lit("IDS"),
            col("MBR_PCP_ATTRBTN_SK"),
            col("ROW_EFF_DT_SK"),
            Logging
        )
    )
    .withColumn("ErrCt", col("ERR_CT"))
)

# Recycle link output (constraint: ErrCt > 0) -> hf_recycle (CHashedFileStage - scenario C => write parquet)
df_recycle = (
    df_transformer_1
    .filter(col("ErrCt") > 0)
    .select(
        GetRecycleKey(col("MBR_PCP_ATTRBTN_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DiscardIn").alias("DISCARD_IN"),
        col("PassThru").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCt").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("MBR_PCP_ATTRBTN_SK").alias("MBR_PCP_ATTRBTN_SK"),
        col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("ATTRBTN_BCBS_PLN_CD").alias("ATTRBTN_BCBS_PLN_CD"),
        lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
        col("svEffDtSk").alias("ROW_EFF_DT_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("MBR_SK").alias("MBR_SK"),
        col("PROV_SK").alias("PROV_SK"),
        col("REL_GRP_PROV_SK").alias("REL_GRP_PROV_SK"),
        col("COB_IN").alias("COB_IN"),
        col("LAST_EVAL_AND_MNG_SVC_DT_SK").alias("LAST_EVAL_AND_MNG_SVC_DT_SK"),
        col("INDV_BE_KEY").alias("INDV_BE_KEY"),
        col("MBR_PCP_MO_NO").alias("MBR_PCP_MO_NO"),
        col("MED_HOME_ID").alias("MED_HOME_ID"),
        col("MED_HOME_DESC").alias("MED_HOME_DESC"),
        col("MED_HOME_GRP_ID").alias("MED_HOME_GRP_ID"),
        col("MED_HOME_GRP_DESC").alias("MED_HOME_GRP_DESC"),
        col("MED_HOME_LOC_ID").alias("MED_HOME_LOC_ID"),
        col("MED_HOME_LOC_DESC").alias("MED_HOME_LOC_DESC")
    )
)

write_files(
    df_recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# FKey link output (constraint: ErrCt=0 or PassThru='Y')
df_fkey = (
    df_transformer_1
    .filter((col("ErrCt") == 0) | (col("PassThru") == lit("Y")))
    .select(
        col("MBR_PCP_ATTRBTN_SK").alias("MBR_PCP_ATTRBTN_SK"),
        col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("ATTRBTN_BCBS_PLN_CD").alias("ATTRBTN_BCBS_PLN_CD"),
        col("svEffDtSk").alias("ROW_EFF_DT_SK"),
        lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("MBR_SK").alias("MBR_SK"),
        col("PROV_SK").alias("PROV_SK"),
        col("svAttrbtnBcbsPlnCdSk").alias("ATTRBTN_BCBS_PLN_CD_SK"),
        col("REL_GRP_PROV_SK").alias("REL_GRP_PROV_SK"),
        col("COB_IN").alias("COB_IN"),
        col("LAST_EVAL_AND_MNG_SVC_DT_SK").alias("LAST_EVAL_AND_MNG_SVC_DT_SK"),
        col("INDV_BE_KEY").alias("INDV_BE_KEY"),
        col("MBR_PCP_MO_NO").alias("MBR_PCP_MO_NO"),
        col("MED_HOME_ID").alias("MED_HOME_ID"),
        col("MED_HOME_DESC").alias("MED_HOME_DESC"),
        col("MED_HOME_GRP_ID").alias("MED_HOME_GRP_ID"),
        col("MED_HOME_GRP_DESC").alias("MED_HOME_GRP_DESC"),
        col("MED_HOME_LOC_ID").alias("MED_HOME_LOC_ID"),
        col("MED_HOME_LOC_DESC").alias("MED_HOME_LOC_DESC")
    )
)

# DefaultUNK link output (constraint: @INROWNUM=1 => exactly 1 row with constants)
df_defaultUNK = spark.createDataFrame(
    [
        {
            "MBR_PCP_ATTRBTN_SK": 0,
            "MBR_UNIQ_KEY": 0,
            "ATTRBTN_BCBS_PLN_CD": "UNK",
            "ROW_EFF_DT_SK": "1753-01-01",
            "SRC_SYS_CD_SK": 0,
            "CRT_RUN_CYC_EXCTN_SK": RunCycle,
            "LAST_UPDT_RUN_CYC_EXCTN_SK": RunCycle,
            "MBR_SK": 0,
            "PROV_SK": 0,
            "ATTRBTN_BCBS_PLN_CD_SK": 0,
            "REL_GRP_PROV_SK": 0,
            "COB_IN": "0",
            "LAST_EVAL_AND_MNG_SVC_DT_SK": "1753-01-01",
            "INDV_BE_KEY": 0,
            "MBR_PCP_MO_NO": 0,
            "MED_HOME_ID": "UNK",
            "MED_HOME_DESC": "UNK",
            "MED_HOME_GRP_ID": "UNK",
            "MED_HOME_GRP_DESC": "UNK",
            "MED_HOME_LOC_ID": "UNK",
            "MED_HOME_LOC_DESC": "UNK"
        }
    ]
)

# DefaultNA link output (constraint: @INROWNUM=1 => exactly 1 row with constants)
df_defaultNA = spark.createDataFrame(
    [
        {
            "MBR_PCP_ATTRBTN_SK": 1,
            "MBR_UNIQ_KEY": 1,
            "ATTRBTN_BCBS_PLN_CD": "NA",
            "ROW_EFF_DT_SK": "1753-01-01",
            "SRC_SYS_CD_SK": 1,
            "CRT_RUN_CYC_EXCTN_SK": RunCycle,
            "LAST_UPDT_RUN_CYC_EXCTN_SK": RunCycle,
            "MBR_SK": 1,
            "PROV_SK": 1,
            "ATTRBTN_BCBS_PLN_CD_SK": 1,
            "REL_GRP_PROV_SK": 1,
            "COB_IN": "1",
            "LAST_EVAL_AND_MNG_SVC_DT_SK": "1753-01-01",
            "INDV_BE_KEY": 1,
            "MBR_PCP_MO_NO": 1,
            "MED_HOME_ID": "NA",
            "MED_HOME_DESC": "NA",
            "MED_HOME_GRP_ID": "NA",
            "MED_HOME_GRP_DESC": "NA",
            "MED_HOME_LOC_ID": "NA",
            "MED_HOME_LOC_DESC": "NA"
        }
    ]
)

# Collector stage (CCollector): union of df_fkey, df_defaultUNK, df_defaultNA => single output
df_collected = (
    df_fkey
    .unionByName(df_defaultUNK)
    .unionByName(df_defaultNA)
)

# Final select with rpad for char columns in the final file
df_final = df_collected.select(
    col("MBR_PCP_ATTRBTN_SK"),
    col("MBR_UNIQ_KEY"),
    col("ATTRBTN_BCBS_PLN_CD"),
    rpad(col("ROW_EFF_DT_SK"), 10, " ").alias("ROW_EFF_DT_SK"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_SK"),
    col("PROV_SK"),
    col("ATTRBTN_BCBS_PLN_CD_SK"),
    col("REL_GRP_PROV_SK"),
    rpad(col("COB_IN"), 1, " ").alias("COB_IN"),
    rpad(col("LAST_EVAL_AND_MNG_SVC_DT_SK"), 10, " ").alias("LAST_EVAL_AND_MNG_SVC_DT_SK"),
    col("INDV_BE_KEY"),
    col("MBR_PCP_MO_NO"),
    col("MED_HOME_ID"),
    col("MED_HOME_DESC"),
    col("MED_HOME_GRP_ID"),
    col("MED_HOME_GRP_DESC"),
    col("MED_HOME_LOC_ID"),
    col("MED_HOME_LOC_DESC")
)

# MBR_PCP_ATTRBTN CSeqFileStage write to .dat
write_files(
    df_final,
    f"{adls_path}/load/MBR_PCP_ATTRBTN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)