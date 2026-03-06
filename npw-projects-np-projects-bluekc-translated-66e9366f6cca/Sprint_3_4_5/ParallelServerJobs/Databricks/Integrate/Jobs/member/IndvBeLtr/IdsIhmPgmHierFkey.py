# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsIhmPgmHierFkey
# MAGIC 
# MAGIC DESCRIPTION:    Populates foreign keys for IHM Program Hierarchy table
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	Flat file created in job UWSIhmPgmHierExtr with primary key
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:    reads flat file from job UWSIhmPgmHierExtr and performs foreign-key and code translation via corresponding lookup routines
# MAGIC   
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Judy Reynolds                  2010-07-09       4297  - Alineo PH2     Original Programming                                                                             IntegrateNewDevl          Steph Goddard            07/15/2010
# MAGIC 
# MAGIC Judy Reynolds                  2010-08-13       4297 - Alineo Ph 2      Modified to support table key and field changes                                    RebuildIntNewDevl        Steph Goddard           08/18/2010

# MAGIC Read IhmPgmHier file created in UWSIhmPgmHierExtr for foreign key look-up in a common record format
# MAGIC Capture records generating translation errors to be uploaded to the IDS recycle table
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
    TimestampType,
    DecimalType
)
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    monotonically_increasing_id,
    rpad
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Job Parameters
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
InFile = get_widget_value('InFile', '')
Logging = get_widget_value('Logging', '')
SourceSK = get_widget_value('SourceSK', '')
CurrDate = get_widget_value('CurrDate', '')

# Read from IhmPgmHierFile (CSeqFileStage)
schema_IhmPgmHierFile = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), True),
    StructField("INSRT_UPDT_CD", StringType(), True),
    StructField("DISCARD_IN", StringType(), True),
    StructField("PASS_THRU_IN", StringType(), True),
    StructField("FIRST_RECYC_DT", TimestampType(), True),
    StructField("ERR_CT", IntegerType(), True),
    StructField("RECYCLE_CT", DecimalType(38, 10), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("PRI_KEY_STRING", StringType(), True),
    StructField("IHM_PGM_HIER_SK", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("PGM_ID", StringType(), True),
    StructField("SBPRG_ID", StringType(), True),
    StructField("RISK_SVRTY_CD", StringType(), True),
    StructField("IHM_PGM_HIER_NO", IntegerType(), True),
    StructField("IHM_PGM_HIER_RANK_NO", IntegerType(), True),
    StructField("USER_ID", StringType(), True),
    StructField("LAST_UPDT_DT_SK", StringType(), True)
])

df_IhmPgmHierFile = (
    spark.read
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_IhmPgmHierFile)
    .csv(f"{adls_path}/key/{InFile}")
)

# ForeignKey (CTransformerStage) logic
dfNew = (
    df_IhmPgmHierFile
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn(
        "RiskSvrtySk",
        GetFkeyCodes(
            col("SRC_SYS_CD"),
            col("IHM_PGM_HIER_SK"),
            lit("RISK SEVERITY"),
            col("RISK_SVRTY_CD"),
            lit(Logging)
        )
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(col("IHM_PGM_HIER_SK")))
)

df_Fkey = (
    dfNew
    .filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
    .select(
        col("IHM_PGM_HIER_SK").alias("IHM_PGM_HIER_SK"),
        col("PGM_ID").alias("PGM_ID"),
        col("SBPRG_ID").alias("SBPRG_ID"),
        col("RISK_SVRTY_CD").alias("RISK_SVRTY_CD"),
        lit(SourceSK).alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("RiskSvrtySk").alias("RISK_SVRTY_CD_SK"),
        col("IHM_PGM_HIER_NO").alias("IHM_PGM_HIER_NO"),
        col("IHM_PGM_HIER_RANK_NO").alias("IHM_PGM_HIER_RANK_NO"),
        col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
        col("USER_ID").alias("LAST_UPDT_USER_ID")
    )
)

df_Recycle = (
    dfNew
    .filter(col("ErrCount") > 0)
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(col("IHM_PGM_HIER_SK")))
    .withColumn("RECYCLE_CT", col("RECYCLE_CT") + lit(1))
    .select(
        col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        lit(SourceSK).alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("PGM_ID").alias("PGM_ID"),
        col("SBPRG_ID").alias("SBPRG_ID"),
        col("RISK_SVRTY_CD").alias("RISK_SVRTY_CD"),
        col("IHM_PGM_HIER_NO").alias("IHM_PGM_HIER_NO"),
        col("IHM_PGM_HIER_RANK_NO").alias("IHM_PGM_HIER_RANK_NO"),
        col("USER_ID").alias("USER_ID"),
        col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
    )
)

dfOne = dfNew.limit(1)

df_DefaultNA = dfOne.select(
    lit(1).alias("IHM_PGM_HIER_SK"),
    lit('NA').alias("PGM_ID"),
    lit('NA').alias("SBPRG_ID"),
    col("RISK_SVRTY_CD").alias("RISK_SVRTY_CD"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("RISK_SVRTY_CD_SK"),
    lit(1).alias("IHM_PGM_HIER_NO"),
    lit(1).alias("IHM_PGM_HIER_RANK_NO"),
    lit('1753-01-01').alias("LAST_UPDT_DT_SK"),
    lit('NA').alias("LAST_UPDT_USER_ID")
)

df_DefaultUNK = dfOne.select(
    lit(0).alias("IHM_PGM_HIER_SK"),
    lit('UNK').alias("PGM_ID"),
    lit('UNK').alias("SBPRG_ID"),
    col("RISK_SVRTY_CD").alias("RISK_SVRTY_CD"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("RISK_SVRTY_CD_SK"),
    lit(0).alias("IHM_PGM_HIER_NO"),
    lit(0).alias("IHM_PGM_HIER_RANK_NO"),
    lit('1753-01-01').alias("LAST_UPDT_DT_SK"),
    lit('UNK').alias("LAST_UPDT_USER_ID")
)

# Write hf_recycle (CHashedFileStage, scenario C)
write_files(
    df_Recycle,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Collector (CCollector) - union all
df_Collector = (
    df_Fkey
    .unionByName(df_DefaultNA)
    .unionByName(df_DefaultUNK)
)

# Apply rpad for final output columns that are char or varchar
df_Collector_final = (
    df_Collector
    .withColumn("PGM_ID", rpad(col("PGM_ID"), <...>, " "))
    .withColumn("SBPRG_ID", rpad(col("SBPRG_ID"), <...>, " "))
    .withColumn("RISK_SVRTY_CD", rpad(col("RISK_SVRTY_CD"), <...>, " "))
    .withColumn("LAST_UPDT_DT_SK", rpad(col("LAST_UPDT_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_USER_ID", rpad(col("LAST_UPDT_USER_ID"), <...>, " "))
)

df_Collector_final = df_Collector_final.select(
    "IHM_PGM_HIER_SK",
    "PGM_ID",
    "SBPRG_ID",
    "RISK_SVRTY_CD",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "RISK_SVRTY_CD_SK",
    "IHM_PGM_HIER_NO",
    "IHM_PGM_HIER_RANK_NO",
    "LAST_UPDT_DT_SK",
    "LAST_UPDT_USER_ID"
)

# Write to IHM_PGM_HIER_LoadFile (CSeqFileStage)
write_files(
    df_Collector_final,
    f"{adls_path}/load/IHM_PGM_HIER.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)