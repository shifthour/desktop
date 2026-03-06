# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2021 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmLnDsalwFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:     .../key/FctsClmLnDsalwExtr.FctsClmLnDsalw.uniq
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle - recycle hash file written to if there are errors in the foreign key assignment process
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes - for SRC_SYS_CD and CLM_LN_DSALW_CD
# MAGIC                             GetFkeyClmLn
# MAGIC                             GetFkeyExcd
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:   assign foreign key codes and create clm_ln_dsalw file
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:   .../load/CLM_LN_DSALW.#Source#.dat
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Tom Harrocks  04/12/2004  -   Originally Programmed
# MAGIC             Tom Harrocks  09/08/2004  -   Added UNK and NA handling
# MAGIC             Steph Goddard 10/19/2004 -   updated labels to read clm ln dsalw 
# MAGIC             BJ luce              1/12/2006 -    add CLM_LN_DSALW_TYP_CAT_CD_SK to output. input stayed the same because code lookup uses CLM_LN_DSALW_TYP_CD with a different domain
# MAGIC             Brent Leland       08/07/2006    Added current run cycle to output records.  Without the current run cycle, error recycle records would have an old run cycle and would be removed in delete process.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Ralph Tucker          10/11/2007      15                          Took out trim on EXCD field; Caused Nasco to output UNK's     devlIDS30                       Steph Goddard           10/18/2007   
# MAGIC 
# MAGIC Parik                       2008-07-25        3567(Primary Key)  Added Source System Code Surrogate key as parameter            devlIDS                          Steph Goddard           07/29/2008
# MAGIC 
# MAGIC Reddy Sanam       2020-10-10                                      Created stage variable "svSrcSysCd" to map FACETS
# MAGIC                                                                                      when the source is LUMERIS
# MAGIC 
# MAGIC Sunitha Ganta         10-12-2020                                    Brought up to standards 
# MAGIC 
# MAGIC Lakshmi Devagiri     2021-02-03       RA/US343103    Updated Stage Variable "ClmLnDsalwTypCatCd" for "DentaQuest"  IntegrateDev2             Kalyan Neelam            2021-02-03
# MAGIC Mrudula Kodali                05/03/2021  US-373652        Updated the stage variables ClmLnDsalwTypCatCd		IntegrateDev2               Jaideep Mankala       05/10/2021
# MAGIC                                                                                        Removed usages of FACETS for EYEMED, DENTAQUEST, SOLUTRON, LVNGHLTH, EMR

# MAGIC Writing Sequential File to /load
# MAGIC Recycle records with ErrCount > 0
# MAGIC Read common record format file from extract job.
# MAGIC Merge source data with default rows
# MAGIC Set all foreign surragote keys
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
from pyspark.sql.functions import col, when, row_number, monotonically_increasing_id, lit, rpad, trim
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','')
Source = get_widget_value('Source','')
Logging = get_widget_value('Logging','')
RunCycle = get_widget_value('RunCycle','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_ClmLnDsalwExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("CLM_LN_DSALW_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), nullable=False),
    StructField("CLM_LN_DSALW_TYP_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CLM_LN_DSALW_EXCD", StringType(), nullable=False),
    StructField("DSALW_AMT", DecimalType(38,10), nullable=False)
])

df_ClmLnDsalwExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_ClmLnDsalwExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_ForeignKeyStage = (
    df_ClmLnDsalwExtr
    .withColumn(
        "svSrcSysCd",
        when(col("SRC_SYS_CD") == "LUMERIS", lit("FACETS")).otherwise(col("SRC_SYS_CD"))
    )
    .withColumn(
        "ClmLnSk",
        GetFkeyClmLn(
            col("SRC_SYS_CD"),
            col("CLM_LN_DSALW_SK"),
            trim(col("CLM_ID")),
            col("CLM_LN_SEQ_NO"),
            lit(Logging)
        )
    )
    .withColumn(
        "ClmLnDsalwTypCd",
        GetFkeyCodes(
            col("svSrcSysCd"),
            col("CLM_LN_DSALW_SK"),
            lit("CLAIM LINE DISALLOW TYPE"),
            trim(col("CLM_LN_DSALW_TYP_CD")),
            lit(Logging)
        )
    )
    .withColumn(
        "ClmLnDsalwTypCatCd",
        when(
            col("SRC_SYS_CD") == "DENTAQUEST",
            GetFkeyCodes(
                col("svSrcSysCd"),
                col("CLM_LN_DSALW_SK"),
                lit("DISALLOW TYPE CATEGORY"),
                lit("EX"),
                lit(Logging)
            )
        ).otherwise(
            GetFkeyCodes(
                col("svSrcSysCd"),
                col("CLM_LN_DSALW_SK"),
                lit("DISALLOW TYPE CATEGORY"),
                trim(col("CLM_LN_DSALW_TYP_CD")),
                lit(Logging)
            )
        )
    )
    .withColumn(
        "ClmLnDsalwExcd",
        GetFkeyExcd(
            col("SRC_SYS_CD"),
            col("CLM_LN_DSALW_SK"),
            col("CLM_LN_DSALW_EXCD"),
            lit(Logging)
        )
    )
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("CLM_LN_DSALW_SK")))
    .withColumn("_row_num", row_number().over(Window.orderBy(monotonically_increasing_id())))
)

df_Fkey = (
    df_ForeignKeyStage
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("CLM_LN_DSALW_SK").alias("CLM_LN_DSALW_SK"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("CLM_ID").alias("CLM_ID"),
        col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("ClmLnDsalwTypCd").alias("CLM_LN_DSALW_TYP_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("ClmLnSk").alias("CLM_LN_SK"),
        col("ClmLnDsalwExcd").alias("CLM_LN_DSALW_EXCD_SK"),
        col("DSALW_AMT").alias("DSALW_AMT"),
        col("ClmLnDsalwTypCatCd").alias("CLM_LN_DSALW_TYP_CAT_CD_SK")
    )
)

df_DefaultUNK = (
    df_ForeignKeyStage
    .filter(col("_row_num") == 1)
    .select(
        lit(0).alias("CLM_LN_DSALW_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("CLM_ID"),
        lit(0).alias("CLM_LN_SEQ_NO"),
        lit(0).alias("CLM_LN_DSALW_TYP_CD_SK"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("CLM_LN_SK"),
        lit(0).alias("CLM_LN_DSALW_EXCD_SK"),
        lit(0).alias("DSALW_AMT"),
        lit(0).alias("CLM_LN_DSALW_TYP_CAT_CD_SK")
    )
)

df_DefaultNA = (
    df_ForeignKeyStage
    .filter(col("_row_num") == 1)
    .select(
        lit(1).alias("CLM_LN_DSALW_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("CLM_ID"),
        lit(1).alias("CLM_LN_SEQ_NO"),
        lit(1).alias("CLM_LN_DSALW_TYP_CD_SK"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("CLM_LN_SK"),
        lit(1).alias("CLM_LN_DSALW_EXCD_SK"),
        lit(0).alias("DSALW_AMT"),
        lit(1).alias("CLM_LN_DSALW_TYP_CAT_CD_SK")
    )
)

df_Recycle = (
    df_ForeignKeyStage
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("CLM_LN_DSALW_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("CLM_LN_DSALW_SK").alias("CLM_LN_DSALW_SK"),
        col("CLM_ID").alias("CLM_ID"),
        col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CLM_LN_DSALW_EXCD").alias("CLM_LN_DSALW_EXCD"),
        col("DSALW_AMT").alias("DSALW_AMT")
    )
)

df_Recycle_Clms = (
    df_ForeignKeyStage
    .filter(col("ErrCount") > 0)
    .select(
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("CLM_ID").alias("CLM_ID")
    )
)

write_files(
    df_Recycle,
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

write_files(
    df_Recycle_Clms,
    f"{adls_path}/hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_collRows = (
    df_Fkey
    .unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
)

write_files(
    df_collRows,
    f"{adls_path}/load/CLM_LN_DSALW.{Source}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)