# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClnCOBFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.    
# MAGIC       
# MAGIC 
# MAGIC INPUTS: Sequential file in common record format created by primary key job.
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:      hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes for  "SOURCE SYSTEM"
# MAGIC                             GetFkeyClmLn  
# MAGIC                             GetFkeyCodes  for  "CLAIM LINE CLINICAL EDIT ACTION"
# MAGIC                             GetFkeyCodes for  "CLAIM LINE CLINICAL EDIT FORMAT CHANGE"
# MAGIC                             GetFkeyCodes for  "CLAIM LINE CLINICAL EDIT TYPE'
# MAGIC                             GetFkeyErrorCnt
# MAGIC                             GetRecycleKey
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING: get foreign key as stated above and create file to load to CLM_COB
# MAGIC OUTPUTS:  #TmpOutFile# which will be used to update CLM_COB
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Steph Goddard   1/2005 -   Originally Programmed
# MAGIC             Hall                     3/2005 -   Added logic to handle the lookup for COB Type for ITS Host
# MAGIC             SAndrew             6//2005 -  Removed fields from CLM_COB record output that is loaded to table.  Did not removed from Common Record Format nor from the recycle record to prevent conversion of recycled records.
# MAGIC             Steph Goddard   2/2006      Changed for sequencer
# MAGIC            Brent Leland       08/23/3006   Changed CLM_COB_TYP field definition from unknown to varchar(20).
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer           Date                        Project/Altius #     Change Description                                                     Development Project          Code Reviewer          Date Reviewed  
# MAGIC -----------------------    ----------------------------    ---------------------------  ----------------------------------------------------------------------------        ------------------------------------       ----------------------------      -------------------------
# MAGIC Ralph Tucker      2008-7-25              3657 Primary Key   Added SrcSysCdSk parameter                                     devlIDS                              Steph Goddard           07/27/2008
# MAGIC 
# MAGIC Ramu A                 2020-10-12                                          Takes the sorted file and applies the foreign keys.    IntegrateDev2                   Jaideep Mankala         12/10/2020

# MAGIC Read common record format file created in the primary key job.
# MAGIC Writing Sequential File to /load
# MAGIC Add default records for NA and UNK.
# MAGIC Recycle records with ErrCount > 0
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value('Source','')
InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','')
SrcSysCdSkParam = get_widget_value('SrcSysCdSk','')

schema_ClmCOBPk = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CLM_COB_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_COB_TYP_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CLM_SK", IntegerType(), False),
    StructField("CLM_COB_LIAB_TYP_CD", StringType(), False),
    StructField("ALW_AMT", DecimalType(38,10), False),
    StructField("COPAY_AMT", DecimalType(38,10), False),
    StructField("DEDCT_AMT", DecimalType(38,10), False),
    StructField("DSALW_AMT", DecimalType(38,10), False),
    StructField("MED_COINS_AMT", DecimalType(38,10), False),
    StructField("MNTL_HLTH_COINS_AMT", DecimalType(38,10), False),
    StructField("PD_AMT", DecimalType(38,10), False),
    StructField("SANC_AMT", DecimalType(38,10), False),
    StructField("COB_CAR_RSN_CD_TX", StringType(), False),
    StructField("COB_CAR_RSN_TX", StringType(), False)
])

df_ClmCOBPk = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_ClmCOBPk)
    .load(f"{adls_path}/key/{InFile}")
)

df_ForeignKey = (
    df_ClmCOBPk
    .withColumn("_svSrcSysCd", F.when(F.col("SRC_SYS_CD") == "LUMERIS", "FACETS").otherwise(F.col("SRC_SYS_CD")))
    .withColumn("_ITSHost", F.when(F.substring(F.col("CLM_COB_TYP_CD"), 1, 1) == "*", "Y").otherwise("N"))
    .withColumn("_ClmSk", GetFkeyClm(F.col("SRC_SYS_CD"), F.col("CLM_COB_SK"), F.col("CLM_ID"), Logging))
    .withColumn("_ITSCOBType", F.when(F.col("_ITSHost") == "Y", F.substring(F.col("CLM_COB_TYP_CD"), 2, 1)).otherwise("NA"))
    .withColumn(
        "_ClmCobTypCdSk",
        F.when(
            F.col("SRC_SYS_CD") == "OPTUMRX",
            GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CLM_COB_SK"), F.lit("CLAIM COB"), F.col("CLM_COB_TYP_CD"), Logging)
        ).otherwise(
            F.when(
                F.col("_ITSHost") == "N",
                GetFkeyCodes(F.col("_svSrcSysCd"), F.col("CLM_COB_SK"), F.lit("CLAIM COB"), F.col("CLM_COB_TYP_CD"), Logging)
            ).otherwise(
                GetFkeyCodes(F.lit("FIT"), F.col("CLM_COB_SK"), F.lit("CLAIM COB"), F.col("_ITSCOBType"), Logging)
            )
        )
    )
    .withColumn(
        "_ClmCobLiabTypCd",
        F.when(
            F.col("SRC_SYS_CD") == "OPTUMRX",
            GetFkeyCodes(
                F.col("SRC_SYS_CD"),
                F.col("CLM_COB_SK"),
                F.lit("CLAIM COB LIABILITY GROUP TYPE"),
                F.col("CLM_COB_LIAB_TYP_CD"),
                Logging
            )
        ).otherwise(
            GetFkeyCodes(
                F.col("_svSrcSysCd"),
                F.col("CLM_COB_SK"),
                F.lit("CLAIM COB LIABILITY GROUP TYPE"),
                F.col("CLM_COB_LIAB_TYP_CD"),
                Logging
            )
        )
    )
    .withColumn("_PassThru", F.col("PASS_THRU_IN"))
    .withColumn("_ErrCount", GetFkeyErrorCnt(F.col("CLM_COB_SK")))
)

w = Window.orderBy(F.lit(1))
df_ForeignKeyWithRownum = df_ForeignKey.withColumn("_row_num", F.row_number().over(w))

df_recycle = (
    df_ForeignKeyWithRownum.filter(F.col("_ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("CLM_COB_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("_ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CLM_COB_SK").alias("CLM_COB_SK"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.when(F.col("_ITSHost") == "N", F.col("CLM_COB_TYP_CD")).otherwise(F.col("_ITSCOBType")).alias("CLM_COB_TYP_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("CLM_COB_LIAB_TYP_CD").alias("CLM_COB_LIAB_TYP_CD"),
        F.col("ALW_AMT").alias("ALW_AMT"),
        F.col("COPAY_AMT").alias("COPAY_AMT"),
        F.col("DEDCT_AMT").alias("DEDCT_AMT"),
        F.col("DSALW_AMT").alias("DSALW_AMT"),
        F.col("MED_COINS_AMT").alias("MED_COINS_AMT"),
        F.col("MNTL_HLTH_COINS_AMT").alias("MNTL_HLTH_COINS_AMT"),
        F.col("PD_AMT").alias("PD_AMT"),
        F.col("SANC_AMT").alias("SANC_AMT"),
        F.col("COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
        F.col("COB_CAR_RSN_TX").alias("COB_CAR_RSN_TX")
    )
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

df_recycle_clms = (
    df_ForeignKeyWithRownum.filter(F.col("_ErrCount") > 0)
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID")
    )
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

df_defaultUNK = (
    df_ForeignKeyWithRownum.filter(F.col("_row_num") == 1)
    .select(
        F.lit(0).alias("CLM_COB_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("CLM_ID"),
        F.lit(0).alias("CLM_COB_TYP_CD_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("CLM_SK"),
        F.lit(0).alias("CLM_COB_LIAB_TYP_CD_SK"),
        F.lit(0.00).alias("ALW_AMT"),
        F.lit(0.00).alias("COPAY_AMT"),
        F.lit(0.00).alias("DEDCT_AMT"),
        F.lit(0.00).alias("DSALW_AMT"),
        F.lit(0.00).alias("MED_COINS_AMT"),
        F.lit(0.00).alias("MNTL_HLTH_COINS_AMT"),
        F.lit(0.00).alias("PD_AMT"),
        F.lit(0.00).alias("SANC_AMT"),
        F.lit("UNK").alias("COB_CAR_RSN_CD_TX"),
        F.lit("UNK").alias("COB_CAR_RSN_TX")
    )
)

df_defaultNA = (
    df_ForeignKeyWithRownum.filter(F.col("_row_num") == 1)
    .select(
        F.lit(1).alias("CLM_COB_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("CLM_ID"),
        F.lit(1).alias("CLM_COB_TYP_CD_SK"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("CLM_SK"),
        F.lit(1).alias("CLM_COB_LIAB_TYP_CD_SK"),
        F.lit(0.00).alias("ALW_AMT"),
        F.lit(0.00).alias("COPAY_AMT"),
        F.lit(0.00).alias("DEDCT_AMT"),
        F.lit(0.00).alias("DSALW_AMT"),
        F.lit(0.00).alias("MED_COINS_AMT"),
        F.lit(0.00).alias("MNTL_HLTH_COINS_AMT"),
        F.lit(0.00).alias("PD_AMT"),
        F.lit(0.00).alias("SANC_AMT"),
        F.lit("NA").alias("COB_CAR_RSN_CD_TX"),
        F.lit("NA").alias("COB_CAR_RSN_TX")
    )
)

df_fkey = (
    df_ForeignKeyWithRownum.filter((F.col("_ErrCount") == 0) | (F.col("_PassThru") == "Y"))
    .select(
        F.col("CLM_COB_SK").alias("CLM_COB_SK"),
        F.lit(SrcSysCdSkParam).alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("_ClmCobTypCdSk").alias("CLM_COB_TYP_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("_ClmSk").alias("CLM_SK"),
        F.col("_ClmCobLiabTypCd").alias("CLM_COB_LIAB_TYP_CD_SK"),
        F.col("ALW_AMT").alias("ALW_AMT"),
        F.col("COPAY_AMT").alias("COPAY_AMT"),
        F.col("DEDCT_AMT").alias("DEDCT_AMT"),
        F.col("DSALW_AMT").alias("DSALW_AMT"),
        F.col("MED_COINS_AMT").alias("MED_COINS_AMT"),
        F.col("MNTL_HLTH_COINS_AMT").alias("MNTL_HLTH_COINS_AMT"),
        F.col("PD_AMT").alias("PD_AMT"),
        F.col("SANC_AMT").alias("SANC_AMT"),
        F.col("COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
        F.col("COB_CAR_RSN_TX").alias("COB_CAR_RSN_TX")
    )
)

df_collector = df_defaultUNK.union(df_defaultNA).union(df_fkey)

write_files(
    df_collector.select(
        "CLM_COB_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_COB_TYP_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "CLM_COB_LIAB_TYP_CD_SK",
        "ALW_AMT",
        "COPAY_AMT",
        "DEDCT_AMT",
        "DSALW_AMT",
        "MED_COINS_AMT",
        "MNTL_HLTH_COINS_AMT",
        "PD_AMT",
        "SANC_AMT",
        "COB_CAR_RSN_CD_TX",
        "COB_CAR_RSN_TX"
    ),
    f"{adls_path}/load/CLM_COB.{Source}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)