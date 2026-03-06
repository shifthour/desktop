# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClnLnCOBFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.    
# MAGIC       
# MAGIC 
# MAGIC INPUTS:   .../key/FctsClmLnCOBExtr.FctsClmLnCOB.uniq
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
# MAGIC PROCESSING: get foreign key as stated above and create file to load to CLM_LN_COB
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  .../load/CLM_LN_COB.#Source#.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Steph Goddard   12/2005 -   Originally Programmed
# MAGIC             Steph Goddard   03/01/2006  changes for sequencer
# MAGIC 
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                          Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                   --------------------------------       -------------------------------   ----------------------------    
# MAGIC Parik                        2008-07-25       3567(Primary Key)  Added Source System Code SK to the job parameters                                       devlIDS                         Steph Goddard          07/27/2008
# MAGIC 
# MAGIC Sunitha Ganta         10-12-2020                         Brought up to standards

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Writing Sequential File to /load
# MAGIC Merge source data with default rows
# MAGIC Recycle records with ErrCount > 0
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value('Source','')
InFile = get_widget_value('InFile','IdsClmLnCOBPkey.ClmLnCOBTMP.dat')
Logging = get_widget_value('Logging','Y')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_ClmLnCOBExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("CLM_COB_LN_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), nullable=False),
    StructField("CLM_LN_COB_TYP_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CLM_LN_COB_CAR_PRORTN_CD", StringType(), nullable=False),
    StructField("CLM_LN_COB_LIAB_TYP_CD", StringType(), nullable=False),
    StructField("COB_CAR_ADJ_AMT", DecimalType(38,10), nullable=False),
    StructField("ALW_AMT", DecimalType(38,10), nullable=False),
    StructField("APLD_AMT", DecimalType(38,10), nullable=False),
    StructField("COPAY_AMT", DecimalType(38,10), nullable=False),
    StructField("DEDCT_AMT", DecimalType(38,10), nullable=False),
    StructField("DSALW_AMT", DecimalType(38,10), nullable=False),
    StructField("COINS_AMT", DecimalType(38,10), nullable=False),
    StructField("MNTL_HLTH_COINS_AMT", DecimalType(38,10), nullable=False),
    StructField("OOP_AMT", DecimalType(38,10), nullable=False),
    StructField("PD_AMT", DecimalType(38,10), nullable=False),
    StructField("SANC_AMT", DecimalType(38,10), nullable=False),
    StructField("SAV_AMT", DecimalType(38,10), nullable=False),
    StructField("SUBTR_AMT", DecimalType(38,10), nullable=False),
    StructField("COB_CAR_RSN_CD_TX", StringType(), nullable=True),
    StructField("COB_CAR_RSN_TX", StringType(), nullable=True),
])

dfKey = (
    spark.read
    .option("quote", '"')
    .option("header", False)
    .option("delimiter", ",")
    .schema(schema_ClmLnCOBExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_enriched = dfKey.withColumn(
    "svSrcSysCd",
    F.when(F.col("SRC_SYS_CD") == F.lit("LUMERIS"), F.lit("FACETS")).otherwise(F.col("SRC_SYS_CD"))
)

df_enriched = df_enriched.withColumn(
    "ClmLnSk",
    GetFkeyClmLn(
        F.col("SRC_SYS_CD"),
        F.col("CLM_COB_LN_SK"),
        F.col("CLM_ID"),
        F.col("CLM_LN_SEQ_NO"),
        F.lit(Logging)
    )
)

df_enriched = df_enriched.withColumn(
    "ClmLnCobTypCdSk",
    F.when(
        F.col("CLM_LN_COB_TYP_CD").substr(1, 1) == F.lit("*"),
        GetFkeyCodes(
            F.lit("FIT"),
            F.col("CLM_COB_LN_SK"),
            F.lit("CLAIM COB"),
            F.col("CLM_LN_COB_TYP_CD").substr(2, 1),
            F.lit(Logging)
        )
    ).otherwise(
        GetFkeyCodes(
            F.col("svSrcSysCd"),
            F.col("CLM_COB_LN_SK"),
            F.lit("CLAIM COB"),
            F.col("CLM_LN_COB_TYP_CD"),
            F.lit(Logging)
        )
    )
)

df_enriched = df_enriched.withColumn(
    "CarPrortnCdSk",
    GetFkeyCodes(
        F.col("svSrcSysCd"),
        F.col("CLM_COB_LN_SK"),
        F.lit("CLAIM LINE COB CARRIER PRORATION"),
        F.col("CLM_LN_COB_CAR_PRORTN_CD"),
        F.lit(Logging)
    )
)

df_enriched = df_enriched.withColumn(
    "LiabTypCd",
    GetFkeyCodes(
        F.col("svSrcSysCd"),
        F.col("CLM_COB_LN_SK"),
        F.lit("CLAIM LINE COB MEDICARE LIABILITY GROUP TYPE"),
        F.col("CLM_LN_COB_LIAB_TYP_CD"),
        F.lit(Logging)
    )
)

df_enriched = df_enriched.withColumn("PassThru", F.col("PASS_THRU_IN"))

df_enriched = df_enriched.withColumn(
    "ErrCount",
    GetFkeyErrorCnt(F.col("CLM_COB_LN_SK"))
)

df_recycle = df_enriched.filter(F.col("ErrCount") > 0).select(
    GetRecycleKey(F.col("CLM_COB_LN_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_COB_LN_SK").alias("CLM_COB_LN_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_COB_TYP_CD").alias("CLM_LN_COB_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_COB_CAR_PRORTN_CD").alias("CLM_LN_COB_CAR_PRORTN_CD"),
    F.col("CLM_LN_COB_LIAB_TYP_CD").alias("CLM_LN_COB_LIAB_TYP_CD"),
    F.col("COB_CAR_ADJ_AMT").alias("COB_CAR_ADJ_AMT"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("APLD_AMT").alias("APLD_AMT"),
    F.col("COPAY_AMT").alias("COPAY_AMT"),
    F.col("DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.col("COINS_AMT").alias("MED_COINS_AMT"),
    F.col("MNTL_HLTH_COINS_AMT").alias("MNTL_HLTH_COINS_AMT"),
    F.col("OOP_AMT").alias("OOP_AMT"),
    F.col("PD_AMT").alias("PD_AMT"),
    F.col("SANC_AMT").alias("SANC_AMT"),
    F.col("SAV_AMT").alias("SAV_AMT"),
    F.col("SUBTR_AMT").alias("SUBTR_AMT"),
    F.col("COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
    F.col("COB_CAR_RSN_TX").alias("COB_CAR_RSN_TX")
)

write_files(
    df_recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_claim_recycle_keys = df_enriched.filter(F.col("ErrCount") > 0).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID")
)

write_files(
    df_claim_recycle_keys,
    f"{adls_path}/hf_claim_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

data_defaultUNK = [
    (0, 0, "UNK", 0, 0, 0, 0, 0, 0, 0, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, "UNK", "UNK")
]
schema_default = StructType([
    StructField("CLM_COB_LN_SK", IntegerType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_SEQ_NO", IntegerType(), True),
    StructField("CLM_LN_COB_TYP_CD_SK", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CLM_LN_SK", IntegerType(), True),
    StructField("CLM_LN_COB_CAR_PRORTN_CD_SK", IntegerType(), True),
    StructField("CLM_LN_COB_LIAB_TYP_CD_SK", IntegerType(), True),
    StructField("COB_CAR_ADJ_AMT", DecimalType(38,10), True),
    StructField("ALW_AMT", DecimalType(38,10), True),
    StructField("APLD_AMT", DecimalType(38,10), True),
    StructField("COPAY_AMT", DecimalType(38,10), True),
    StructField("DEDCT_AMT", DecimalType(38,10), True),
    StructField("DSALW_AMT", DecimalType(38,10), True),
    StructField("MED_COINS_AMT", DecimalType(38,10), True),
    StructField("MNTL_HLTH_COINS_AMT", DecimalType(38,10), True),
    StructField("OOP_AMT", DecimalType(38,10), True),
    StructField("PD_AMT", DecimalType(38,10), True),
    StructField("SANC_AMT", DecimalType(38,10), True),
    StructField("SAV_AMT", DecimalType(38,10), True),
    StructField("SUBTR_AMT", DecimalType(38,10), True),
    StructField("COB_CAR_RSN_CD_TX", StringType(), True),
    StructField("COB_CAR_RSN_TX", StringType(), True),
])

df_defaultUNK = spark.createDataFrame(data_defaultUNK, schema_default)

data_defaultNA = [
    (1, 1, "NA", 0, 1, 1, 1, 1, 1, 1, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, "NA", "NA")
]
df_defaultNA = spark.createDataFrame(data_defaultNA, schema_default)

df_fkey = df_enriched.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == "Y")
).select(
    F.col("CLM_COB_LN_SK").alias("CLM_COB_LN_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("ClmLnCobTypCdSk").alias("CLM_LN_COB_TYP_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ClmLnSk").alias("CLM_LN_SK"),
    F.col("CarPrortnCdSk").alias("CLM_LN_COB_CAR_PRORTN_CD_SK"),
    F.col("LiabTypCd").alias("CLM_LN_COB_LIAB_TYP_CD_SK"),
    F.col("COB_CAR_ADJ_AMT").alias("COB_CAR_ADJ_AMT"),
    F.when(F.col("ALW_AMT").isNull(), F.lit(0.00)).otherwise(F.col("ALW_AMT")).alias("ALW_AMT"),
    F.col("APLD_AMT").alias("APLD_AMT"),
    F.col("COPAY_AMT").alias("COPAY_AMT"),
    F.when(F.col("DEDCT_AMT").isNull(), F.lit(0.00)).otherwise(F.col("DEDCT_AMT")).alias("DEDCT_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.when(F.col("COINS_AMT").isNull(), F.lit(0.00)).otherwise(F.col("COINS_AMT")).alias("MED_COINS_AMT"),
    F.col("MNTL_HLTH_COINS_AMT").alias("MNTL_HLTH_COINS_AMT"),
    F.col("OOP_AMT").alias("OOP_AMT"),
    F.when(F.col("PD_AMT").isNull(), F.lit(0.00)).otherwise(F.col("PD_AMT")).alias("PD_AMT"),
    F.col("SANC_AMT").alias("SANC_AMT"),
    F.col("SAV_AMT").alias("SAV_AMT"),
    F.col("SUBTR_AMT").alias("SUBTR_AMT"),
    F.col("COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
    F.col("COB_CAR_RSN_TX").alias("COB_CAR_RSN_TX")
)

df_collector = df_defaultUNK.unionByName(df_defaultNA).unionByName(df_fkey)

df_final = df_collector.select(
    "CLM_COB_LN_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_COB_TYP_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_SK",
    "CLM_LN_COB_CAR_PRORTN_CD_SK",
    "CLM_LN_COB_LIAB_TYP_CD_SK",
    "COB_CAR_ADJ_AMT",
    "ALW_AMT",
    "APLD_AMT",
    "COPAY_AMT",
    "DEDCT_AMT",
    "DSALW_AMT",
    "MED_COINS_AMT",
    "MNTL_HLTH_COINS_AMT",
    "OOP_AMT",
    "PD_AMT",
    "SANC_AMT",
    "SAV_AMT",
    "SUBTR_AMT",
    "COB_CAR_RSN_CD_TX",
    "COB_CAR_RSN_TX"
).withColumn(
    "CLM_ID", F.rpad(F.col("CLM_ID"), <...>, " ")
).withColumn(
    "COB_CAR_RSN_CD_TX", F.rpad(F.col("COB_CAR_RSN_CD_TX"), <...>, " ")
).withColumn(
    "COB_CAR_RSN_TX", F.rpad(F.col("COB_CAR_RSN_TX"), <...>, " ")
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_LN_COB.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)