# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006, 2021 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmLnDiagFkey
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  .../key/FctsClmLnDiagExtr.FctsClmLnDiag.uniq
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     ProvHsh    - this is both read from and written to.   
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC OUTPUTS:   .../load/CLM_LN_DIAG.#Source#.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------    
# MAGIC Tom Harrocks         04/12/2004                                    Originally Programmed  
# MAGIC Brent Leland           09/07/2004                                    Added default rows for UNK and NA
# MAGIC Suzanne Saylor      03/01/2006                                    Updated parameters, renamed links
# MAGIC Brent Leland           08/03/2006                                    Assigned last update run cycle from input to NA 
# MAGIC                                                                                       and UNK rows so delete process would not remove them.
# MAGIC Rick Henry             20120424           4850                    Added DIAG_CD_TYP_CD to DIAG_SK lookup                           NewDevl                      Sandrew                         2012-05-17
# MAGIC 
# MAGIC Kalyan Neelam       2014-12-17           5212                  Added If SRC_SYS_CD = 'BCBSA' Then 'BCA' Else SRC_SYS_CD    IntegrateCurDevl    Bhoomi Dasari             02/04/2015
# MAGIC                                                                                       in the stage variables and pass it to GetFkeyCodes because code sets are created under BCA for BCBSA
# MAGIC 
# MAGIC Sunitha Ganta         10-12-2020                                     Brought up to standards
# MAGIC 
# MAGIC Vikas Abbu           2021-03-01          RA                       Added Src Sys Cd values for EMR Procedure data                      IntegrateDev2               Jaideep Mankala        03/04/2021
# MAGIC Mrudula Kodali      2021-03-24          311337                Reverted EMR Src Sys Code  changes as we have added         IntegrateDev2               Hugh Sisson                2021-03-26
# MAGIC                                                                                      CDMA entries 
# MAGIC 
# MAGIC 
# MAGIC Mrudula Kodali   2021-04-21            332384             Updated DiagOrd for src_sys_cd DENTAQUEST and           IntegrateDev2                            Kalyan Neelam                2021-04-22
# MAGIC                                                                                ASH to use LUMERISWRHS

# MAGIC Writing Sequential File to /load
# MAGIC Merge source data with default rows
# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
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


SrcSysCd = get_widget_value("SrcSysCd","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","1")
InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","Y")

schema_ClmLnDiagExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CLM_LN_DIAG_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("CLM_LN_DIAG_ORDNL_CD", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("DIAG_CD", StringType(), False),
    StructField("DIAG_CD_TYP_CD", StringType(), False)
])

df_ClmLnDiagExtr = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_ClmLnDiagExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

dfForeignKey = (
    df_ClmLnDiagExtr
    .withColumn(
        "svCdMpngSrcSysCd",
        F.when(F.col("SRC_SYS_CD") == "BCBSA", F.lit("BCA"))
         .otherwise(
             F.when(
                 (F.col("SRC_SYS_CD") == "LUMERIS")
                 | (F.col("SRC_SYS_CD") == "DOMINION")
                 | (F.col("SRC_SYS_CD") == "MARC"),
                 F.lit("FACETS")
             ).otherwise(F.col("SRC_SYS_CD"))
         )
    )
    .withColumn(
        "ClmLnSk",
        GetFkeyClmLn(
            F.col("SRC_SYS_CD"),
            F.col("CLM_LN_DIAG_SK"),
            F.col("CLM_ID"),
            F.col("CLM_LN_SEQ_NO"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "DiagOrd",
        F.when(
            (F.col("SRC_SYS_CD") == "DENTAQUEST") | (F.col("SRC_SYS_CD") == "ASH"),
            GetFkeyCodes(
                F.lit("LUMERISWRHS"),
                F.col("CLM_LN_DIAG_SK"),
                F.lit("DIAGNOSIS ORDINAL"),
                F.col("CLM_LN_DIAG_ORDNL_CD"),
                F.lit(Logging)
            )
        ).otherwise(
            GetFkeyCodes(
                F.col("svCdMpngSrcSysCd"),
                F.col("CLM_LN_DIAG_SK"),
                F.lit("DIAGNOSIS ORDINAL"),
                F.col("CLM_LN_DIAG_ORDNL_CD"),
                F.lit(Logging)
            )
        )
    )
    .withColumn(
        "DiagCd",
        GetFkeyDiagCd(
            F.lit("FACETS"),
            F.col("CLM_LN_DIAG_SK"),
            F.col("DIAG_CD"),
            F.col("DIAG_CD_TYP_CD"),
            F.lit(Logging)
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CLM_LN_DIAG_SK")))
)

dfFkey1 = dfForeignKey.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == "Y")
).select(
    F.col("CLM_LN_DIAG_SK").alias("CLM_LN_DIAG_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("DiagOrd").alias("CLM_LN_DIAG_ORDNL_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ClmLnSk").alias("CLM_LN_SK"),
    F.col("DiagCd").alias("DIAG_CD_SK")
)

dfRecycle1 = dfForeignKey.filter(
    F.col("ErrCount") > 0
).select(
    F.expr("GetRecycleKey(CLM_LN_DIAG_SK)").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_LN_DIAG_SK").alias("CLM_LN_DIAG_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_DIAG_ORDNL_CD").alias("CLM_LN_DIAG_ORDNL_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DIAG_CD").alias("DIAG_CD"),
    F.col("DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD")
)

dfRecycle_Clms = dfForeignKey.filter(
    F.col("ErrCount") > 0
).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID")
)

windowSpec = Window.orderBy(F.lit(1))
dfForeignKeyIndexed = dfForeignKey.withColumn("_rownum", F.row_number().over(windowSpec))

dfDefaultUNK = dfForeignKeyIndexed.filter(
    F.col("_rownum") == 1
).select(
    F.lit(0).alias("CLM_LN_DIAG_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit(0).alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("CLM_LN_DIAG_ORDNL_CD_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_LN_SK"),
    F.lit(0).alias("DIAG_CD_SK")
)

dfDefaultNA = dfForeignKeyIndexed.filter(
    F.col("_rownum") == 1
).select(
    F.lit(1).alias("CLM_LN_DIAG_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("CLM_ID"),
    F.lit(1).alias("CLM_LN_SEQ_NO"),
    F.lit(1).alias("CLM_LN_DIAG_ORDNL_CD_SK"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CLM_LN_SK"),
    F.lit(1).alias("DIAG_CD_SK")
)

dfCollector = dfFkey1.unionByName(dfDefaultUNK).unionByName(dfDefaultNA)

dfCollector_final = dfCollector.withColumn(
    "CLM_ID", F.rpad(F.col("CLM_ID"), 18, " ")
).select(
    "CLM_LN_DIAG_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_DIAG_ORDNL_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_SK",
    "DIAG_CD_SK"
)

dfRecycle1_final = (
    dfRecycle1
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 50, " "))
    .withColumn("PRI_KEY_STRING", F.rpad(F.col("PRI_KEY_STRING"), 50, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
    .withColumn("DIAG_CD", F.rpad(F.col("DIAG_CD"), 50, " "))
    .withColumn("DIAG_CD_TYP_CD", F.rpad(F.col("DIAG_CD_TYP_CD"), 50, " "))
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
        "CLM_LN_DIAG_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_DIAG_ORDNL_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "DIAG_CD",
        "DIAG_CD_TYP_CD"
    )
)

write_files(
    dfRecycle1_final,
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_hf_claim_recycle_keys_final = (
    dfRecycle_Clms
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 50, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
    .select("SRC_SYS_CD","CLM_ID")
)

write_files(
    df_hf_claim_recycle_keys_final,
    f"{adls_path}/hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

write_files(
    dfCollector_final,
    f"{adls_path}/load/CLM_LN_DIAG.{SrcSysCd}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)