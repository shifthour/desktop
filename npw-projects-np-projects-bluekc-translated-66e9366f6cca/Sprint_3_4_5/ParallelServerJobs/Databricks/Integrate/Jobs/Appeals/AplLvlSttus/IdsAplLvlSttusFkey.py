# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2007 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     FctsAplLoadSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC              Previous Run Successful:    
# MAGIC              Previous Run Aborted:         Restart, no other step neccessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                     Project/                                                                                                                                         Code                             Date
# MAGIC Developer        Date                    Altiris #                                             Change Description                                                              Reviewer                       Reviewed
# MAGIC ----------------------  -------------------       -------------------                                       ------------------------------------------------------------------------------------         ----------------------                 -------------------   
# MAGIC Hugh Sisson    07/24/2007       3028                                                 Original program                                                                   Steph Goddard                9/6/07
# MAGIC 
# MAGIC Ravi Singh      2018-10- 04      IntegrateDev2/ MTM-5841                Updated stage variables for 3rd party Vendor	        Abhiram Dasarathy	2018-10-30	
# MAGIC                                                                                                             Evicore(MEDSLTNS) data to load to IDS
# MAGIC 
# MAGIC Ravi Singh      2018-10- 31      IntegrateDev2/ MTM-5841                Updated stage variables for 3rd party Vendor	         Kalyan Neelam                2018-11-12	
# MAGIC                                                                                                           TELLIGEN data to load to IDS
# MAGIC 
# MAGIC Ravi Singh     2018-11- 28     IntegrateDev2/.MTM-5841                  Updated stage vairable and validated code for 3rd party Vendor                  Kalyan Neelam             2018-12-10   
# MAGIC                                                                                                            New Direction data to load to IDS

# MAGIC Set all foreign surrogate keys
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
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","X")
InFile = get_widget_value("InFile","")

schema_IdsAplLvlSttusExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(10,0), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("APL_LVL_STTUS_SK", IntegerType(), nullable=False),
    StructField("APL_ID", StringType(), nullable=False),
    StructField("APL_LVL_SEQ_NO", IntegerType(), nullable=False),
    StructField("SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("APL_LVL_SK", IntegerType(), nullable=False),
    StructField("APST_USID", StringType(), nullable=False),
    StructField("APST_USID_ROUTE", StringType(), nullable=False),
    StructField("APL_LVL_STTUS_CD", StringType(), nullable=False),
    StructField("APL_LVL_STTUS_RSN_CD", StringType(), nullable=False),
    StructField("STTUS_DTM", TimestampType(), nullable=False)
])

df_IdsAplLvlSttusExtr = (
    spark.read
    .option("sep", ",")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_IdsAplLvlSttusExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_sv = (
    df_IdsAplLvlSttusExtr
    .withColumn(
        "SrcSysCdSk",
        GetFkeyCodes(
            F.lit("IDS"),
            F.col("APL_LVL_SK"),
            F.lit("SOURCE SYSTEM"),
            F.col("SRC_SYS_CD"),
            F.lit(Logging)
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn(
        "svAplLvlSk",
        GetFkeyAplLvl(
            F.col("SRC_SYS_CD"),
            F.col("APL_LVL_STTUS_SK"),
            F.col("APL_ID"),
            F.col("APL_LVL_SEQ_NO"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svRteUsrSk",
        GetFkeyAppUsr(
            F.col("SRC_SYS_CD"),
            F.col("APL_LVL_STTUS_SK"),
            F.col("APST_USID_ROUTE"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svSttusUsrSk",
        GetFkeyAppUsr(
            F.col("SRC_SYS_CD"),
            F.col("APL_LVL_STTUS_SK"),
            F.col("APST_USID"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svAplLvlSttusCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("APL_LVL_STTUS_SK"),
            F.lit("APPEAL STATUS"),
            F.col("APL_LVL_STTUS_CD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svAplLvlSttsRsnCdSk",
        F.when(
            (F.col("SRC_SYS_CD") == "TELLIGEN") | (F.col("SRC_SYS_CD") == "NDBH"),
            F.col("APL_LVL_STTUS_RSN_CD")
        )
        .otherwise(
            GetFkeyCodes(
                F.col("SRC_SYS_CD"),
                F.col("APL_LVL_STTUS_SK"),
                F.lit("APPEAL LEVEL STATUS REASON"),
                F.col("APL_LVL_STTUS_RSN_CD"),
                F.lit(Logging)
            )
        )
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("APL_LVL_SK")))
)

df_Fkey = df_sv.filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
df_Fkey_select = df_Fkey.select(
    F.col("APL_LVL_STTUS_SK").alias("APL_LVL_STTUS_SK"),
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("APL_ID").alias("APL_ID"),
    F.col("APL_LVL_SEQ_NO").alias("APL_LVL_SEQ_NO"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svAplLvlSk").alias("APL_LVL_SK"),
    F.col("svRteUsrSk").alias("RTE_USER_SK"),
    F.col("svSttusUsrSk").alias("STTUS_USER_SK"),
    F.col("svAplLvlSttusCdSk").alias("APL_LVL_STTUS_CD_SK"),
    F.col("svAplLvlSttsRsnCdSk").alias("APL_LVL_STTUS_RSN_CD_SK"),
    F.col("STTUS_DTM").alias("STTUS_DTM")
)

df_Recycle = df_sv.filter(F.col("ErrCount") > 0)
df_Recycle_select = df_Recycle.select(
    GetRecycleKey(F.col("APL_LVL_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("APL_LVL_STTUS_SK").alias("APL_LVL_STTUS_SK"),
    F.col("APL_ID").alias("APL_ID"),
    F.col("APL_LVL_SEQ_NO").alias("APL_LVL_SEQ_NO"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APL_LVL_SK").alias("APL_LVL_SK"),
    F.col("APST_USID").alias("APST_USID"),
    F.col("APST_USID_ROUTE").alias("APST_USID_ROUTE"),
    F.col("APL_LVL_STTUS_CD").alias("APL_LVL_STTUS_CD"),
    F.col("APL_LVL_STTUS_RSN_CD").alias("APL_LVL_STTUS_RSN_CD"),
    F.col("STTUS_DTM").alias("STTUS_DTM")
)

windowSpec = Window.orderBy(F.lit(1))

df_DefaultUNK_base = df_sv.withColumn("row_number", F.row_number().over(windowSpec)).filter(F.col("row_number") == 1)
df_DefaultUNK = df_DefaultUNK_base.select(
    F.lit(0).alias("APL_LVL_STTUS_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("APL_ID"),
    F.lit(0).alias("APL_LVL_SEQ_NO"),
    F.lit(0).alias("SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("APL_LVL_SK"),
    F.lit(0).alias("RTE_USER_SK"),
    F.lit(0).alias("STTUS_USER_SK"),
    F.lit(0).alias("APL_LVL_STTUS_CD_SK"),
    F.lit(0).alias("APL_LVL_STTUS_RSN_CD_SK"),
    F.lit("2199-12-31 23:59:59.999999").alias("STTUS_DTM")
)

df_DefaultNA_base = df_sv.withColumn("row_number", F.row_number().over(windowSpec)).filter(F.col("row_number") == 1)
df_DefaultNA = df_DefaultNA_base.select(
    F.lit(1).alias("APL_LVL_STTUS_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("APL_ID"),
    F.lit(1).alias("APL_LVL_SEQ_NO"),
    F.lit(1).alias("SEQ_NO"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("APL_LVL_SK"),
    F.lit(1).alias("RTE_USER_SK"),
    F.lit(1).alias("STTUS_USER_SK"),
    F.lit(1).alias("APL_LVL_STTUS_CD_SK"),
    F.lit(1).alias("APL_LVL_STTUS_RSN_CD_SK"),
    F.lit("1753-01-01 00:00:00.000000").alias("STTUS_DTM")
)

df_collector = (
    df_Fkey_select
    .unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
)

df_recycle_out = (
    df_Recycle_select
    .withColumn("INSRT_UPDT_CD", rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("APST_USID", rpad(F.col("APST_USID"), 10, " "))
    .withColumn("APST_USID_ROUTE", rpad(F.col("APST_USID_ROUTE"), 10, " "))
    .withColumn("APL_LVL_STTUS_CD", rpad(F.col("APL_LVL_STTUS_CD"), 2, " "))
    .withColumn("APL_LVL_STTUS_RSN_CD", rpad(F.col("APL_LVL_STTUS_RSN_CD"), 4, " "))
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
        "APL_LVL_STTUS_SK",
        "APL_ID",
        "APL_LVL_SEQ_NO",
        "SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "APL_LVL_SK",
        "APST_USID",
        "APST_USID_ROUTE",
        "APL_LVL_STTUS_CD",
        "APL_LVL_STTUS_RSN_CD",
        "STTUS_DTM"
    )
)

write_files(
    df_recycle_out,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_final = df_collector.select(
    "APL_LVL_STTUS_SK",
    "SRC_SYS_CD_SK",
    "APL_ID",
    "APL_LVL_SEQ_NO",
    "SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "APL_LVL_SK",
    "RTE_USER_SK",
    "STTUS_USER_SK",
    "APL_LVL_STTUS_CD_SK",
    "APL_LVL_STTUS_RSN_CD_SK",
    "STTUS_DTM"
)

write_files(
    df_final,
    f"{adls_path}/load/APL_LVL_STTUS.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)