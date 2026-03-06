# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Foriegn keying process
# MAGIC  
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer             Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Naren Garapaty     02/20/2007                Initial program                                                                                                             devlIDS30                          
# MAGIC 
# MAGIC Bhoomi Dasari       03/26/2007               Modification made to PRVCY_EXTRNL_ENTY_SK
# MAGIC                                                                 to PRVCY_ENTY_SK and removed DSCLSUR_ID                    CDS Sunset/3279   devlIDS30 
# MAGIC 
# MAGIC Bhoomi Dasari       03/18/2009               Updated with new rules for Mbr_SK                                             Prod supp/15         devlIDS                             Steph Goddard           04/01/2009
# MAGIC 
# MAGIC Manasa Andru       6/26/2013                Removed RunID from parameters as                                              TTR - 778            IntegrateCurDevl                 Kalyan Neelam            2013-07-02
# MAGIC                                                                  it is not being used anywhere in the job
# MAGIC 
# MAGIC Manasa Andru             7/23/2013        Updated Metadata of PRVCY_MBR_SRC_CD in the input file         TTR - 902           IntegrateCurDevl                   Kalyan Neelam            2013-07-31

# MAGIC Read common record format file from extract job.
# MAGIC Writing Sequential File to /load
# MAGIC Merge source data with default rows
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
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile", "FctsPrvcyDsclsurExtr.PrvcyDsclsur.dat")
Logging = get_widget_value("Logging", "Y")
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

schema_IdsPrvcyDsclurExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("PRVCY_DSCLSUR_SK", IntegerType(), False),
    StructField("PRVCY_MBR_UNIQ_KEY", IntegerType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("PRVCY_MBR_SRC_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("PRVCY_DSCLSUR_ENTY", IntegerType(), False),
    StructField("MBR", IntegerType(), False),
    StructField("PRVCY_EXTRNL_MBR", IntegerType(), False),
    StructField("PRVCY_RECPNT_EXTRNL_ENTY", IntegerType(), False),
    StructField("PRVCY_DSCLSUR_CAT_CD_1", StringType(), False),
    StructField("PRVCY_DSCLSUR_CAT_CD_2", StringType(), False),
    StructField("PRVCY_DSCLSUR_CAT_CD_3", StringType(), False),
    StructField("PRVCY_DSCLSUR_CAT_CD_4", StringType(), False),
    StructField("PRVCY_DSCLSUR_METH_CD", StringType(), False),
    StructField("PRVCY_DSCLSUR_RSN_CD", StringType(), False),
    StructField("CRT_DT_SK", StringType(), False),
    StructField("DSCLSUR_DT", StringType(), False),
    StructField("DSCLSUR_DESC", StringType(), False),
    StructField("SRC_SYS_LAST_UPDT_DT", StringType(), False),
    StructField("SRC_SYS_LAST_UPDT_USER", StringType(), False)
])

filePath_IdsPrvcyDsclurExtr = f"{adls_path}/key/{InFile}"

df_IdsPrvcyDsclurExtr = (
    spark.read.format("csv")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_IdsPrvcyDsclurExtr)
    .load(filePath_IdsPrvcyDsclurExtr)
)

windowSpec = Window.orderBy(lit(1))

df_Transformer = (
    df_IdsPrvcyDsclurExtr
    .withColumn("SrcSysCdSk", GetFkeyCodes("IDS", col("PRVCY_DSCLSUR_SK"), "SOURCE SYSTEM", col("SRC_SYS_CD"), Logging))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svPrvcyDsclsurEntySk", GetFkeyPrvcyDsclsurEnty(col("SRC_SYS_CD"), col("PRVCY_DSCLSUR_SK"), col("PRVCY_DSCLSUR_ENTY"), Logging))
    .withColumn("svPrvcyMbrSrcCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("PRVCY_DSCLSUR_SK"), "PRIVACY MEMBER SOURCE", col("PRVCY_MBR_SRC_CD"), Logging))
    .withColumn("svMbrSk", GetFkeyMbr(col("SRC_SYS_CD"), col("PRVCY_DSCLSUR_SK"), col("MBR"), Logging))
    .withColumn("svPrvcyExtrnlMbrsK", GetFkeyPrvcyExtrnlMbr(col("SRC_SYS_CD"), col("PRVCY_DSCLSUR_SK"), col("PRVCY_EXTRNL_MBR"), Logging))
    .withColumn("svRecpntExtrnlEntySk", GetFkeyPrvcyExtrnlEnty(col("SRC_SYS_CD"), col("PRVCY_DSCLSUR_SK"), col("PRVCY_RECPNT_EXTRNL_ENTY"), Logging))
    .withColumn("svPrvcyDsclsurCatCd1Sk", GetFkeyCodes(col("SRC_SYS_CD"), col("PRVCY_DSCLSUR_SK"), "DISCLOSURE CATEGORY", col("PRVCY_DSCLSUR_CAT_CD_1"), Logging))
    .withColumn("svPrvcyDsclsurCatCd2Sk", GetFkeyCodes(col("SRC_SYS_CD"), col("PRVCY_DSCLSUR_SK"), "DISCLOSURE CATEGORY", col("PRVCY_DSCLSUR_CAT_CD_2"), Logging))
    .withColumn("svPrvcyDsclsurCatCd3Sk", GetFkeyCodes(col("SRC_SYS_CD"), col("PRVCY_DSCLSUR_SK"), "DISCLOSURE CATEGORY", col("PRVCY_DSCLSUR_CAT_CD_3"), Logging))
    .withColumn("svPrvcyDsclsurCatCd4Sk", GetFkeyCodes(col("SRC_SYS_CD"), col("PRVCY_DSCLSUR_SK"), "DISCLOSURE CATEGORY", col("PRVCY_DSCLSUR_CAT_CD_4"), Logging))
    .withColumn("svPrvcyDsclsurMethCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("PRVCY_DSCLSUR_SK"), "DISCLOSURE METHOD", col("PRVCY_DSCLSUR_METH_CD"), Logging))
    .withColumn("svPrvcyDsclsurRsnCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("PRVCY_DSCLSUR_SK"), "DISCLOSURE REASON", col("PRVCY_DSCLSUR_RSN_CD"), Logging))
    .withColumn("svSrcSysLastUpdtDtSk", GetFkeyDate("IDS", col("PRVCY_DSCLSUR_SK"), col("SRC_SYS_LAST_UPDT_DT"), Logging))
    .withColumn("svSrcSysLastUpdtUserSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("PRVCY_DSCLSUR_SK"), col("SRC_SYS_LAST_UPDT_USER"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("PRVCY_DSCLSUR_SK")))
    .withColumn("_inrownum", row_number().over(windowSpec))
)

df_Fkey = (
    df_Transformer
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("PRVCY_DSCLSUR_SK").alias("PRVCY_DSCLSUR_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        col("SEQ_NO").alias("SEQ_NO"),
        col("svPrvcyMbrSrcCdSk").alias("PRVCY_MBR_SRC_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svMbrSk").alias("MBR_SK"),
        col("svPrvcyDsclsurEntySk").alias("PRVCY_DSCLSUR_ENTY_SK"),
        col("svPrvcyExtrnlMbrsK").alias("PRVCY_EXTRNL_MBR_SK"),
        col("svRecpntExtrnlEntySk").alias("PRVCY_RECPNT_EXTRNL_ENTY_SK"),
        col("svPrvcyDsclsurCatCd1Sk").alias("PRVCY_DSCLSUR_CAT_CD_1_SK"),
        col("svPrvcyDsclsurCatCd2Sk").alias("PRVCY_DSCLSUR_CAT_CD_2_SK"),
        col("svPrvcyDsclsurCatCd3Sk").alias("PRVCY_DSCLSUR_CAT_CD_3_SK"),
        col("svPrvcyDsclsurCatCd4Sk").alias("PRVCY_DSCLSUR_CAT_CD_4_SK"),
        col("svPrvcyDsclsurMethCdSk").alias("PRVCY_DSCLSUR_METH_CD_SK"),
        col("svPrvcyDsclsurRsnCdSk").alias("PRVCY_DSCLSUR_RSN_CD_SK"),
        rpad(col("CRT_DT_SK"), 10, " ").alias("CRT_DT_SK"),
        rpad(col("DSCLSUR_DT"), 10, " ").alias("DSCLSUR_DT_SK"),
        rpad(col("DSCLSUR_DESC"), 255, " ").alias("DSCLSUR_DESC"),
        rpad(col("svSrcSysLastUpdtDtSk"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        col("svSrcSysLastUpdtUserSk").alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

df_Recycle = (
    df_Transformer
    .filter(col("ErrCount") > 0)
    .select(
        col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        rpad(col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
        rpad(col("PRI_KEY_STRING"), 255, " ").alias("PRI_KEY_STRING"),
        col("PRVCY_DSCLSUR_SK").alias("PRVCY_DSCLSUR_SK"),
        col("PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        col("SEQ_NO").alias("SEQ_NO"),
        rpad(col("PRVCY_MBR_SRC_CD"), 1, " ").alias("PRVCY_MBR_SRC_CD"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("PRVCY_DSCLSUR_ENTY").alias("PRVCY_DSCLSUR_ENTY"),
        col("MBR").alias("MBR"),
        col("PRVCY_EXTRNL_MBR").alias("PRVCY_EXTRNL_MBR"),
        col("PRVCY_RECPNT_EXTRNL_ENTY").alias("PRVCY_RECPNT_EXTRNL_ENTY"),
        rpad(col("PRVCY_DSCLSUR_CAT_CD_1"), 10, " ").alias("PRVCY_DSCLSUR_CAT_CD_1"),
        rpad(col("PRVCY_DSCLSUR_CAT_CD_2"), 10, " ").alias("PRVCY_DSCLSUR_CAT_CD_2"),
        rpad(col("PRVCY_DSCLSUR_CAT_CD_3"), 10, " ").alias("PRVCY_DSCLSUR_CAT_CD_3"),
        rpad(col("PRVCY_DSCLSUR_CAT_CD_4"), 10, " ").alias("PRVCY_DSCLSUR_CAT_CD_4"),
        rpad(col("PRVCY_DSCLSUR_METH_CD"), 10, " ").alias("PRVCY_DSCLSUR_METH_CD"),
        rpad(col("PRVCY_DSCLSUR_RSN_CD"), 10, " ").alias("PRVCY_DSCLSUR_RSN_CD"),
        rpad(col("CRT_DT_SK"), 10, " ").alias("CRT_DT_SK"),
        rpad(col("DSCLSUR_DT"), 10, " ").alias("DSCLSUR_DT_SK"),
        rpad(col("DSCLSUR_DESC"), 255, " ").alias("DSCLSUR_DESC"),
        rpad(col("SRC_SYS_LAST_UPDT_DT"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT"),
        rpad(col("SRC_SYS_LAST_UPDT_USER"), 255, " ").alias("SRC_SYS_LAST_UPDT_USER")
    )
)

write_files(
    df_Recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_DefaultUNK = (
    df_Transformer
    .filter(col("_inrownum") == 1)
    .select(
        lit(0).alias("PRVCY_DSCLSUR_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit(0).alias("PRVCY_MBR_UNIQ_KEY"),
        lit(0).alias("SEQ_NO"),
        lit(0).alias("PRVCY_MBR_SRC_CD_SK"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("MBR_SK"),
        lit(0).alias("PRVCY_DSCLSUR_ENTY_SK"),
        lit(0).alias("PRVCY_EXTRNL_MBR_SK"),
        lit(0).alias("PRVCY_RECPNT_EXTRNL_ENTY_SK"),
        lit(0).alias("PRVCY_DSCLSUR_CAT_CD_1_SK"),
        lit(0).alias("PRVCY_DSCLSUR_CAT_CD_2_SK"),
        lit(0).alias("PRVCY_DSCLSUR_CAT_CD_3_SK"),
        lit(0).alias("PRVCY_DSCLSUR_CAT_CD_4_SK"),
        lit(0).alias("PRVCY_DSCLSUR_METH_CD_SK"),
        lit(0).alias("PRVCY_DSCLSUR_RSN_CD_SK"),
        rpad(lit("UNK"), 10, " ").alias("CRT_DT_SK"),
        rpad(lit("UNK"), 10, " ").alias("DSCLSUR_DT_SK"),
        rpad(lit("UNK"), 255, " ").alias("DSCLSUR_DESC"),
        rpad(lit("UNK"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        lit(0).alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

df_DefaultNA = (
    df_Transformer
    .filter(col("_inrownum") == 1)
    .select(
        lit(1).alias("PRVCY_DSCLSUR_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit(1).alias("PRVCY_MBR_UNIQ_KEY"),
        lit(1).alias("SEQ_NO"),
        lit(1).alias("PRVCY_MBR_SRC_CD_SK"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("MBR_SK"),
        lit(1).alias("PRVCY_DSCLSUR_ENTY_SK"),
        lit(1).alias("PRVCY_EXTRNL_MBR_SK"),
        lit(1).alias("PRVCY_RECPNT_EXTRNL_ENTY_SK"),
        lit(1).alias("PRVCY_DSCLSUR_CAT_CD_1_SK"),
        lit(1).alias("PRVCY_DSCLSUR_CAT_CD_2_SK"),
        lit(1).alias("PRVCY_DSCLSUR_CAT_CD_3_SK"),
        lit(1).alias("PRVCY_DSCLSUR_CAT_CD_4_SK"),
        lit(1).alias("PRVCY_DSCLSUR_METH_CD_SK"),
        lit(1).alias("PRVCY_DSCLSUR_RSN_CD_SK"),
        rpad(lit("NA"), 10, " ").alias("CRT_DT_SK"),
        rpad(lit("NA"), 10, " ").alias("DSCLSUR_DT_SK"),
        rpad(lit("NA"), 255, " ").alias("DSCLSUR_DESC"),
        rpad(lit("NA"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        lit(1).alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

df_Collector = df_Fkey.union(df_DefaultUNK).union(df_DefaultNA)

df_Final = df_Collector.select(
    col("PRVCY_DSCLSUR_SK"),
    col("SRC_SYS_CD_SK"),
    col("PRVCY_MBR_UNIQ_KEY"),
    col("SEQ_NO"),
    col("PRVCY_MBR_SRC_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_SK"),
    col("PRVCY_DSCLSUR_ENTY_SK"),
    col("PRVCY_EXTRNL_MBR_SK"),
    col("PRVCY_RECPNT_EXTRNL_ENTY_SK"),
    col("PRVCY_DSCLSUR_CAT_CD_1_SK"),
    col("PRVCY_DSCLSUR_CAT_CD_2_SK"),
    col("PRVCY_DSCLSUR_CAT_CD_3_SK"),
    col("PRVCY_DSCLSUR_CAT_CD_4_SK"),
    col("PRVCY_DSCLSUR_METH_CD_SK"),
    col("PRVCY_DSCLSUR_RSN_CD_SK"),
    col("CRT_DT_SK"),
    col("DSCLSUR_DT_SK"),
    col("DSCLSUR_DESC"),
    col("SRC_SYS_LAST_UPDT_DT_SK"),
    col("SRC_SYS_LAST_UPDT_USER_SK")
)

write_files(
    df_Final,
    f"{adls_path}/load/PRVCY_DSCLSUR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)