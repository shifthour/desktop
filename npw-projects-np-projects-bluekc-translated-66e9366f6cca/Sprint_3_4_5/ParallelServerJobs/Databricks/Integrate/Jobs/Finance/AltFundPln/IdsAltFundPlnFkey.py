# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 09:39:22 Batch  14638_34767 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/27/07 09:46:56 Batch  14606_35219 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/07/07 14:46:06 Batch  14556_53195 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_1 11/07/07 14:33:10 Batch  14556_52396 INIT bckcett testIDScur dsadm bls for rt
# MAGIC ^1_1 10/17/07 13:27:15 Batch  14535_48445 PROMOTE bckcett testIDScur u06640 Ralph
# MAGIC ^1_1 10/17/07 13:17:55 Batch  14535_47880 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC 
# MAGIC 
# MAGIC ****************************************************************************************************************************************************************************
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Foriegn keying process
# MAGIC 
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Naren Garapaty     09/20/2007                Initial program                                                                                     3259                       devlIDS30                Steph Goddard           09/27/2007

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC Assign all foreign keys and create default rows for UNK and NA.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, rpad, row_number
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile", "IdsAltFundPlnPkey.AltFundPlnTmp.dat")
Logging = get_widget_value("Logging", "N")

schema_IdsAltFundPln = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("ALT_FUND_PLN_SK", IntegerType(), False),
    StructField("ALT_FUND_PLN_ROW_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("ALT_FUND", IntegerType(), False),
    StructField("CLS_PLN", IntegerType(), False),
    StructField("GRP", IntegerType(), False),
    StructField("SUBGRP", IntegerType(), False),
    StructField("ALT_FUND_CLS_PROD_CAT_CD", IntegerType(), False),
    StructField("ALT_FUND_CNTR_PERD_NO", IntegerType(), False),
    StructField("ALT_FUND_UNIQ_KEY", IntegerType(), False),
    StructField("SGSG_ID", StringType(), False),
    StructField("GRGR_ID", StringType(), False)
])

df_IdsAltFundPln = (
    spark.read
    .option("header", False)
    .option("quote", '"')
    .schema(schema_IdsAltFundPln)
    .csv(f"{adls_path}/key/{InFile}")
)

df_IdsAltFundPln = df_IdsAltFundPln.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "ALT_FUND_PLN_SK",
    "ALT_FUND_PLN_ROW_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ALT_FUND",
    "CLS_PLN",
    "GRP",
    "SUBGRP",
    "ALT_FUND_CLS_PROD_CAT_CD",
    "ALT_FUND_CNTR_PERD_NO",
    "ALT_FUND_UNIQ_KEY",
    "SGSG_ID",
    "GRGR_ID"
)

df_PurgeTrn = (
    df_IdsAltFundPln
    .withColumn("SrcSysCdSk", GetFkeyCodes(lit("IDS"), col("ALT_FUND_PLN_SK"), lit("SOURCE SYSTEM"), col("SRC_SYS_CD"), Logging))
    .withColumn("svAltFundSk", GetFkeyAltFund(col("SRC_SYS_CD"), col("ALT_FUND_PLN_SK"), col("ALT_FUND"), Logging))
    .withColumn("svClsPlnSk", GetFkeyClsPln(col("SRC_SYS_CD"), col("ALT_FUND_PLN_SK"), col("CLS_PLN"), Logging))
    .withColumn("svGrpSk", GetFkeyGrp(col("SRC_SYS_CD"), col("ALT_FUND_PLN_SK"), col("GRGR_ID"), Logging))
    .withColumn("svSubGrpSk", GetFkeySubgrp(col("SRC_SYS_CD"), col("ALT_FUND_PLN_SK"), col("GRGR_ID"), col("SGSG_ID"), Logging))
    .withColumn("svProdCatCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("ALT_FUND_PLN_SK"), lit("CLASS PLAN PRODUCT CATEGORY"), col("ALT_FUND_CLS_PROD_CAT_CD"), Logging))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("ALT_FUND_PLN_SK")))
)

windowSpec = Window.orderBy(lit(1))
df_temp = df_PurgeTrn.withColumn("rn", row_number().over(windowSpec))

df_Fkey = (
    df_PurgeTrn
    .filter((col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y")))
    .select(
        col("ALT_FUND_PLN_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("ALT_FUND_PLN_ROW_ID"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svAltFundSk").alias("ALT_FUND_SK"),
        col("svClsPlnSk").alias("CLS_PLN_SK"),
        col("svGrpSk").alias("GRP_SK"),
        col("svSubGrpSk").alias("SUBGRP_SK"),
        col("svProdCatCdSk").alias("ALT_FUND_CLS_PROD_CAT_CD_SK"),
        col("ALT_FUND_CNTR_PERD_NO"),
        col("ALT_FUND_UNIQ_KEY")
    )
)

df_lnkRecycle = (
    df_PurgeTrn
    .filter(col("ErrCount") > lit(0))
    .select(
        GetRecycleKey(col("ALT_FUND_PLN_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("ALT_FUND_PLN_SK").alias("ALT_FUND_PLN_SK"),
        col("ALT_FUND_PLN_ROW_ID").alias("ALT_FUND_PLN_ROW_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("ALT_FUND").alias("ALT_FUND_SK"),
        col("CLS_PLN").alias("CLS_PLN_SK"),
        col("GRP").alias("GRP_SK"),
        col("SUBGRP").alias("SUBGRP_SK"),
        col("ALT_FUND_CLS_PROD_CAT_CD").alias("ALT_FUND_CLS_PROD_CAT_CD_SK"),
        col("ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
        col("ALT_FUND_UNIQ_KEY").alias("ALT_FUND_UNIQ_KEY")
    )
)

df_DefaultUNK = (
    df_temp
    .filter(col("rn") == lit(1))
    .select(
        lit(0).alias("ALT_FUND_PLN_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("ALT_FUND_PLN_ROW_ID"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("ALT_FUND_SK"),
        lit(0).alias("CLS_PLN_SK"),
        lit(0).alias("GRP_SK"),
        lit(0).alias("SUBGRP_SK"),
        lit(0).alias("ALT_FUND_CLS_PROD_CAT_CD_SK"),
        lit(0).alias("ALT_FUND_CNTR_PERD_NO"),
        lit(0).alias("ALT_FUND_UNIQ_KEY")
    )
)

df_DefaultNA = (
    df_temp
    .filter(col("rn") == lit(1))
    .select(
        lit(1).alias("ALT_FUND_PLN_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("ALT_FUND_PLN_ROW_ID"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("ALT_FUND_SK"),
        lit(1).alias("CLS_PLN_SK"),
        lit(1).alias("GRP_SK"),
        lit(1).alias("SUBGRP_SK"),
        lit(1).alias("ALT_FUND_CLS_PROD_CAT_CD_SK"),
        lit(1).alias("ALT_FUND_CNTR_PERD_NO"),
        lit(1).alias("ALT_FUND_UNIQ_KEY")
    )
)

df_Collector = df_Fkey.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

df_lnkRecycle = df_lnkRecycle.withColumn("INSRT_UPDT_CD", rpad("INSRT_UPDT_CD", 10, " "))
df_lnkRecycle = df_lnkRecycle.withColumn("DISCARD_IN", rpad("DISCARD_IN", 1, " "))
df_lnkRecycle = df_lnkRecycle.withColumn("PASS_THRU_IN", rpad("PASS_THRU_IN", 1, " "))

write_files(
    df_lnkRecycle,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

write_files(
    df_Collector.select(
        "ALT_FUND_PLN_SK",
        "SRC_SYS_CD_SK",
        "ALT_FUND_PLN_ROW_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ALT_FUND_SK",
        "CLS_PLN_SK",
        "GRP_SK",
        "SUBGRP_SK",
        "ALT_FUND_CLS_PROD_CAT_CD_SK",
        "ALT_FUND_CNTR_PERD_NO",
        "ALT_FUND_UNIQ_KEY"
    ),
    f"{adls_path}/load/ALT_FUND_PLN.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)