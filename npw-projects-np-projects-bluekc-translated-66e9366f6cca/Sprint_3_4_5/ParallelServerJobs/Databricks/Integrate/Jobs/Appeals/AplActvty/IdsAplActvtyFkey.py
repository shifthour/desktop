# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Takes the primary key file and applies the foreign keys.   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari    07/11/2007                Initial program                                                                                   3028                  devlIDS30                          Steph Goddard           8/23/07   
# MAGIC 
# MAGIC Ravi Singh          2018-10- 04                Updated stage variables for 3rd party Vendor                                   MTM-5841       IntegrateDev1                     Abhiram Dasarathy      2018-10-30
# MAGIC                                                               Evicore(MEDSLTNS) data to load to IDS
# MAGIC 
# MAGIC Ravi Singh          2018-10- 25                Updated stage variables for 3rd party Vendor                                   MTM-5841       IntegrateDev2                            Kalyan Neelam         2018-11-12
# MAGIC                                                               TELLIGEN data to load to IDS
# MAGIC 
# MAGIC Ravi Singh          2018-11- 30                Updated stage variables for 3rd party Vendor                                   MTM-5841       IntegrateDev2                        Kalyan Neelam             2018-12-10   
# MAGIC                                                               New Direction data to load to IDS

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql.functions import col, when, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging", "X")
InFile = get_widget_value("InFile", "")

schema_IdsAplActvtyExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("APL_ACTVTY_SK", IntegerType(), False),
    StructField("APL_ID", StringType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("APL_SK", IntegerType(), False),
    StructField("APL_RVWR_SK", IntegerType(), False),
    StructField("CRT_USER_SK", IntegerType(), False),
    StructField("LAST_UPDT_USER_SK", IntegerType(), False),
    StructField("APL_ACTVTY_METH_CD_SK", IntegerType(), False),
    StructField("APL_ACTVTY_TYP_CD_SK", IntegerType(), False),
    StructField("ACTVTY_DT_SK", StringType(), False),
    StructField("CRT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("APL_LVL_SEQ_NO", IntegerType(), False),
    StructField("ACTVTY_SUM", StringType(), False),
    StructField("APL_RVWR_ID", StringType(), False)
])

df_IdsAplActvtyExtr = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_IdsAplActvtyExtr)
    .load(f"{adls_path}/key/{InFile}")
)

dfForeignKey = (
    df_IdsAplActvtyExtr
    .withColumn("SrcSysCdSk", GetFkeyCodes(lit("IDS"), col("APL_ACTVTY_SK"), lit("SOURCE SYSTEM"), col("SRC_SYS_CD"), lit(Logging)))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svAplSk", GetFkeyApl(col("SRC_SYS_CD"), col("APL_ACTVTY_SK"), col("APL_SK"), lit(Logging)))
    .withColumn(
        "svCrtUsrSk",
        when(
            col("SRC_SYS_CD").isin("MEDSLTNS", "TELLIGEN", "NDBH"),
            col("CRT_USER_SK")
        ).otherwise(GetFkeyAppUsr(col("SRC_SYS_CD"), col("APL_ACTVTY_SK"), col("CRT_USER_SK"), lit(Logging)))
    )
    .withColumn(
        "svLstUpdtUsrSk",
        when(
            col("SRC_SYS_CD").isin("MEDSLTNS", "TELLIGEN", "NDBH"),
            col("LAST_UPDT_USER_SK")
        ).otherwise(GetFkeyAppUsr(col("SRC_SYS_CD"), col("APL_ACTVTY_SK"), col("LAST_UPDT_USER_SK"), lit(Logging)))
    )
    .withColumn("svAplActvtyMethCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("APL_ACTVTY_SK"), lit("APPEAL ACTIVITY METHOD"), col("APL_ACTVTY_METH_CD_SK"), lit(Logging)))
    .withColumn("svAplActvtyTypCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("APL_ACTVTY_SK"), lit("APPEAL ACTIVITY TYPE"), col("APL_ACTVTY_TYP_CD_SK"), lit(Logging)))
    .withColumn("svActvtyDtCdSk", GetFkeyDate(lit("IDS"), col("APL_ACTVTY_SK"), col("ACTVTY_DT_SK"), lit(Logging)))
    .withColumn(
        "svAplRvwrSk",
        when(
            col("SRC_SYS_CD").isin("MEDSLTNS", "NDBH"),
            col("APL_RVWR_SK")
        ).otherwise(GetFkeyAplRvwr(col("SRC_SYS_CD"), col("APL_ACTVTY_SK"), col("APL_RVWR_SK"), lit(Logging)))
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(col("APL_ACTVTY_SK")))
)

dfForeignKeyFkey = dfForeignKey.filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
dfForeignKeyFkeySel = (
    dfForeignKeyFkey
    .withColumn("ACTVTY_DT_SK", rpad(col("svActvtyDtCdSk"), 10, " "))
    .select(
        col("APL_ACTVTY_SK").alias("APL_ACTVTY_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("APL_ID").alias("APL_ID"),
        col("SEQ_NO").alias("SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svAplSk").alias("APL_SK"),
        col("svAplRvwrSk").alias("APL_RVWR_SK"),
        col("svCrtUsrSk").alias("CRT_USER_SK"),
        col("svLstUpdtUsrSk").alias("LAST_UPDT_USER_SK"),
        col("svAplActvtyMethCdSk").alias("APL_ACTVTY_METH_CD_SK"),
        col("svAplActvtyTypCdSk").alias("APL_ACTVTY_TYP_CD_SK"),
        col("ACTVTY_DT_SK").alias("ACTVTY_DT_SK"),
        col("CRT_DTM").alias("CRT_DTM"),
        col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        col("APL_LVL_SEQ_NO").alias("APL_LVL_SEQ_NO"),
        col("ACTVTY_SUM").alias("ACTVTY_SUM"),
        col("APL_RVWR_ID").alias("APL_RVWR_ID")
    )
)

dfForeignKeyRecycle = dfForeignKey.filter(col("ErrCount") > 0)
dfForeignKeyRecycleFinal = (
    dfForeignKeyRecycle
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(col("APL_ACTVTY_SK")))
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("ERR_CT", col("ErrCount"))
    .withColumn("RECYCLE_CT", col("RECYCLE_CT") + lit(1))
    .withColumn("ACTVTY_DT_SK", rpad(col("ACTVTY_DT_SK"), 10, " "))
    .select(
        col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ERR_CT").alias("ERR_CT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("APL_ACTVTY_SK").alias("APL_ACTVTY_SK"),
        col("APL_ID").alias("APL_ID"),
        col("SEQ_NO").alias("SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("APL_SK").alias("APL_SK"),
        col("APL_RVWR_SK").alias("APL_RVWR_SK"),
        col("CRT_USER_SK").alias("CRT_USER_SK"),
        col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
        col("APL_ACTVTY_METH_CD_SK").alias("APL_ACTVTY_METH_CD_SK"),
        col("APL_ACTVTY_TYP_CD_SK").alias("APL_ACTVTY_TYP_CD_SK"),
        col("ACTVTY_DT_SK").alias("ACTVTY_DT_SK"),
        col("CRT_DTM").alias("CRT_DTM"),
        col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        col("APL_LVL_SEQ_NO").alias("APL_LVL_SEQ_NO"),
        col("ACTVTY_SUM").alias("ACTVTY_SUM"),
        col("APL_RVWR_ID").alias("APL_RVWR_ID")
    )
)
write_files(
    dfForeignKeyRecycleFinal,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

dfForeignKeyDefaultUNK = dfForeignKey.limit(1)
dfForeignKeyDefaultUNKSel = (
    dfForeignKeyDefaultUNK
    .select(
        rpad(lit("0"), 1, " ").cast("int").alias("APL_ACTVTY_SK"),
        rpad(lit("0"), 1, " ").cast("int").alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("APL_ID"),
        rpad(lit("0"), 1, " ").cast("int").alias("SEQ_NO"),
        rpad(lit("0"), 1, " ").cast("int").alias("CRT_RUN_CYC_EXCTN_SK"),
        rpad(lit("0"), 1, " ").cast("int").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        rpad(lit("0"), 1, " ").cast("int").alias("APL_SK"),
        rpad(lit("0"), 1, " ").cast("int").alias("APL_RVWR_SK"),
        rpad(lit("0"), 1, " ").cast("int").alias("CRT_USER_SK"),
        rpad(lit("0"), 1, " ").cast("int").alias("LAST_UPDT_USER_SK"),
        rpad(lit("0"), 1, " ").cast("int").alias("APL_ACTVTY_METH_CD_SK"),
        rpad(lit("0"), 1, " ").cast("int").alias("APL_ACTVTY_TYP_CD_SK"),
        rpad(lit("UNK"), 10, " ").alias("ACTVTY_DT_SK"),
        lit("1753-01-01 00:00:00").cast("timestamp").alias("CRT_DTM"),
        lit("1753-01-01 00:00:00").cast("timestamp").alias("LAST_UPDT_DTM"),
        rpad(lit("0"), 1, " ").cast("int").alias("APL_LVL_SEQ_NO"),
        lit("UNK").alias("ACTVTY_SUM"),
        lit("UNK").alias("APL_RVWR_ID")
    )
)

dfForeignKeyDefaultNA = dfForeignKey.limit(1)
dfForeignKeyDefaultNASel = (
    dfForeignKeyDefaultNA
    .select(
        rpad(lit("1"), 1, " ").cast("int").alias("APL_ACTVTY_SK"),
        rpad(lit("1"), 1, " ").cast("int").alias("SRC_SYS_CD_SK"),
        lit("NA").alias("APL_ID"),
        rpad(lit("1"), 1, " ").cast("int").alias("SEQ_NO"),
        rpad(lit("1"), 1, " ").cast("int").alias("CRT_RUN_CYC_EXCTN_SK"),
        rpad(lit("1"), 1, " ").cast("int").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        rpad(lit("1"), 1, " ").cast("int").alias("APL_SK"),
        rpad(lit("1"), 1, " ").cast("int").alias("APL_RVWR_SK"),
        rpad(lit("1"), 1, " ").cast("int").alias("CRT_USER_SK"),
        rpad(lit("1"), 1, " ").cast("int").alias("LAST_UPDT_USER_SK"),
        rpad(lit("1"), 1, " ").cast("int").alias("APL_ACTVTY_METH_CD_SK"),
        rpad(lit("1"), 1, " ").cast("int").alias("APL_ACTVTY_TYP_CD_SK"),
        rpad(lit("NA"), 10, " ").alias("ACTVTY_DT_SK"),
        lit("1753-01-01 00:00:00").cast("timestamp").alias("CRT_DTM"),
        lit("1753-01-01 00:00:00").cast("timestamp").alias("LAST_UPDT_DTM"),
        rpad(lit("1"), 1, " ").cast("int").alias("APL_LVL_SEQ_NO"),
        lit("NA").alias("ACTVTY_SUM"),
        lit("NA").alias("APL_RVWR_ID")
    )
)

dfCollector = dfForeignKeyFkeySel.unionByName(dfForeignKeyDefaultUNKSel).unionByName(dfForeignKeyDefaultNASel)

dfCollectorFinal = dfCollector.select(
    col("APL_ACTVTY_SK"),
    col("SRC_SYS_CD_SK"),
    col("APL_ID"),
    col("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("APL_SK"),
    col("APL_RVWR_SK"),
    col("CRT_USER_SK"),
    col("LAST_UPDT_USER_SK"),
    col("APL_ACTVTY_METH_CD_SK"),
    col("APL_ACTVTY_TYP_CD_SK"),
    col("ACTVTY_DT_SK"),
    col("CRT_DTM"),
    col("LAST_UPDT_DTM"),
    col("APL_LVL_SEQ_NO"),
    col("ACTVTY_SUM"),
    col("APL_RVWR_ID")
)

write_files(
    dfCollectorFinal,
    f"{adls_path}/load/APL_ACTVTY.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)