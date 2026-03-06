# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Takes the primary key file and applies the foreign keys.   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Naren Garapaty    11/2006                    Initial program                                                                                                             devlIDS30            
# MAGIC Shanmugham A    2018-03-13               Updated to Uppercase for APL_RVWR_ID in ForeignKey Stage     TFS21145         IntegrateDev1                    Jaideep Mankala      03/13/2018

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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','X')
InFile = get_widget_value('InFile','FctsAplLvlRvwrExtr.AplLvlRvwr.dat.2007010105')

schema_IdsAplLvlRvwrExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(10,0), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("APL_LVL_RVWR_SK", IntegerType(), False),
    StructField("APL_ID", StringType(), False),
    StructField("APL_LVL_SEQ_NO", IntegerType(), False),
    StructField("APL_RVWR_ID", StringType(), False),
    StructField("EFF_DT", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("APL_LVL", IntegerType(), False),
    StructField("APL_RVW", IntegerType(), False),
    StructField("LAST_UPDT_USER", IntegerType(), False),
    StructField("APL_REP_LVL_RVWR_TERM_CD", IntegerType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("TERM_DT", StringType(), False),
    StructField("APL_RVWR_NM", StringType(), True)
])

df_IdsAplLvlRvwrExtr = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsAplLvlRvwrExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_foreignKey = (
    df_IdsAplLvlRvwrExtr
    .withColumn("SrcSysCdSk", GetFkeyCodes("IDS", col("APL_LVL_RVWR_SK"), "SOURCE SYSTEM", col("SRC_SYS_CD"), Logging))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svEffDtSk", GetFkeyDate("IDS", col("APL_LVL_RVWR_SK"), col("EFF_DT"), Logging))
    .withColumn("svAplLvlSk", GetFkeyAplLvl(col("SRC_SYS_CD"), col("APL_LVL_RVWR_SK"), col("APL_ID"), col("APL_LVL_SEQ_NO"), Logging))
    .withColumn("svUpcaseAplRvwr", UpCase(Trim(col("APL_RVWR_ID"))))
    .withColumn("svAplRvwrSk", GetFkeyAplRvwr(col("SRC_SYS_CD"), col("APL_LVL_RVWR_SK"), col("svUpcaseAplRvwr"), Logging))
    .withColumn("svLstUpdUsrSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("APL_LVL_RVWR_SK"), col("LAST_UPDT_USER"), Logging))
    .withColumn("svAplTermCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("APL_LVL_RVWR_SK"), "APPEAL REPRESENTATIVE LEVEL REVIEWER TERMINATION", col("APL_REP_LVL_RVWR_TERM_CD"), Logging))
    .withColumn("svTermDtSk", GetFkeyDate("IDS", col("APL_LVL_RVWR_SK"), col("TERM_DT"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("APL_LVL_RVWR_SK")))
)

df_fkey = (
    df_foreignKey
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("APL_LVL_RVWR_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("APL_ID"),
        col("APL_LVL_SEQ_NO"),
        col("APL_RVWR_ID"),
        col("svEffDtSk").alias("EFF_DT_SK"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svAplLvlSk").alias("APL_LVL_SK"),
        col("svAplRvwrSk").alias("APL_RVWR_SK"),
        col("svLstUpdUsrSk").alias("LAST_UPDT_USER_SK"),
        col("svAplTermCdSk").alias("APL_REP_LVL_RVWR_TERM_CD_SK"),
        col("LAST_UPDT_DTM"),
        col("svTermDtSk").alias("TERM_DT_SK"),
        col("APL_RVWR_NM")
    )
)

df_recycle = (
    df_foreignKey
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("APL_LVL_RVWR_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("APL_LVL_RVWR_SK").alias("APL_LVL_RVWR_SK"),
        col("APL_ID").alias("APL_ID"),
        col("APL_LVL_SEQ_NO").alias("APL_LVL_SEQ_NO"),
        col("APL_RVWR_ID").alias("APL_RVWR_ID"),
        col("EFF_DT").alias("EFF_DT_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("APL_LVL").alias("APL_LVL_SK"),
        col("APL_RVW").alias("APL_RVWR_SK"),
        col("LAST_UPDT_USER").alias("LAST_UPDT_USER_SK"),
        col("APL_REP_LVL_RVWR_TERM_CD").alias("APL_REP_LVL_RVWR_TERM_CD_SK"),
        col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        col("TERM_DT").alias("TERM_DT_SK"),
        col("APL_RVWR_NM").alias("APL_RVWR_NM")
    )
)

df_recycle_final = (
    df_recycle
    .select(
        col("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD"),
        col("DISCARD_IN"),
        col("PASS_THRU_IN"),
        col("FIRST_RECYC_DT"),
        col("ERR_CT"),
        col("RECYCLE_CT"),
        col("SRC_SYS_CD"),
        col("PRI_KEY_STRING"),
        col("APL_LVL_RVWR_SK"),
        col("APL_ID"),
        col("APL_LVL_SEQ_NO"),
        col("APL_RVWR_ID"),
        col("EFF_DT_SK"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("APL_LVL_SK"),
        col("APL_RVWR_SK"),
        col("LAST_UPDT_USER_SK"),
        col("APL_REP_LVL_RVWR_TERM_CD_SK"),
        col("LAST_UPDT_DTM"),
        col("TERM_DT_SK"),
        col("APL_RVWR_NM")
    )
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", rpad(col("TERM_DT_SK"), 10, " "))
)

write_files(
    df_recycle_final,
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

w = Window.orderBy(lit(1))
df_foreignKey_withRnum = df_foreignKey.withColumn("rownum", row_number().over(w))

df_defaultUNK = (
    df_foreignKey_withRnum
    .filter(col("rownum") == 1)
    .select(
        lit(0).alias("APL_LVL_RVWR_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("APL_ID"),
        lit(0).alias("APL_LVL_SEQ_NO"),
        lit("UNK").alias("APL_RVWR_ID"),
        lit("UNK").alias("EFF_DT_SK"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("APL_LVL_SK"),
        lit(0).alias("APL_RVWR_SK"),
        lit(0).alias("LAST_UPDT_USER_SK"),
        lit(0).alias("APL_REP_LVL_RVWR_TERM_CD_SK"),
        lit("1753-01-01 00:00:00").cast("timestamp").alias("LAST_UPDT_DTM"),
        lit("UNK").alias("TERM_DT_SK"),
        lit("UNK").alias("APL_RVWR_NM")
    )
)

df_defaultNA = (
    df_foreignKey_withRnum
    .filter(col("rownum") == 1)
    .select(
        lit(1).alias("APL_LVL_RVWR_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("APL_ID"),
        lit(1).alias("APL_LVL_SEQ_NO"),
        lit("NA").alias("APL_RVWR_ID"),
        lit("NA").alias("EFF_DT_SK"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("APL_LVL_SK"),
        lit(1).alias("APL_RVWR_SK"),
        lit(1).alias("LAST_UPDT_USER_SK"),
        lit(1).alias("APL_REP_LVL_RVWR_TERM_CD_SK"),
        lit("1753-01-01 00:00:00").cast("timestamp").alias("LAST_UPDT_DTM"),
        lit("NA").alias("TERM_DT_SK"),
        lit("NA").alias("APL_RVWR_NM")
    )
)

df_collector = (
    df_fkey
    .unionByName(df_defaultUNK)
    .unionByName(df_defaultNA)
)

df_collector_final = (
    df_collector
    .select(
        col("APL_LVL_RVWR_SK"),
        col("SRC_SYS_CD_SK"),
        col("APL_ID"),
        col("APL_LVL_SEQ_NO"),
        col("APL_RVWR_ID"),
        rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("APL_LVL_SK"),
        col("APL_RVWR_SK"),
        col("LAST_UPDT_USER_SK"),
        col("APL_REP_LVL_RVWR_TERM_CD_SK"),
        col("LAST_UPDT_DTM"),
        rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
        col("APL_RVWR_NM")
    )
)

write_files(
    df_collector_final,
    f"{adls_path}/load/APL_LVL_RVWR.dat",
    ",",
    "overwrite",
    False,
    True,
    "\"",
    None
)