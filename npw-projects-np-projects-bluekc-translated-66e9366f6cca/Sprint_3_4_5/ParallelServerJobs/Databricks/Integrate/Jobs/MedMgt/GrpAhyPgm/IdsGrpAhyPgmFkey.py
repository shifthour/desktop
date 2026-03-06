# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2012 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  HlthFtnsGrpAhyPgmExtrSeq
# MAGIC 
# MAGIC PROCESSING:   Fkey job for IDS table GRP_AHY_PGM
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------   
# MAGIC Kalyan Neelam         2012-11-09              4830                        Initial Programming                                                                       IntegrateNewDevl          Bhoomi Dasari            11/29/2012                
# MAGIC 
# MAGIC Manasa Andru         2014-04-03             TFS - 8459              Added 3 new fields - GRP_AHY_PGM_AHY_BUYUP_CD_SK     IntegrateNewDevl           Kalyan Neelam           2014-06-24
# MAGIC                                                                                                    GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK and 
# MAGIC                                                                                                        GRP_AHY_PGM_SUB_BUYUP_CD_SK
# MAGIC 
# MAGIC Raja Gummadi         2016-06-22             5414                         Added new column GRP_AHY_PGM_OPT_OUT_IN                   IntegrateDev1                Jag Yelavarthi            2016-06-24

# MAGIC Set all foreign surragote keys
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
from pyspark.sql.functions import col, lit, row_number, monotonically_increasing_id, rpad
InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","X")
SrcSysCd = get_widget_value("SrcSysCd","")
RunID = get_widget_value("RunID","")

schema_IdsGrpAhyPgmExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("GRP_AHY_PGM_SK", IntegerType(), nullable=False),
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("GRP_AHY_PGM_STRT_DT_SK", StringType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("GRP_AHY_PGM_END_DT_SK", StringType(), nullable=False),
    StructField("GRP_AHY_PGM_INCNTV_STRT_DT_SK", StringType(), nullable=False),
    StructField("GRP_AHY_PGM_INCNTV_END_DT_SK", StringType(), nullable=False),
    StructField("GRP_SEL_SPOUSE_AHY_PGM_ID", StringType(), nullable=False),
    StructField("GRP_SEL_SUB_AHY_PGM_ID", StringType(), nullable=False),
    StructField("GRP_SEL_SUB_AHY_ONLY_PGM_ID", StringType(), nullable=False),
    StructField("GRP_AHY_PGM_AHY_BUYUP_CD", StringType(), nullable=True),
    StructField("GRP_AHY_PGM_SPOUSE_BUYUP_CD", StringType(), nullable=True),
    StructField("GRP_AHY_PGM_SUB_BUYUP_CD", StringType(), nullable=True),
    StructField("GRP_AHY_PGM_OPT_OUT_IN", StringType(), nullable=True)
])

df_IdsGrpAhyPgmExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsGrpAhyPgmExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_foreignKey = (
    df_IdsGrpAhyPgmExtr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svGrpAhyPgmStrtDtDtSk", GetFkeyDate("IDS", col("GRP_AHY_PGM_SK"), col("GRP_AHY_PGM_STRT_DT_SK"), lit(Logging)))
    .withColumn("svGrp", GetFkeyGrp("FACETS", col("GRP_AHY_PGM_SK"), col("GRP_ID"), lit(Logging)))
    .withColumn("svGrpAhyPgmEndDtSk", GetFkeyDate("IDS", col("GRP_AHY_PGM_SK"), col("GRP_AHY_PGM_END_DT_SK"), lit(Logging)))
    .withColumn("svGrpAhyPgmIncntvStrtDtSk", GetFkeyDate("IDS", col("GRP_AHY_PGM_SK"), col("GRP_AHY_PGM_INCNTV_STRT_DT_SK"), lit(Logging)))
    .withColumn("svGrpAhyPgmIncntvEndDtSk", GetFkeyDate("IDS", col("GRP_AHY_PGM_SK"), col("GRP_AHY_PGM_INCNTV_END_DT_SK"), lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("GRP_AHY_PGM_SK")))
    .withColumn("svGrpAhyBuyUpCd", GetFkeyCodes("HLTHFTNS", col("GRP_AHY_PGM_SK"), lit("AHY PROGRAM BUY UP"), col("GRP_AHY_PGM_AHY_BUYUP_CD"), lit("X")))
    .withColumn("svGrpAhySpBuyUpCd", GetFkeyCodes("HLTHFTNS", col("GRP_AHY_PGM_SK"), lit("AHY PROGRAM BUY UP"), col("GRP_AHY_PGM_SPOUSE_BUYUP_CD"), lit("X")))
    .withColumn("svGrpSubBuyUpCd", GetFkeyCodes("HLTHFTNS", col("GRP_AHY_PGM_SK"), lit("AHY PROGRAM BUY UP"), col("GRP_AHY_PGM_SUB_BUYUP_CD"), lit("X")))
)

from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

windowSpec = Window.orderBy(monotonically_increasing_id())
df_foreignKey = df_foreignKey.withColumn("_row_num", row_number().over(windowSpec))

df_fkey = df_foreignKey.filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
df_recycle = df_foreignKey.filter(col("ErrCount") > 0)
df_defaultUNK = df_foreignKey.filter(col("_row_num") == 1)
df_defaultNA = df_foreignKey.filter(col("_row_num") == 1)

df_fkeySelect = df_fkey.select(
    col("GRP_AHY_PGM_SK").alias("GRP_AHY_PGM_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("svGrpAhyPgmStrtDtDtSk").alias("GRP_AHY_PGM_STRT_DT_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svGrp").alias("GRP_SK"),
    col("svGrpAhyPgmEndDtSk").alias("GRP_AHY_PGM_END_DT_SK"),
    col("svGrpAhyPgmIncntvStrtDtSk").alias("GRP_AHY_PGM_INCNTV_STRT_DT_SK"),
    col("svGrpAhyPgmIncntvEndDtSk").alias("GRP_AHY_PGM_INCNTV_END_DT_SK"),
    col("GRP_SEL_SPOUSE_AHY_PGM_ID").alias("GRP_SEL_SPOUSE_AHY_PGM_ID"),
    col("GRP_SEL_SUB_AHY_PGM_ID").alias("GRP_SEL_SUB_AHY_PGM_ID"),
    col("GRP_SEL_SUB_AHY_ONLY_PGM_ID").alias("GRP_SEL_SUB_AHY_ONLY_PGM_ID"),
    col("svGrpAhyBuyUpCd").alias("GRP_AHY_PGM_AHY_BUYUP_CD_SK"),
    col("svGrpAhySpBuyUpCd").alias("GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK"),
    col("svGrpSubBuyUpCd").alias("GRP_AHY_PGM_SUB_BUYUP_CD_SK"),
    col("GRP_AHY_PGM_OPT_OUT_IN").alias("GRP_AHY_PGM_OPT_OUT_IN")
)

df_recycleSelect_pre = df_recycle.select(
    GetRecycleKey(col("GRP_AHY_PGM_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("GRP_AHY_PGM_SK").alias("GRP_AHY_PGM_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_AHY_PGM_STRT_DT_SK").alias("GRP_AHY_PGM_STRT_DT_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_AHY_PGM_END_DT_SK").alias("GRP_AHY_PGM_END_DT_SK"),
    col("GRP_AHY_PGM_INCNTV_STRT_DT_SK").alias("GRP_AHY_PGM_INCNTV_STRT_DT_SK"),
    col("GRP_AHY_PGM_INCNTV_END_DT_SK").alias("GRP_AHY_PGM_INCNTV_END_DT_SK"),
    col("GRP_SEL_SPOUSE_AHY_PGM_ID").alias("GRP_SEL_SPOUSE_AHY_PGM_ID"),
    col("GRP_SEL_SUB_AHY_PGM_ID").alias("GRP_SEL_SUB_AHY_PGM_ID"),
    col("GRP_SEL_SUB_AHY_ONLY_PGM_ID").alias("GRP_SEL_SUB_AHY_ONLY_PGM_ID"),
    col("GRP_AHY_PGM_AHY_BUYUP_CD").alias("GRP_AHY_PGM_AHY_BUYUP_CD_SK"),
    col("GRP_AHY_PGM_SPOUSE_BUYUP_CD").alias("GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK"),
    col("GRP_AHY_PGM_SUB_BUYUP_CD").alias("GRP_AHY_PGM_SUB_BUYUP_CD_SK")
)

df_recycleSelect = (
    df_recycleSelect_pre
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("GRP_AHY_PGM_STRT_DT_SK", rpad(col("GRP_AHY_PGM_STRT_DT_SK"), 10, " "))
    .withColumn("GRP_AHY_PGM_END_DT_SK", rpad(col("GRP_AHY_PGM_END_DT_SK"), 10, " "))
    .withColumn("GRP_AHY_PGM_INCNTV_STRT_DT_SK", rpad(col("GRP_AHY_PGM_INCNTV_STRT_DT_SK"), 10, " "))
    .withColumn("GRP_AHY_PGM_INCNTV_END_DT_SK", rpad(col("GRP_AHY_PGM_INCNTV_END_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), 100, " "))
    .withColumn("PRI_KEY_STRING", rpad(col("PRI_KEY_STRING"), 100, " "))
    .withColumn("GRP_ID", rpad(col("GRP_ID"), 100, " "))
    .withColumn("GRP_SEL_SPOUSE_AHY_PGM_ID", rpad(col("GRP_SEL_SPOUSE_AHY_PGM_ID"), 100, " "))
    .withColumn("GRP_SEL_SUB_AHY_PGM_ID", rpad(col("GRP_SEL_SUB_AHY_PGM_ID"), 100, " "))
    .withColumn("GRP_SEL_SUB_AHY_ONLY_PGM_ID", rpad(col("GRP_SEL_SUB_AHY_ONLY_PGM_ID"), 100, " "))
    .withColumn("GRP_AHY_PGM_AHY_BUYUP_CD_SK", rpad(col("GRP_AHY_PGM_AHY_BUYUP_CD_SK"), 100, " "))
    .withColumn("GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK", rpad(col("GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK"), 100, " "))
    .withColumn("GRP_AHY_PGM_SUB_BUYUP_CD_SK", rpad(col("GRP_AHY_PGM_SUB_BUYUP_CD_SK"), 100, " "))
)

write_files(
    df_recycleSelect.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "GRP_AHY_PGM_SK",
        "GRP_ID",
        "GRP_AHY_PGM_STRT_DT_SK",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "GRP_AHY_PGM_END_DT_SK",
        "GRP_AHY_PGM_INCNTV_STRT_DT_SK",
        "GRP_AHY_PGM_INCNTV_END_DT_SK",
        "GRP_SEL_SPOUSE_AHY_PGM_ID",
        "GRP_SEL_SUB_AHY_PGM_ID",
        "GRP_SEL_SUB_AHY_ONLY_PGM_ID",
        "GRP_AHY_PGM_AHY_BUYUP_CD_SK",
        "GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK",
        "GRP_AHY_PGM_SUB_BUYUP_CD_SK"
    ),
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_defaultUNK_pre = df_defaultUNK.select(
    lit(0).alias("GRP_AHY_PGM_SK"),
    lit("UNK").alias("GRP_ID"),
    lit("1753-01-01").alias("GRP_AHY_PGM_STRT_DT_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("GRP_SK"),
    lit("2199-12-31").alias("GRP_AHY_PGM_END_DT_SK"),
    lit("1753-01-01").alias("GRP_AHY_PGM_INCNTV_STRT_DT_SK"),
    lit("2199-12-31").alias("GRP_AHY_PGM_INCNTV_END_DT_SK"),
    lit("UNK").alias("GRP_SEL_SPOUSE_AHY_PGM_ID"),
    lit("UNK").alias("GRP_SEL_SUB_AHY_PGM_ID"),
    lit("UNK").alias("GRP_SEL_SUB_AHY_ONLY_PGM_ID"),
    lit(0).alias("GRP_AHY_PGM_AHY_BUYUP_CD_SK"),
    lit(0).alias("GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK"),
    lit(0).alias("GRP_AHY_PGM_SUB_BUYUP_CD_SK"),
    lit("N").alias("GRP_AHY_PGM_OPT_OUT_IN")
)

df_defaultUNKSelect = (
    df_defaultUNK_pre
    .withColumn("GRP_ID", rpad(col("GRP_ID"), 100, " "))
    .withColumn("GRP_AHY_PGM_STRT_DT_SK", rpad(col("GRP_AHY_PGM_STRT_DT_SK"), 10, " "))
    .withColumn("GRP_AHY_PGM_END_DT_SK", rpad(col("GRP_AHY_PGM_END_DT_SK"), 10, " "))
    .withColumn("GRP_AHY_PGM_INCNTV_STRT_DT_SK", rpad(col("GRP_AHY_PGM_INCNTV_STRT_DT_SK"), 10, " "))
    .withColumn("GRP_AHY_PGM_INCNTV_END_DT_SK", rpad(col("GRP_AHY_PGM_INCNTV_END_DT_SK"), 10, " "))
    .withColumn("GRP_SEL_SPOUSE_AHY_PGM_ID", rpad(col("GRP_SEL_SPOUSE_AHY_PGM_ID"), 100, " "))
    .withColumn("GRP_SEL_SUB_AHY_PGM_ID", rpad(col("GRP_SEL_SUB_AHY_PGM_ID"), 100, " "))
    .withColumn("GRP_SEL_SUB_AHY_ONLY_PGM_ID", rpad(col("GRP_SEL_SUB_AHY_ONLY_PGM_ID"), 100, " "))
    .withColumn("GRP_AHY_PGM_OPT_OUT_IN", rpad(col("GRP_AHY_PGM_OPT_OUT_IN"), 1, " "))
)

df_defaultNA_pre = df_defaultNA.select(
    lit(1).alias("GRP_AHY_PGM_SK"),
    lit("NA").alias("GRP_ID"),
    lit("1753-01-01").alias("GRP_AHY_PGM_STRT_DT_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("GRP_SK"),
    lit("2199-12-31").alias("GRP_AHY_PGM_END_DT_SK"),
    lit("1753-01-01").alias("GRP_AHY_PGM_INCNTV_STRT_DT_SK"),
    lit("2199-12-31").alias("GRP_AHY_PGM_INCNTV_END_DT_SK"),
    lit("NA").alias("GRP_SEL_SPOUSE_AHY_PGM_ID"),
    lit("NA").alias("GRP_SEL_SUB_AHY_PGM_ID"),
    lit("NA").alias("GRP_SEL_SUB_AHY_ONLY_PGM_ID"),
    lit(1).alias("GRP_AHY_PGM_AHY_BUYUP_CD_SK"),
    lit(1).alias("GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK"),
    lit(1).alias("GRP_AHY_PGM_SUB_BUYUP_CD_SK"),
    lit("N").alias("GRP_AHY_PGM_OPT_OUT_IN")
)

df_defaultNASelect = (
    df_defaultNA_pre
    .withColumn("GRP_ID", rpad(col("GRP_ID"), 100, " "))
    .withColumn("GRP_AHY_PGM_STRT_DT_SK", rpad(col("GRP_AHY_PGM_STRT_DT_SK"), 10, " "))
    .withColumn("GRP_AHY_PGM_END_DT_SK", rpad(col("GRP_AHY_PGM_END_DT_SK"), 10, " "))
    .withColumn("GRP_AHY_PGM_INCNTV_STRT_DT_SK", rpad(col("GRP_AHY_PGM_INCNTV_STRT_DT_SK"), 10, " "))
    .withColumn("GRP_AHY_PGM_INCNTV_END_DT_SK", rpad(col("GRP_AHY_PGM_INCNTV_END_DT_SK"), 10, " "))
    .withColumn("GRP_SEL_SPOUSE_AHY_PGM_ID", rpad(col("GRP_SEL_SPOUSE_AHY_PGM_ID"), 100, " "))
    .withColumn("GRP_SEL_SUB_AHY_PGM_ID", rpad(col("GRP_SEL_SUB_AHY_PGM_ID"), 100, " "))
    .withColumn("GRP_SEL_SUB_AHY_ONLY_PGM_ID", rpad(col("GRP_SEL_SUB_AHY_ONLY_PGM_ID"), 100, " "))
    .withColumn("GRP_AHY_PGM_OPT_OUT_IN", rpad(col("GRP_AHY_PGM_OPT_OUT_IN"), 1, " "))
)

df_fkeySelect_final = (
    df_fkeySelect
    .withColumn("GRP_ID", rpad(col("GRP_ID"), 100, " "))
    .withColumn("GRP_AHY_PGM_STRT_DT_SK", rpad(col("GRP_AHY_PGM_STRT_DT_SK"), 10, " "))
    .withColumn("GRP_AHY_PGM_END_DT_SK", rpad(col("GRP_AHY_PGM_END_DT_SK"), 10, " "))
    .withColumn("GRP_AHY_PGM_INCNTV_STRT_DT_SK", rpad(col("GRP_AHY_PGM_INCNTV_STRT_DT_SK"), 10, " "))
    .withColumn("GRP_AHY_PGM_INCNTV_END_DT_SK", rpad(col("GRP_AHY_PGM_INCNTV_END_DT_SK"), 10, " "))
    .withColumn("GRP_SEL_SPOUSE_AHY_PGM_ID", rpad(col("GRP_SEL_SPOUSE_AHY_PGM_ID"), 100, " "))
    .withColumn("GRP_SEL_SUB_AHY_PGM_ID", rpad(col("GRP_SEL_SUB_AHY_PGM_ID"), 100, " "))
    .withColumn("GRP_SEL_SUB_AHY_ONLY_PGM_ID", rpad(col("GRP_SEL_SUB_AHY_ONLY_PGM_ID"), 100, " "))
    .withColumn("GRP_AHY_PGM_AHY_BUYUP_CD_SK", rpad(col("GRP_AHY_PGM_AHY_BUYUP_CD_SK"), 100, " "))
    .withColumn("GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK", rpad(col("GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK"), 100, " "))
    .withColumn("GRP_AHY_PGM_SUB_BUYUP_CD_SK", rpad(col("GRP_AHY_PGM_SUB_BUYUP_CD_SK"), 100, " "))
    .withColumn("GRP_AHY_PGM_OPT_OUT_IN", rpad(col("GRP_AHY_PGM_OPT_OUT_IN"), 1, " "))
)

df_collector = df_fkeySelect_final.unionByName(df_defaultUNKSelect).unionByName(df_defaultNASelect)

write_files(
    df_collector.select(
        "GRP_AHY_PGM_SK",
        "GRP_ID",
        "GRP_AHY_PGM_STRT_DT_SK",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "GRP_SK",
        "GRP_AHY_PGM_END_DT_SK",
        "GRP_AHY_PGM_INCNTV_STRT_DT_SK",
        "GRP_AHY_PGM_INCNTV_END_DT_SK",
        "GRP_SEL_SPOUSE_AHY_PGM_ID",
        "GRP_SEL_SUB_AHY_PGM_ID",
        "GRP_SEL_SUB_AHY_ONLY_PGM_ID",
        "GRP_AHY_PGM_AHY_BUYUP_CD_SK",
        "GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK",
        "GRP_AHY_PGM_SUB_BUYUP_CD_SK",
        "GRP_AHY_PGM_OPT_OUT_IN"
    ),
    f"{adls_path}/load/GRP_AHY_PGM.{SrcSysCd}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)