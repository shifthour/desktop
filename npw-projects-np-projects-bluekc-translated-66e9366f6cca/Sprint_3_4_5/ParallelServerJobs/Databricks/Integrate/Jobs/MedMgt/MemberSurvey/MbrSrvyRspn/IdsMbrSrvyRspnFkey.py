# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY: IdsMbrSrvyRspnLoadSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING: IDS Member Survey Response Foregn Key job
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Kalyan Neelam         2011-04-08               4673                       Initial Programming                                                IntegrateWrhsDevl                     SAndrew                  2011-04-20
# MAGIC Raja Gummadi         2013-11-20                5063                Added 5 new columns to table                                    IntegrateNewDevl                      Kalyan Neelam         2013-11-24
# MAGIC                                                                                               MBR_SRVY_RSPN_UPDT_SRC_CD_SK                
# MAGIC                                                                                               CMPLTN_DT_SK
# MAGIC                                                                                               STRT_DT_SK
# MAGIC                                                                                               UPDT_DT_SK
# MAGIC                                                                                               HRA_ID

# MAGIC MBR_SRVY_RSPN Foreign Key job
# MAGIC Writing Sequential File to /load
# MAGIC Merge source data with default rows
# MAGIC Set all foreign surrogate keys
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, row_number, monotonically_increasing_id, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')

schema_MbrSrvyRspn = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("MBR_SRVY_RSPN_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("MBR_SRVY_TYP_CD", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("QSTN_CD_TX", StringType(), True),
    StructField("RSPN_DT_SK", StringType(), False),
    StructField("SEQ_NO", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SYS_CRT_DT_SK", StringType(), False),
    StructField("RCRD_CT", IntegerType(), True),
    StructField("RSPN_ANSWER_TX", StringType(), False),
    StructField("MBR_SRVY_RSPN_UPDT_SRC_CD", StringType(), False),
    StructField("CMPLTN_DT_SK", StringType(), False),
    StructField("STRT_DT_SK", StringType(), False),
    StructField("UPDT_DT_SK", StringType(), False),
    StructField("HRA_ID", StringType(), False)
])

dfMbrSrvyRspn = (
    spark.read
    .option("header", False)
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_MbrSrvyRspn)
    .csv(f"{adls_path}/key/{InFile}")
)

dfKey = dfMbrSrvyRspn

dfEnriched = (
    dfKey
    .withColumn("svRspnDtSk", GetFkeyDate(lit('IDS'), col("MBR_SRVY_RSPN_SK"), col("RSPN_DT_SK"), Logging))
    .withColumn("svMbrSK", GetFkeyMbr(lit('FACETS'), col("MBR_SRVY_RSPN_SK"), col("MBR_UNIQ_KEY"), Logging))
    .withColumn("svGrpSk", GetFkeyGrp(lit('FACETS'), col("MBR_SRVY_RSPN_SK"), col("GRP_ID"), Logging))
    .withColumn("svMbrSrvyTypCdSk", GetFkeyCodes(lit("BCBSKCCOMMON"), col("MBR_SRVY_RSPN_SK"), lit("MEMBER SURVEY TYPE"), col("MBR_SRVY_TYP_CD"), Logging))
    .withColumn("svMbrSrvyQstnSk", GetFkeyMbrSrvyQstn(col("SRC_SYS_CD"), col("MBR_SRVY_RSPN_SK"), col("MBR_SRVY_TYP_CD"), col("QSTN_CD_TX"), Logging))
    .withColumn("svSysCrtDtSk", GetFkeyDate(lit('IDS'), col("MBR_SRVY_RSPN_SK"), col("SYS_CRT_DT_SK"), Logging))
    .withColumn("svMbrSrvyRspnUpdtCdSk", GetFkeyClctnDomainCodes(lit('HLTHFTNS'), col("MBR_SRVY_RSPN_SK"), lit('MEMBER SURVEY RESPONSE UPDATE SOURCE'), lit('HEALTH FITNESS'), lit('MEMBER SURVEY RESPONSE UPDATE SOURCE'), lit('IDS'), Upcase(col("MBR_SRVY_RSPN_UPDT_SRC_CD")), Logging))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("MBR_SRVY_RSPN_SK")))
)

dfFkey = (
    dfEnriched
    .filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
    .select(
        col("MBR_SRVY_RSPN_SK"),
        col("SRC_SYS_CD_SK"),
        col("MBR_SRVY_TYP_CD"),
        col("MBR_UNIQ_KEY"),
        col("QSTN_CD_TX"),
        col("svRspnDtSk").alias("RSPN_DT_SK"),
        col("SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svGrpSk").alias("GRP_SK"),
        col("svMbrSK").alias("MBR_SK"),
        col("svMbrSrvyQstnSk").alias("MBR_SRVY_QSTN_SK"),
        col("svMbrSrvyTypCdSk").alias("MBR_SRVY_TYP_CD_SK"),
        col("svSysCrtDtSk").alias("SYS_CRT_DT_SK"),
        col("RCRD_CT"),
        col("RSPN_ANSWER_TX"),
        col("svMbrSrvyRspnUpdtCdSk").alias("MBR_SRVY_RSPN_UPDT_SRC_CD_SK"),
        col("CMPLTN_DT_SK"),
        col("STRT_DT_SK"),
        col("UPDT_DT_SK"),
        col("HRA_ID")
    )
)

dfRecycle = (
    dfEnriched
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("MBR_SRVY_RSPN_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD"),
        col("DISCARD_IN"),
        col("PASS_THRU_IN"),
        col("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD"),
        col("PRI_KEY_STRING"),
        col("MBR_SRVY_RSPN_SK"),
        col("SRC_SYS_CD_SK"),
        col("MBR_SRVY_TYP_CD"),
        col("MBR_UNIQ_KEY"),
        col("QSTN_CD_TX"),
        col("RSPN_DT_SK"),
        col("SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svGrpSk").alias("GRP_SK"),
        col("SYS_CRT_DT_SK"),
        col("RCRD_CT"),
        col("RSPN_ANSWER_TX"),
        col("MBR_SRVY_RSPN_UPDT_SRC_CD"),
        col("CMPLTN_DT_SK"),
        col("STRT_DT_SK"),
        col("UPDT_DT_SK"),
        col("HRA_ID")
    )
)

dfRecycle = dfRecycle \
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " ")) \
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " ")) \
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " ")) \
    .withColumn("RSPN_DT_SK", rpad(col("RSPN_DT_SK"), 10, " ")) \
    .withColumn("SYS_CRT_DT_SK", rpad(col("SYS_CRT_DT_SK"), 10, " ")) \
    .withColumn("CMPLTN_DT_SK", rpad(col("CMPLTN_DT_SK"), 10, " ")) \
    .withColumn("STRT_DT_SK", rpad(col("STRT_DT_SK"), 10, " ")) \
    .withColumn("UPDT_DT_SK", rpad(col("UPDT_DT_SK"), 10, " "))

write_files(
    dfRecycle,
    f"hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

dfDefaultNaBase = dfEnriched.withColumn("INROWNUM", row_number().over(Window.orderBy(lit(1)))).filter(col("INROWNUM") == 1)
dfDefaultNa = dfDefaultNaBase.select(
    lit(1).alias("MBR_SRVY_RSPN_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("MBR_SRVY_TYP_CD"),
    lit(1).alias("MBR_UNIQ_KEY"),
    lit("NA").alias("QSTN_CD_TX"),
    lit("1753-01-01").alias("RSPN_DT_SK"),
    lit(1).alias("SEQ_NO"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("GRP_SK"),
    lit(1).alias("MBR_SK"),
    lit(1).alias("MBR_SRVY_QSTN_SK"),
    lit(1).alias("MBR_SRVY_TYP_CD_SK"),
    lit("1753-01-01").alias("SYS_CRT_DT_SK"),
    lit(1).alias("RCRD_CT"),
    lit("NA").alias("RSPN_ANSWER_TX"),
    lit(1).alias("MBR_SRVY_RSPN_UPDT_SRC_CD_SK"),
    lit("2199-12-31").alias("CMPLTN_DT_SK"),
    lit("1753-01-01").alias("STRT_DT_SK"),
    lit("2199-12-31").alias("UPDT_DT_SK"),
    lit(1).alias("HRA_ID")
)

dfDefaultUnkBase = dfEnriched.withColumn("INROWNUM", row_number().over(Window.orderBy(lit(1)))).filter(col("INROWNUM") == 1)
dfDefaultUnk = dfDefaultUnkBase.select(
    lit(0).alias("MBR_SRVY_RSPN_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("MBR_SRVY_TYP_CD"),
    lit(0).alias("MBR_UNIQ_KEY"),
    lit("UNK").alias("QSTN_CD_TX"),
    lit("1753-01-01").alias("RSPN_DT_SK"),
    lit(0).alias("SEQ_NO"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("GRP_SK"),
    lit(0).alias("MBR_SK"),
    lit(0).alias("MBR_SRVY_QSTN_SK"),
    lit(0).alias("MBR_SRVY_TYP_CD_SK"),
    lit("1753-01-01").alias("SYS_CRT_DT_SK"),
    lit(0).alias("RCRD_CT"),
    lit("UNK").alias("RSPN_ANSWER_TX"),
    lit(0).alias("MBR_SRVY_RSPN_UPDT_SRC_CD_SK"),
    lit("2199-12-31").alias("CMPLTN_DT_SK"),
    lit("1753-01-01").alias("STRT_DT_SK"),
    lit("2199-12-31").alias("UPDT_DT_SK"),
    lit(0).alias("HRA_ID")
)

dfCollector = dfFkey.unionByName(dfDefaultNa).unionByName(dfDefaultUnk)

dfCollector = dfCollector \
    .withColumn("RSPN_DT_SK", rpad(col("RSPN_DT_SK"), 10, " ")) \
    .withColumn("SYS_CRT_DT_SK", rpad(col("SYS_CRT_DT_SK"), 10, " ")) \
    .withColumn("CMPLTN_DT_SK", rpad(col("CMPLTN_DT_SK"), 10, " ")) \
    .withColumn("STRT_DT_SK", rpad(col("STRT_DT_SK"), 10, " ")) \
    .withColumn("UPDT_DT_SK", rpad(col("UPDT_DT_SK"), 10, " "))

write_files(
    dfCollector.select(
        "MBR_SRVY_RSPN_SK",
        "SRC_SYS_CD_SK",
        "MBR_SRVY_TYP_CD",
        "MBR_UNIQ_KEY",
        "QSTN_CD_TX",
        "RSPN_DT_SK",
        "SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "GRP_SK",
        "MBR_SK",
        "MBR_SRVY_QSTN_SK",
        "MBR_SRVY_TYP_CD_SK",
        "SYS_CRT_DT_SK",
        "RCRD_CT",
        "RSPN_ANSWER_TX",
        "MBR_SRVY_RSPN_UPDT_SRC_CD_SK",
        "CMPLTN_DT_SK",
        "STRT_DT_SK",
        "UPDT_DT_SK",
        "HRA_ID"
    ),
    f"{adls_path}/load/MBR_SRVY_RSPN.{SrcSysCd}.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)