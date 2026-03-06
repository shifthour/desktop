# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: IdsExtrnlUserLoadSeq
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                  02/09/2011      4574                                Originally Programmed                          IntegrateNewDevl          Steph Goddard            02/11/2011

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
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile","FctsExtrnlUser.ExtrnlUser.dat")
Logging = get_widget_value("Logging","X")
SrcSysCdSk = get_widget_value("SrcSysCdSk","1561")

schema_IdsExtrnlUserExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("EXTRNL_USER_SK", IntegerType(), False),
    StructField("EXTRNL_USER_CNSTTNT_TYP_CD", StringType(), False),
    StructField("EXTRNL_USER_FCTS_ID", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("MBR_SFX_NO", StringType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("AGNT_SK", StringType(), False),
    StructField("GRP_SK", StringType(), False),
    StructField("MBR_SK", StringType(), False),
    StructField("PROV_SK", StringType(), False),
    StructField("EXTRNL_USER_CNSTTNT_TYP_CD_SK", IntegerType(), False),
    StructField("STTUS_CD_SK", IntegerType(), False),
    StructField("SRC_SYS_CRT_DTM", TimestampType(), False),
    StructField("SRC_SYS_PRCS_DTM", TimestampType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("EXTRNL_USER_ID", StringType(), False)
])

df_IdsExtrnlUserExtr = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_IdsExtrnlUserExtr)
    .load(f"{adls_path}/key/{InFile}")
)

w = Window.orderBy(F.lit(1))

df_foreignKey_in = (
    df_IdsExtrnlUserExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svAgntSk", GetFkeyAgnt(F.col("SRC_SYS_CD"), F.col("EXTRNL_USER_SK"), F.col("AGNT_SK"), Logging))
    .withColumn("svGrpSk", GetFkeyGrp(F.col("SRC_SYS_CD"), F.col("EXTRNL_USER_SK"), F.col("GRP_SK"), Logging))
    .withColumn("svMbrSk", GetFkeyMbr(F.col("SRC_SYS_CD"), F.col("EXTRNL_USER_SK"), F.col("MBR_SK"), Logging))
    .withColumn("svProvSk", GetFkeyProv(F.col("SRC_SYS_CD"), F.col("EXTRNL_USER_SK"), F.col("PROV_SK"), Logging))
    .withColumn("svExtrnlUsrCnstTypCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("EXTRNL_USER_SK"), F.lit("CONSTITUENT"), F.col("EXTRNL_USER_CNSTTNT_TYP_CD_SK"), Logging))
    .withColumn("svSttusCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("EXTRNL_USER_SK"), F.lit("FRONT END AUTHENTICATION STATUS"), F.col("STTUS_CD_SK"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("EXTRNL_USER_SK")))
    .withColumn("row_num", F.row_number().over(w))
)

df_Fkey = df_foreignKey_in.filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y')).select(
    F.col("EXTRNL_USER_SK").alias("EXTRNL_USER_SK"),
    F.col("EXTRNL_USER_CNSTTNT_TYP_CD").alias("EXTRNL_USER_CNSTTNT_TYP_CD"),
    F.col("EXTRNL_USER_FCTS_ID").alias("EXTRNL_USER_FCTS_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.lit(SrcSysCdSk).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svAgntSk").alias("AGNT_SK"),
    F.col("svGrpSk").alias("GRP_SK"),
    F.col("svMbrSk").alias("MBR_SK"),
    F.col("svProvSk").alias("PROV_SK"),
    F.col("svExtrnlUsrCnstTypCdSk").alias("EXTRNL_USER_CNSTTNT_TYP_CD_SK"),
    F.col("svSttusCdSk").alias("STTUS_CD_SK"),
    F.col("SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
    F.col("SRC_SYS_PRCS_DTM").alias("SRC_SYS_PRCS_DTM"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("EXTRNL_USER_ID").alias("EXTRNL_USER_ID"),
    F.col("row_num")
)

df_Recycle = df_foreignKey_in.filter(F.col("ErrCount") > 0).select(
    GetRecycleKey(F.col("EXTRNL_USER_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + 1).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("EXTRNL_USER_SK").alias("EXTRNL_USER_SK"),
    F.col("EXTRNL_USER_CNSTTNT_TYP_CD").alias("EXTRNL_USER_CNSTTNT_TYP_CD"),
    F.col("EXTRNL_USER_FCTS_ID").alias("EXTRNL_USER_FCTS_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AGNT_SK").alias("AGNT_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("PROV_SK").alias("PROV_SK"),
    F.col("EXTRNL_USER_CNSTTNT_TYP_CD_SK").alias("EXTRNL_USER_CNSTTNT_TYP_CD_SK"),
    F.col("STTUS_CD_SK").alias("STTUS_CD_SK"),
    F.col("SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
    F.col("SRC_SYS_PRCS_DTM").alias("SRC_SYS_PRCS_DTM"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("EXTRNL_USER_ID").alias("EXTRNL_USER_ID"),
    F.col("row_num")
)

df_DefaultUNK = df_foreignKey_in.filter(F.col("row_num") == 1).select(
    F.lit(0).alias("EXTRNL_USER_SK"),
    F.lit("UNK").alias("EXTRNL_USER_CNSTTNT_TYP_CD"),
    F.lit("UNK").alias("EXTRNL_USER_FCTS_ID"),
    F.lit("UNK").alias("GRP_ID"),
    F.lit("0").alias("MBR_SFX_NO"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("AGNT_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("PROV_SK"),
    F.lit(0).alias("EXTRNL_USER_CNSTTNT_TYP_CD_SK"),
    F.lit(0).alias("STTUS_CD_SK"),
    F.lit("1753-01-01-00.00.00.000000").alias("SRC_SYS_CRT_DTM"),
    F.lit("2199-12-31-00.00.00.000000").alias("SRC_SYS_PRCS_DTM"),
    F.lit(0).alias("MBR_UNIQ_KEY"),
    F.lit("UNK").alias("EXTRNL_USER_ID"),
    F.col("row_num")
)

df_DefaultNA = df_foreignKey_in.filter(F.col("row_num") == 1).select(
    F.lit(1).alias("EXTRNL_USER_SK"),
    F.lit("NA").alias("EXTRNL_USER_CNSTTNT_TYP_CD"),
    F.lit("NA").alias("EXTRNL_USER_FCTS_ID"),
    F.lit("NA").alias("GRP_ID"),
    F.lit("1").alias("MBR_SFX_NO"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("AGNT_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("MBR_SK"),
    F.lit(1).alias("PROV_SK"),
    F.lit(1).alias("EXTRNL_USER_CNSTTNT_TYP_CD_SK"),
    F.lit(1).alias("STTUS_CD_SK"),
    F.lit("1753-01-01-00.00.00.000000").alias("SRC_SYS_CRT_DTM"),
    F.lit("2199-12-31-00.00.00.000000").alias("SRC_SYS_PRCS_DTM"),
    F.lit(1).alias("MBR_UNIQ_KEY"),
    F.lit("NA").alias("EXTRNL_USER_ID"),
    F.col("row_num")
)

df_Recycle_final = df_Recycle.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("PRI_KEY_STRING"), 255, " ").alias("PRI_KEY_STRING"),
    F.col("EXTRNL_USER_SK"),
    F.rpad(F.col("EXTRNL_USER_CNSTTNT_TYP_CD"), 255, " ").alias("EXTRNL_USER_CNSTTNT_TYP_CD"),
    F.rpad(F.col("EXTRNL_USER_FCTS_ID"), 255, " ").alias("EXTRNL_USER_FCTS_ID"),
    F.rpad(F.col("GRP_ID"), 255, " ").alias("GRP_ID"),
    F.rpad(F.col("MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("AGNT_SK"), 10, " ").alias("AGNT_SK"),
    F.rpad(F.col("GRP_SK"), 10, " ").alias("GRP_SK"),
    F.rpad(F.col("MBR_SK"), 10, " ").alias("MBR_SK"),
    F.rpad(F.col("PROV_SK"), 10, " ").alias("PROV_SK"),
    F.col("EXTRNL_USER_CNSTTNT_TYP_CD_SK"),
    F.col("STTUS_CD_SK"),
    F.col("SRC_SYS_CRT_DTM"),
    F.col("SRC_SYS_PRCS_DTM"),
    F.col("MBR_UNIQ_KEY"),
    F.rpad(F.col("EXTRNL_USER_ID"), 255, " ").alias("EXTRNL_USER_ID")
)

write_files(
    df_Recycle_final,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_Collector = df_Fkey.select(*[c for c in df_Fkey.columns if c != "row_num"]) \
    .unionByName(df_DefaultUNK.select(*[c for c in df_DefaultUNK.columns if c != "row_num"])) \
    .unionByName(df_DefaultNA.select(*[c for c in df_DefaultNA.columns if c != "row_num"]))

df_Collector_final = df_Collector.select(
    F.col("EXTRNL_USER_SK"),
    F.rpad(F.col("EXTRNL_USER_CNSTTNT_TYP_CD"), 255, " ").alias("EXTRNL_USER_CNSTTNT_TYP_CD"),
    F.rpad(F.col("EXTRNL_USER_FCTS_ID"), 255, " ").alias("EXTRNL_USER_FCTS_ID"),
    F.rpad(F.col("GRP_ID"), 255, " ").alias("GRP_ID"),
    F.rpad(F.col("MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("AGNT_SK"), 10, " ").alias("AGNT_SK"),
    F.rpad(F.col("GRP_SK"), 10, " ").alias("GRP_SK"),
    F.rpad(F.col("MBR_SK"), 10, " ").alias("MBR_SK"),
    F.rpad(F.col("PROV_SK"), 10, " ").alias("PROV_SK"),
    F.col("EXTRNL_USER_CNSTTNT_TYP_CD_SK"),
    F.col("STTUS_CD_SK"),
    F.col("SRC_SYS_CRT_DTM"),
    F.col("SRC_SYS_PRCS_DTM"),
    F.col("MBR_UNIQ_KEY"),
    F.rpad(F.col("EXTRNL_USER_ID"), 255, " ").alias("EXTRNL_USER_ID")
)

write_files(
    df_Collector_final,
    f"EXTRNL_USER.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)