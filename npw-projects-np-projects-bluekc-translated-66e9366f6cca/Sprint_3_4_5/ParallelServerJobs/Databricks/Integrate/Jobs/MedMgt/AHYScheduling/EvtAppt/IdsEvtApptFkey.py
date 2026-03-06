# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by: IdsEvtLoadSeq
# MAGIC 
# MAGIC Processing : Foreign Key job for IDS EVT_APPT
# MAGIC                     
# MAGIC Modifications:                         
# MAGIC                                                    Project/                                                                                                                                                     Code                        Date
# MAGIC Developer             Date              Altiris #        Change Description                                                                                                               Reviewer                  Reviewed
# MAGIC ---------------------------  -------------------   -----------------  ---------------------------------------------------------------------------------------------------                                            ---------------------------     -------------------   
# MAGIC Kalyan Neelam      2010-11-02    4529            Initial Programming                                                                            IntegrateNewDevl       SAndrew                           12/07/2010

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","X")
InFile = get_widget_value("InFile","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
SrcSysCd = get_widget_value("SrcSysCd","")
RunCycle = get_widget_value("RunCycle","")

schema_IdsEvtApptExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("EVT_APPT_SK", IntegerType(), nullable=False),
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("EVT_DT_SK", StringType(), nullable=False),
    StructField("EVT_TYP_ID", StringType(), nullable=False),
    StructField("SEQ_NO", IntegerType(), nullable=False),
    StructField("EVT_APPT_STRT_TM_TX", StringType(), nullable=False),
    StructField("STAFF_ID", IntegerType(), nullable=True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=True),
    StructField("EVT_APPT_RSN_ID", IntegerType(), nullable=True),
    StructField("LOC_ID", IntegerType(), nullable=True),
    StructField("EVT_APPT_DELD_IN", StringType(), nullable=False),
    StructField("EVT_APPT_END_TM_TX", StringType(), nullable=False),
    StructField("LAST_UPDT_DTM", TimestampType(), nullable=False),
    StructField("LAST_UPDT_USER_ID", StringType(), nullable=False),
    StructField("EVT_TYP_CD", StringType(), nullable=False),
    StructField("EVT_APPT_RSN_NM", StringType(), nullable=False),
    StructField("EFF_DT", TimestampType(), nullable=False)
])

df_IdsEvtApptExtr = (
    spark.read.format("csv")
    .option("quote", '"')
    .option("header", False)
    .schema(schema_IdsEvtApptExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_ForeignKey = (
    df_IdsEvtApptExtr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svEvtDtSk", GetFkeyDate('IDS', col("EVT_APPT_SK"), col("EVT_DT_SK"), Logging))
    .withColumn("svEvtLocSk", GetFkeyEvtLoc(SrcSysCd, col("EVT_APPT_SK"), col("LOC_ID"), Logging))
    .withColumn("svEvtStaffSk", GetFkeyEvtStaff(SrcSysCd, col("EVT_APPT_SK"), col("STAFF_ID"), Logging))
    .withColumn("svEvtTypSk", GetFkeyEvtTyp(SrcSysCd, col("EVT_APPT_SK"), col("EVT_TYP_ID"), Logging))
    .withColumn("svEvtApptRsnSk", GetFkeyEvtApptRsn(SrcSysCd, col("EVT_APPT_SK"), col("EVT_TYP_CD"), col("EVT_APPT_RSN_NM"), col("EFF_DT"), Logging))
    .withColumn("svGrpSk", GetFkeyGrp('FACETS', col("EVT_APPT_SK"), col("GRP_ID"), Logging))
    .withColumn("svMbrSk", GetFkeyMbr('FACETS', col("EVT_APPT_SK"), col("MBR_UNIQ_KEY"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("EVT_APPT_SK")))
)

df_Fkey = (
    df_ForeignKey
    .filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
    .select(
        col("EVT_APPT_SK").alias("EVT_APPT_SK"),
        col("GRP_ID").alias("GRP_ID"),
        col("svEvtDtSk").alias("EVT_DT_SK"),
        col("EVT_TYP_ID").alias("EVT_TYP_ID"),
        col("SEQ_NO").alias("SEQ_NO"),
        col("EVT_APPT_STRT_TM_TX").alias("EVT_APPT_STRT_TM_TX"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svEvtApptRsnSk").alias("EVT_APPT_RSN_SK"),
        col("svEvtLocSk").alias("EVT_LOC_SK"),
        col("svEvtStaffSk").alias("EVT_STAFF_SK"),
        col("svEvtTypSk").alias("EVT_TYP_SK"),
        col("svGrpSk").alias("GRP_SK"),
        col("svMbrSk").alias("MBR_SK"),
        col("EVT_APPT_DELD_IN").alias("EVT_APPT_DELD_IN"),
        col("EVT_APPT_END_TM_TX").alias("EVT_APPT_END_TM_TX"),
        col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
    )
)

df_Recycle = (
    df_ForeignKey
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("EVT_APPT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("EVT_APPT_SK").alias("EVT_APPT_SK"),
        col("GRP_ID").alias("GRP_ID"),
        col("EVT_DT_SK").alias("EVT_DT_SK"),
        col("EVT_TYP_ID").alias("EVT_TYP_ID"),
        col("SEQ_NO").alias("SEQ_NO"),
        col("EVT_APPT_STRT_TM_TX").alias("EVT_APPT_STRT_TM_TX"),
        col("STAFF_ID").alias("STAFF_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("EVT_APPT_RSN_ID").alias("EVT_APPT_RSN_ID"),
        col("LOC_ID").alias("LOC_ID"),
        col("EVT_APPT_DELD_IN").alias("EVT_APPT_DELD_IN"),
        col("EVT_APPT_END_TM_TX").alias("EVT_APPT_END_TM_TX"),
        col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
        col("EVT_TYP_CD").alias("EVT_TYP_CD"),
        col("EVT_APPT_RSN_NM").alias("EVT_APPT_RSN_NM"),
        col("EFF_DT").alias("EFF_DT")
    )
)

write_files(
    df_Recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

dfTemp = df_ForeignKey.withColumn("row_num", row_number().over(Window.orderBy(lit(1))))

df_DefaultNA = (
    dfTemp
    .filter(col("row_num") == 1)
    .select(
        lit(1).alias("EVT_APPT_SK"),
        lit("NA").alias("GRP_ID"),
        lit("1753-01-01").alias("EVT_DT_SK"),
        lit("NA").alias("EVT_TYP_ID"),
        lit(1).alias("SEQ_NO"),
        lit("NA").alias("EVT_APPT_STRT_TM_TX"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("EVT_APPT_RSN_SK"),
        lit(1).alias("EVT_LOC_SK"),
        lit(1).alias("EVT_STAFF_SK"),
        lit(1).alias("EVT_TYP_SK"),
        lit(1).alias("GRP_SK"),
        lit(1).alias("MBR_SK"),
        lit("X").alias("EVT_APPT_DELD_IN"),
        lit("NA").alias("EVT_APPT_END_TM_TX"),
        lit("1753-01-01-00.00.00.000000").alias("LAST_UPDT_DTM"),
        lit("NA").alias("LAST_UPDT_USER_ID")
    )
)

df_DefaultUNK = (
    dfTemp
    .filter(col("row_num") == 1)
    .select(
        lit(0).alias("EVT_APPT_SK"),
        lit("UNK").alias("GRP_ID"),
        lit("1753-01-01").alias("EVT_DT_SK"),
        lit("UNK").alias("EVT_TYP_ID"),
        lit(0).alias("SEQ_NO"),
        lit("UNK").alias("EVT_APPT_STRT_TM_TX"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("EVT_APPT_RSN_SK"),
        lit(0).alias("EVT_LOC_SK"),
        lit(0).alias("EVT_STAFF_SK"),
        lit(0).alias("EVT_TYP_SK"),
        lit(0).alias("GRP_SK"),
        lit(0).alias("MBR_SK"),
        lit("X").alias("EVT_APPT_DELD_IN"),
        lit("UNK").alias("EVT_APPT_END_TM_TX"),
        lit("1753-01-01-00.00.00.000000").alias("LAST_UPDT_DTM"),
        lit("UNK").alias("LAST_UPDT_USER_ID")
    )
)

df_Collector = (
    df_Fkey.select(
        "EVT_APPT_SK",
        "GRP_ID",
        "EVT_DT_SK",
        "EVT_TYP_ID",
        "SEQ_NO",
        "EVT_APPT_STRT_TM_TX",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "EVT_APPT_RSN_SK",
        "EVT_LOC_SK",
        "EVT_STAFF_SK",
        "EVT_TYP_SK",
        "GRP_SK",
        "MBR_SK",
        "EVT_APPT_DELD_IN",
        "EVT_APPT_END_TM_TX",
        "LAST_UPDT_DTM",
        "LAST_UPDT_USER_ID"
    )
    .unionByName(
        df_DefaultNA.select(
            "EVT_APPT_SK",
            "GRP_ID",
            "EVT_DT_SK",
            "EVT_TYP_ID",
            "SEQ_NO",
            "EVT_APPT_STRT_TM_TX",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "EVT_APPT_RSN_SK",
            "EVT_LOC_SK",
            "EVT_STAFF_SK",
            "EVT_TYP_SK",
            "GRP_SK",
            "MBR_SK",
            "EVT_APPT_DELD_IN",
            "EVT_APPT_END_TM_TX",
            "LAST_UPDT_DTM",
            "LAST_UPDT_USER_ID"
        )
    )
    .unionByName(
        df_DefaultUNK.select(
            "EVT_APPT_SK",
            "GRP_ID",
            "EVT_DT_SK",
            "EVT_TYP_ID",
            "SEQ_NO",
            "EVT_APPT_STRT_TM_TX",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "EVT_APPT_RSN_SK",
            "EVT_LOC_SK",
            "EVT_STAFF_SK",
            "EVT_TYP_SK",
            "GRP_SK",
            "MBR_SK",
            "EVT_APPT_DELD_IN",
            "EVT_APPT_END_TM_TX",
            "LAST_UPDT_DTM",
            "LAST_UPDT_USER_ID"
        )
    )
)

df_EvtApptOutput = (
    df_Collector
    .withColumn("EVT_DT_SK", rpad("EVT_DT_SK", 10, " "))
    .withColumn("EVT_APPT_DELD_IN", rpad("EVT_APPT_DELD_IN", 1, " "))
    .select(
        "EVT_APPT_SK",
        "GRP_ID",
        "EVT_DT_SK",
        "EVT_TYP_ID",
        "SEQ_NO",
        "EVT_APPT_STRT_TM_TX",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "EVT_APPT_RSN_SK",
        "EVT_LOC_SK",
        "EVT_STAFF_SK",
        "EVT_TYP_SK",
        "GRP_SK",
        "MBR_SK",
        "EVT_APPT_DELD_IN",
        "EVT_APPT_END_TM_TX",
        "LAST_UPDT_DTM",
        "LAST_UPDT_USER_ID"
    )
)

write_files(
    df_EvtApptOutput,
    f"{adls_path}/load/EVT_APPT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)