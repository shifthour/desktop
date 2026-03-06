# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 05/25/07 11:56:51 Batch  14390_43020 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 05/25/07 11:52:00 Batch  14390_42723 INIT bckcett testIDSnew dsadm bls for on
# MAGIC ^1_1 05/17/07 12:03:56 Batch  14382_43445 PROMOTE bckcett testIDSnew u10913 Ollie move from devl to test
# MAGIC ^1_1 05/17/07 09:42:32 Batch  14382_34957 PROMOTE bckcett testIDSnew u10913 Ollie move from devl to test
# MAGIC ^1_1 05/17/07 09:38:32 Batch  14382_34718 INIT bckcett devlIDS30 u10913 Ollie Move from devl to test
# MAGIC ^1_1 04/27/07 12:54:12 Batch  14362_46467 INIT bckcett devlIDS30 u10913 O. Nielsen move from devl to 4.3 Environment
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:Takes the primary key file and use it in the foriegn key process.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari               3/10/2007            CDS Sunset/3279          Originally Programmed                              devlIDS30                  Steph Goddard          3/29/2007

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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType, DateType
from pyspark.sql.functions import col, lit, when, row_number, monotonically_increasing_id, rpad, trim
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile", "FctsPrvcyMbrRelshpExtr.PrvcyMbrRelshp.dat")
Logging = get_widget_value("Logging", "Y")
RunID = get_widget_value("RunID", "2007010105")

schema_IdsPrvcyMbrRelshpExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(10, 0), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("PRVCY_MBR_RELSHP_SK", IntegerType(), nullable=False),
    StructField("PRVCY_MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("MBR_SK", IntegerType(), nullable=False),
    StructField("PRVCY_EXTRNL_MBR_SK", IntegerType(), nullable=False),
    StructField("PRVCY_PRSNL_REP_SK", IntegerType(), nullable=False),
    StructField("CRT_DT_SK", DateType(), nullable=False),
    StructField("EFF_DT_SK", StringType(), nullable=False),
    StructField("TERM_DT_SK", StringType(), nullable=False),
    StructField("PRVCY_MBR_RELSHP_REP_CD_SK", IntegerType(), nullable=False),
    StructField("PRVCY_MBR_SRC_CD_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_LAST_UPDT_DT_SK", StringType(), nullable=False),
    StructField("SRC_SYS_LAST_UPDT_USER_SK", IntegerType(), nullable=False)
])

df_IdsPrvcyMbrRelshpExtr = (
    spark.read
    .option("sep", ",")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_IdsPrvcyMbrRelshpExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_ForeignKey_base = (
    df_IdsPrvcyMbrRelshpExtr
    .withColumn(
        "SrcSysCdSk",
        GetFkeyCodes(
            "IDS",
            col("PRVCY_MBR_RELSHP_SK"),
            trim(lit("SOURCE SYSTEM")),
            trim(col("SRC_SYS_CD")),
            Logging
        )
    )
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn(
        "svMbrSk",
        GetFkeyMbr(
            col("SRC_SYS_CD"),
            col("PRVCY_MBR_RELSHP_SK"),
            col("MBR_SK"),
            Logging
        )
    )
    .withColumn(
        "svPrvcyMbrSrcCdSk",
        GetFkeyCodes(
            col("SRC_SYS_CD"),
            col("PRVCY_MBR_RELSHP_SK"),
            lit(" PRIVACY MEMBER SOURCE "),
            col("PRVCY_MBR_SRC_CD_SK"),
            Logging
        )
    )
    .withColumn(
        "svSrcSysLastUpdtDtSk",
        GetFkeyDate(
            "IDS",
            col("PRVCY_MBR_RELSHP_SK"),
            col("SRC_SYS_LAST_UPDT_DT_SK"),
            Logging
        )
    )
    .withColumn(
        "svSrcSysLastUpdtUserSk",
        GetFkeyAppUsr(
            col("SRC_SYS_CD"),
            col("PRVCY_MBR_RELSHP_SK"),
            col("SRC_SYS_LAST_UPDT_USER_SK"),
            Logging
        )
    )
    .withColumn(
        "svPrvcyExtrnlMbrSk",
        GetFkeyPrvcyExtrnlMbr(
            col("SRC_SYS_CD"),
            col("PRVCY_MBR_RELSHP_SK"),
            col("PRVCY_EXTRNL_MBR_SK"),
            Logging
        )
    )
    .withColumn(
        "svPrvcyMbrRelshpRepCdSk",
        GetFkeyCodes(
            col("SRC_SYS_CD"),
            col("PRVCY_MBR_RELSHP_SK"),
            lit(" PRIVACY MEMBER RELATIONSHIP REPRESENTATIVE "),
            col("PRVCY_MBR_RELSHP_REP_CD_SK"),
            Logging
        )
    )
    .withColumn(
        "svPrvcyPrsnlRepSk",
        GetFkeyPrvcyPrsnlRep(
            col("SRC_SYS_CD"),
            col("PRVCY_MBR_RELSHP_SK"),
            col("PRVCY_PRSNL_REP_SK"),
            Logging
        )
    )
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(
            col("PRVCY_MBR_RELSHP_SK")
        )
    )
)

df_Fkey = (
    df_ForeignKey_base
    .filter((col("ErrCount") == 0) | (col("PassThru") == lit("Y")))
    .select(
        col("PRVCY_MBR_RELSHP_SK").alias("PRVCY_MBR_RELSHP_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        col("SEQ_NO").alias("SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        when(col("PRVCY_MBR_SRC_CD_SK") == lit("F"), col("svMbrSk")).otherwise(lit(1)).alias("MBR_SK"),
        when(col("PRVCY_MBR_SRC_CD_SK") == lit("N"), col("svPrvcyExtrnlMbrSk")).otherwise(lit(1)).alias("PRVCY_EXTRNL_MBR_SK"),
        col("svPrvcyPrsnlRepSk").alias("PRVCY_PRSNL_REP_SK"),
        col("CRT_DT_SK").alias("CRT_DT_SK"),
        col("EFF_DT_SK").alias("EFF_DT_SK"),
        col("TERM_DT_SK").alias("TERM_DT_SK"),
        col("svPrvcyMbrRelshpRepCdSk").alias("PRVCY_MBR_RELSHP_REP_CD_SK"),
        col("svPrvcyMbrSrcCdSk").alias("PRVCY_MBR_SRC_CD_SK"),
        col("svSrcSysLastUpdtDtSk").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        col("svSrcSysLastUpdtUserSk").alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

df_Recycle = (
    df_ForeignKey_base
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("PRVCY_MBR_RELSHP_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("PRVCY_MBR_RELSHP_SK").alias("PRVCY_MBR_RELSHP_SK"),
        col("PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        col("SEQ_NO").alias("SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("MBR_SK").alias("MBR_SK"),
        col("PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
        col("PRVCY_PRSNL_REP_SK").alias("PRVCY_PRSNL_REP_SK"),
        col("CRT_DT_SK").alias("CRT_DT_SK"),
        col("EFF_DT_SK").alias("EFF_DT_SK"),
        col("TERM_DT_SK").alias("TERM_DT_SK"),
        col("PRVCY_MBR_RELSHP_REP_CD_SK").alias("PRVCY_MBR_RELSHP_REP_CD_SK"),
        col("PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK"),
        col("SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        col("SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

df_Recycle_rpad = (
    df_Recycle
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", rpad(col("TERM_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), 100, " "))
    .withColumn("PRI_KEY_STRING", rpad(col("PRI_KEY_STRING"), 100, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", rpad(col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " "))
)

col_order_recycle = [
    "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT","RECYCLE_CT",
    "SRC_SYS_CD","PRI_KEY_STRING","PRVCY_MBR_RELSHP_SK","PRVCY_MBR_UNIQ_KEY","SEQ_NO","CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK","MBR_SK","PRVCY_EXTRNL_MBR_SK","PRVCY_PRSNL_REP_SK","CRT_DT_SK","EFF_DT_SK",
    "TERM_DT_SK","PRVCY_MBR_RELSHP_REP_CD_SK","PRVCY_MBR_SRC_CD_SK","SRC_SYS_LAST_UPDT_DT_SK","SRC_SYS_LAST_UPDT_USER_SK"
]

df_Recycle_final = df_Recycle_rpad.select(col_order_recycle)

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

windowSpec = Window.orderBy(monotonically_increasing_id())
df_ForeignKey_base_rn = df_ForeignKey_base.withColumn("rownum", row_number().over(windowSpec))

df_DefaultUNK = (
    df_ForeignKey_base_rn
    .filter(col("rownum") == 1)
    .select(
        lit(0).alias("PRVCY_MBR_RELSHP_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit(0).alias("PRVCY_MBR_UNIQ_KEY"),
        lit(0).alias("SEQ_NO"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("MBR_SK"),
        lit(0).alias("PRVCY_EXTRNL_MBR_SK"),
        lit(0).alias("PRVCY_PRSNL_REP_SK"),
        lit("1753-01-01").alias("CRT_DT_SK"),
        lit("UNK").alias("EFF_DT_SK"),
        lit("UNK").alias("TERM_DT_SK"),
        lit(0).alias("PRVCY_MBR_RELSHP_REP_CD_SK"),
        lit(0).alias("PRVCY_MBR_SRC_CD_SK"),
        lit("UNK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        lit(0).alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

df_DefaultNA = (
    df_ForeignKey_base_rn
    .filter(col("rownum") == 1)
    .select(
        lit(1).alias("PRVCY_MBR_RELSHP_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit(1).alias("PRVCY_MBR_UNIQ_KEY"),
        lit(1).alias("SEQ_NO"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("MBR_SK"),
        lit(1).alias("PRVCY_EXTRNL_MBR_SK"),
        lit(1).alias("PRVCY_PRSNL_REP_SK"),
        lit("1753-01-01").alias("CRT_DT_SK"),
        lit("NA").alias("EFF_DT_SK"),
        lit("NA").alias("TERM_DT_SK"),
        lit(1).alias("PRVCY_MBR_RELSHP_REP_CD_SK"),
        lit(1).alias("PRVCY_MBR_SRC_CD_SK"),
        lit("NA").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        lit(1).alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

collector_cols = [
    "PRVCY_MBR_RELSHP_SK","SRC_SYS_CD_SK","PRVCY_MBR_UNIQ_KEY","SEQ_NO","CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK","MBR_SK","PRVCY_EXTRNL_MBR_SK","PRVCY_PRSNL_REP_SK","CRT_DT_SK",
    "EFF_DT_SK","TERM_DT_SK","PRVCY_MBR_RELSHP_REP_CD_SK","PRVCY_MBR_SRC_CD_SK","SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK"
]

df_Collector = (
    df_Fkey.select(collector_cols)
    .unionByName(df_DefaultUNK.select(collector_cols))
    .unionByName(df_DefaultNA.select(collector_cols))
)

df_Collector_rpad = (
    df_Collector
    .withColumn("EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", rpad(col("TERM_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", rpad(col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " "))
)

df_Collector_final = df_Collector_rpad.select(collector_cols)

write_files(
    df_Collector_final,
    f"{adls_path}/load/PRVCY_MBR_RELSHP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)