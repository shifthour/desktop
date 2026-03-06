# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By:FacetsBCBSCustSvcIdCardPhnLoadSeq (FacetsBCBSCustSvcIdCardPhnMbrCntl)
# MAGIC  
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Santosh Bokka       2013-09-03                Initial programming                                                                               4390                IDSNEWDEVL                  Kalyan Neelam          2013-10-29

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
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","X")
InFile = get_widget_value("InFile","IdsCustSvcIdCardPhnExtr.CustSvcIdCardPhn.dat.20090512")
SourceSK = get_widget_value("SourceSK","99869")
RunCycle = get_widget_value("RunCycle","")

schema_FacetsBCBSCustSvcIdCardPhnExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(10,0), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CUST_SVC_ID_CARD_PHN_SK", IntegerType(), False),
    StructField("CUST_SVC_ID_CARD_PHN_KEY", DecimalType(10,0), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CUST_SVC_ID_CARD_PHN_NO_TYP_CD", IntegerType(), False),
    StructField("CUST_SVC_ID_CARD_PHN_UNLST_IN", StringType(), False),
    StructField("CUST_SVC_ID_CARD_PHN_AREA_CD", StringType(), False),
    StructField("CUST_SVC_ID_CARD_PHN_EXCH_NO", StringType(), False),
    StructField("CUST_SVC_ID_CARD_PHN_LN_NO", StringType(), False),
    StructField("CUST_SVC_ID_CARD_PHNEXT_NO", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_FRGN_NO", StringType(), True)
])

df_FacetsBCBSCustSvcIdCardPhnExtr = (
    spark.read.format("csv")
    .schema(schema_FacetsBCBSCustSvcIdCardPhnExtr)
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .load(f"{adls_path}/key/{InFile}")
)

df_foreignkey = (
    df_FacetsBCBSCustSvcIdCardPhnExtr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("CUST_SVC_ID_CARD_PHN_SK")))
    .withColumn(
        "svCustSvcIdCardPhnNoTypCd",
        GetFkeyCodes(
            col("SRC_SYS_CD"),
            col("CUST_SVC_ID_CARD_PHN_SK"),
            lit("CUSTOMER SERVICE ID CARD PHONE NUMBER TYPE"),
            col("CUST_SVC_ID_CARD_PHN_NO_TYP_CD"),
            lit(Logging)
        )
    )
    .withColumn("SourceSK", lit(SourceSK).cast("int"))
)

df_fkey = (
    df_foreignkey
    .filter((col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y")))
    .select(
        col("CUST_SVC_ID_CARD_PHN_SK"),
        col("SourceSK").alias("SRC_SYS_CD_SK"),
        col("CUST_SVC_ID_CARD_PHN_KEY"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svCustSvcIdCardPhnNoTypCd").alias("CUST_SVC_ID_CARD_PHN_NO_TYP_CD"),
        col("CUST_SVC_ID_CARD_PHN_UNLST_IN"),
        col("CUST_SVC_ID_CARD_PHN_AREA_CD"),
        col("CUST_SVC_ID_CARD_PHN_EXCH_NO"),
        col("CUST_SVC_ID_CARD_PHN_LN_NO"),
        col("CUST_SVC_ID_CARD_PHNEXT_NO"),
        col("CUST_SVC_ID_CARD_PHN_FRGN_NO")
    )
)

df_recycle_pre = df_foreignkey.filter(col("ErrCount") > lit(0))
df_recycle = (
    df_recycle_pre
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(col("CUST_SVC_ID_CARD_PHN_SK")))
    .withColumn("RECYCLE_CT", col("RECYCLE_CT") + lit(1))
    .select(
        col("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD"),
        col("DISCARD_IN"),
        col("PASS_THRU_IN"),
        col("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        col("RECYCLE_CT"),
        col("SRC_SYS_CD"),
        col("PRI_KEY_STRING"),
        col("CUST_SVC_ID_CARD_PHN_SK"),
        col("CUST_SVC_ID_CARD_PHN_KEY"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CUST_SVC_ID_CARD_PHN_NO_TYP_CD"),
        col("CUST_SVC_ID_CARD_PHN_UNLST_IN"),
        col("CUST_SVC_ID_CARD_PHN_AREA_CD"),
        col("CUST_SVC_ID_CARD_PHN_EXCH_NO"),
        col("CUST_SVC_ID_CARD_PHN_LN_NO"),
        col("CUST_SVC_ID_CARD_PHNEXT_NO"),
        col("CUST_SVC_ID_CARD_PHN_FRGN_NO")
    )
)

write_files(
    df_recycle,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_defaultUNK = (
    df_foreignkey
    .limit(1)
    .select(
        lit(0).cast("int").alias("CUST_SVC_ID_CARD_PHN_SK"),
        lit(0).cast("int").alias("SRC_SYS_CD_SK"),
        lit(0).cast("decimal(10,0)").alias("CUST_SVC_ID_CARD_PHN_KEY"),
        lit(RunCycle).cast("int").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(RunCycle).cast("int").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).cast("int").alias("CUST_SVC_ID_CARD_PHN_NO_TYP_CD"),
        lit("U").alias("CUST_SVC_ID_CARD_PHN_UNLST_IN"),
        lit("UNK").alias("CUST_SVC_ID_CARD_PHN_AREA_CD"),
        lit("UNK").alias("CUST_SVC_ID_CARD_PHN_EXCH_NO"),
        lit("UNK").alias("CUST_SVC_ID_CARD_PHN_LN_NO"),
        lit("UNK").alias("CUST_SVC_ID_CARD_PHNEXT_NO"),
        lit("UNK").alias("CUST_SVC_ID_CARD_PHN_FRGN_NO")
    )
)

df_defaultNA = (
    df_foreignkey
    .limit(1)
    .select(
        lit(1).cast("int").alias("CUST_SVC_ID_CARD_PHN_SK"),
        lit(1).cast("int").alias("SRC_SYS_CD_SK"),
        lit(1).cast("decimal(10,0)").alias("CUST_SVC_ID_CARD_PHN_KEY"),
        lit(RunCycle).cast("int").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(RunCycle).cast("int").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).cast("int").alias("CUST_SVC_ID_CARD_PHN_NO_TYP_CD"),
        lit("X").alias("CUST_SVC_ID_CARD_PHN_UNLST_IN"),
        lit("NA").alias("CUST_SVC_ID_CARD_PHN_AREA_CD"),
        lit("NA").alias("CUST_SVC_ID_CARD_PHN_EXCH_NO"),
        lit("NA").alias("CUST_SVC_ID_CARD_PHN_LN_NO"),
        lit("NA").alias("CUST_SVC_ID_CARD_PHNEXT_NO"),
        lit("NA").alias("CUST_SVC_ID_CARD_PHN_FRGN_NO")
    )
)

df_collector = df_fkey.unionByName(df_defaultUNK).unionByName(df_defaultNA)

df_collector_final = (
    df_collector
    .select(
        col("CUST_SVC_ID_CARD_PHN_SK"),
        col("SRC_SYS_CD_SK"),
        col("CUST_SVC_ID_CARD_PHN_KEY"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CUST_SVC_ID_CARD_PHN_NO_TYP_CD"),
        rpad(col("CUST_SVC_ID_CARD_PHN_UNLST_IN"), 1, " ").alias("CUST_SVC_ID_CARD_PHN_UNLST_IN"),
        rpad(col("CUST_SVC_ID_CARD_PHN_AREA_CD"), 3, " ").alias("CUST_SVC_ID_CARD_PHN_AREA_CD"),
        rpad(col("CUST_SVC_ID_CARD_PHN_EXCH_NO"), 3, " ").alias("CUST_SVC_ID_CARD_PHN_EXCH_NO"),
        rpad(col("CUST_SVC_ID_CARD_PHN_LN_NO"), 4, " ").alias("CUST_SVC_ID_CARD_PHN_LN_NO"),
        rpad(col("CUST_SVC_ID_CARD_PHNEXT_NO"), 5, " ").alias("CUST_SVC_ID_CARD_PHNEXT_NO"),
        rpad(col("CUST_SVC_ID_CARD_PHN_FRGN_NO"), 22, " ").alias("CUST_SVC_ID_CARD_PHN_FRGN_NO")
    )
)

write_files(
    df_collector_final,
    f"{adls_path}/load/CUST_SVC_ID_CARD_PHN.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)