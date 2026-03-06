# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By:FacetsBCBSCustSvcIdCardPhnLoadSeq (FacetsBCBSCustSvcIdCardPhnMbrCntl)
# MAGIC  
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                   Project/                                                                                                                        Code                  Date
# MAGIC Developer            Date              Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -----------------------    ------------------   -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Santosh Bokka   2013-09-03     4930       Originally Programmed                                                                                 Kalyan Neelam    2013-10-29

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
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DoubleType,
    DateType
)
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
Logging = get_widget_value('Logging','')
InFile = get_widget_value('InFile','')
SourceSK = get_widget_value('SourceSK','')
RunDate = get_widget_value('RunDate','')
RunCycle = get_widget_value('RunCycle','')

schema_facetsbcbsclsplncustsvcidcardphnextr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DoubleType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CLS_PLN_CUST_SVC_IDCARD_PHN_SK", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("CLS_ID", StringType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("EFF_DT_SK", StringType(), False),
    StructField("CUST_SVC_ID_CARD_PHN_KEY", DoubleType(), False),
    StructField("CUST_SVC_ID_CARD_PHN_USE_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("TERM_DT_SK", DateType(), False),
    StructField("CLS_PLN_DTL_PROD_CAT_CD", StringType(), True)
])

df_facetsbcbsclsplncustsvcidcardphnextr = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_facetsbcbsclsplncustsvcidcardphnextr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_foreignkey_stagevars = (
    df_facetsbcbsclsplncustsvcidcardphnextr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svGrpSk", GetFkeyGrp("FACETS", col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"), col("GRP_ID"), Logging))
    .withColumn("svSubgrpSk", GetFkeySubgrp("FACETS", col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"), col("GRP_ID"), col("SUBGRP_ID"), Logging))
    .withColumn("svClsSk", GetFkeyCls("FACETS", col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"), col("GRP_ID"), col("CLS_ID"), Logging))
    .withColumn("svClsPlnSk", GetFkeyClsPln("FACETS", col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"), col("CLS_PLN_ID"), Logging))
    .withColumn("svClsPlnDtlProdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"), lit("CLASS PLAN PRODUCT CATEGORY"), col("CLS_PLN_DTL_PROD_CAT_CD"), Logging))
    .withColumn("svProdSk", GetFkeyProd("FACETS", col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"), col("PROD_ID"), Logging))
    .withColumn("svCustSvcIdCardPhnUseCd", GetFkeyCodes(col("SRC_SYS_CD"), col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"), lit("CUSTOMER SERVICE ID CARD PHONE USE"), col("CUST_SVC_ID_CARD_PHN_USE_CD"), Logging))
    .withColumn("svCustSvcIdCardPhnSk", GetFkeyCustSvcIdCardPhn(col("SRC_SYS_CD"), col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"), col("CUST_SVC_ID_CARD_PHN_KEY"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK")))
)

df_fkey = (
    df_foreignkey_stagevars
    .filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
    .select(
        col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK").alias("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"),
        lit(SourceSK).alias("SRC_SYS_CD_SK"),
        col("GRP_ID").alias("GRP_ID"),
        col("SUBGRP_ID").alias("SUBGRP_ID"),
        col("CLS_ID").alias("CLS_ID"),
        col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        col("PROD_ID").alias("PROD_ID"),
        col("EFF_DT_SK").alias("EFF_DT_SK"),
        col("CUST_SVC_ID_CARD_PHN_KEY").alias("CUST_SVC_ID_CARD_PHN_KEY"),
        col("CUST_SVC_ID_CARD_PHN_USE_CD").alias("CUST_SVC_ID_CARD_PHN_USE_CD"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svClsSk").alias("CLS_SK"),
        col("svClsPlnSk").alias("CLS_PLN_SK"),
        col("svCustSvcIdCardPhnSk").alias("CUST_SVC_ID_CARD_PHN_SK"),
        col("svGrpSk").alias("GRP_SK"),
        col("svProdSk").alias("PROD_SK"),
        col("svSubgrpSk").alias("SUBGRP_SK"),
        col("svClsPlnDtlProdSk").alias("CLS_PLN_DTL_PROD_CAT_CD_SK"),
        col("svCustSvcIdCardPhnUseCd").alias("CUST_SVC_ID_CARD_PHN_USE_CD_SK"),
        col("TERM_DT_SK").alias("TERM_DT_SK")
    )
)

df_recycle = (
    df_foreignkey_stagevars
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK").alias("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"),
        col("GRP_ID").alias("GRP_ID"),
        col("SUBGRP_ID").alias("SUBGRP_ID"),
        col("CLS_ID").alias("CLS_ID"),
        col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        col("PROD_ID").alias("PROD_ID"),
        col("EFF_DT_SK").alias("EFF_DT_SK"),
        col("CUST_SVC_ID_CARD_PHN_KEY").alias("CUST_SVC_ID_CARD_PHN_KEY"),
        col("CUST_SVC_ID_CARD_PHN_USE_CD").alias("CUST_SVC_ID_CARD_PHN_USE_CD"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("TERM_DT_SK").alias("TERM_DT_SK"),
        col("CLS_PLN_DTL_PROD_CAT_CD").alias("CLS_PLN_DTL_PROD_CAT_CD")
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
    df_foreignkey_stagevars
    .limit(1)
    .select(
        lit(0).alias("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("GRP_ID"),
        lit("UNK").alias("SUBGRP_ID"),
        lit("UNK").alias("CLS_ID"),
        lit("UNK").alias("CLS_PLN_ID"),
        lit("UNK").alias("PROD_ID"),
        lit("1753-01-01").alias("EFF_DT_SK"),
        lit(0).alias("CUST_SVC_ID_CARD_PHN_KEY"),
        lit("UNK").alias("CUST_SVC_ID_CARD_PHN_USE_CD"),
        lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("CLS_SK"),
        lit(0).alias("CLS_PLN_SK"),
        lit(0).alias("CUST_SVC_ID_CARD_PHN_SK"),
        lit(0).alias("GRP_SK"),
        lit(0).alias("PROD_SK"),
        lit(0).alias("SUBGRP_SK"),
        lit(0).alias("CLS_PLN_DTL_PROD_CAT_CD_SK"),
        lit(0).alias("CUST_SVC_ID_CARD_PHN_USE_CD_SK"),
        lit("2199-12-31").alias("TERM_DT_SK")
    )
)

df_defaultNA = (
    df_foreignkey_stagevars
    .limit(1)
    .select(
        lit(1).alias("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("GRP_ID"),
        lit("NA").alias("SUBGRP_ID"),
        lit("NA").alias("CLS_ID"),
        lit("NA").alias("CLS_PLN_ID"),
        lit("NA").alias("PROD_ID"),
        lit("1753-01-01").alias("EFF_DT_SK"),
        lit(1).alias("CUST_SVC_ID_CARD_PHN_KEY"),
        lit("NA").alias("CUST_SVC_ID_CARD_PHN_USE_CD"),
        lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("CLS_SK"),
        lit(1).alias("CLS_PLN_SK"),
        lit(1).alias("CUST_SVC_ID_CARD_PHN_SK"),
        lit(1).alias("GRP_SK"),
        lit(1).alias("PROD_SK"),
        lit(1).alias("SUBGRP_SK"),
        lit(1).alias("CLS_PLN_DTL_PROD_CAT_CD_SK"),
        lit(1).alias("CUST_SVC_ID_CARD_PHN_USE_CD_SK"),
        lit("2199-12-31").alias("TERM_DT_SK")
    )
)

df_collector = df_fkey.unionByName(df_defaultUNK).unionByName(df_defaultNA)

df_final = df_collector.select(
    col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"),
    col("SRC_SYS_CD_SK"),
    col("GRP_ID"),
    col("SUBGRP_ID"),
    col("CLS_ID"),
    col("CLS_PLN_ID"),
    col("PROD_ID"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    col("CUST_SVC_ID_CARD_PHN_KEY"),
    col("CUST_SVC_ID_CARD_PHN_USE_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("CUST_SVC_ID_CARD_PHN_SK"),
    col("GRP_SK"),
    col("PROD_SK"),
    col("SUBGRP_SK"),
    col("CLS_PLN_DTL_PROD_CAT_CD_SK"),
    col("CUST_SVC_ID_CARD_PHN_USE_CD_SK"),
    rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/CLS_PLN_CUST_SVC_ID_CARD_PHN.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)