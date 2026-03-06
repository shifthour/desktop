# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extracting data from APL_ACTVTY
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                2/08/2007           Appeals/3028              Originally Programmed                        devlEDW10                  Steph Goddard              09/15/2007
# MAGIC 
# MAGIC 
# MAGIC Raj Mangalampally         11-04-2013          5114                             Server to Parallel Conv                      EnterpriseWrhsDevl       Jag Yelavarthi               2014-01-17

# MAGIC Read data from source table APL and Extract record based on LAST_UPDT_RUN_CYC_EXCTN_SK
# MAGIC Lookup Keys
# MAGIC 
# MAGIC 1) APL_ACTVTY_CAT_CD_SK
# MAGIC Job Name : IdsEdwAplCatHistDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
EDWOwner = get_widget_value("EDWOwner","")
edw_secret_name = get_widget_value("edw_secret_name","")
ExtractRunCycle = get_widget_value("ExtractRunCycle","")
EDWRunCycleDate = get_widget_value("EDWRunCycleDate","")
EDWRunCycle = get_widget_value("EDWRunCycle","")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_db2_APL_CAT_HIST_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
        SELECT 
          APL.APL_SK,
          COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
          APL.APL_ID,
          APL.CRT_RUN_CYC_EXCTN_SK,
          APL.LAST_UPDT_RUN_CYC_EXCTN_SK,
          APL.APL_CAT_CD_SK
        FROM {IDSOwner}.APL APL
        LEFT JOIN {IDSOwner}.CD_MPPNG CD
          ON APL.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
        WHERE APL.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
        """
    )
    .load()
)

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
        SELECT 
          CD_MPPNG_SK,
          COALESCE(TRGT_CD,'NA') TRGT_CD,
          COALESCE(TRGT_CD_NM,'NA') TRGT_CD_NM
        FROM {IDSOwner}.CD_MPPNG
        """
    )
    .load()
)

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
df_db2_APL_CAT_HIST_D_Prev = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        f"""
        SELECT
          HIST.APL_SK,
          HIST.EDW_RCRD_STRT_DT_SK,
          HIST.SRC_SYS_CD,
          HIST.APL_ID,
          HIST.CRT_RUN_CYC_EXCTN_DT_SK,
          HIST.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
          HIST.EDW_CUR_RCRD_IN,
          HIST.EDW_RCRD_END_DT_SK,
          HIST.APL_CAT_CD,
          HIST.APL_CAT_NM,
          HIST.CRT_RUN_CYC_EXCTN_SK,
          HIST.LAST_UPDT_RUN_CYC_EXCTN_SK,
          'A' AS DUMMY
        FROM {EDWOwner}.APL_CAT_HIST_D HIST
        WHERE HIST.EDW_CUR_RCRD_IN='Y'
        """
    )
    .load()
)

df_lkp_CDMA_Codes = (
    df_db2_APL_CAT_HIST_D_in.alias("Ink_IdsEdwAplCatHistDExtr_inABC")
    .join(
        df_db2_CD_MPPNG_Extr.alias("Ref_AplCatCdSKLkup"),
        F.col("Ink_IdsEdwAplCatHistDExtr_inABC.APL_CAT_CD_SK") == F.col("Ref_AplCatCdSKLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_APL_CAT_HIST_D_Prev.alias("Ref_AplSKLkup"),
        F.col("Ink_IdsEdwAplCatHistDExtr_inABC.APL_SK") == F.col("Ref_AplSKLkup.APL_SK"),
        "left"
    )
)

df_lkp_CDMA_Codes = df_lkp_CDMA_Codes.select(
    F.col("Ink_IdsEdwAplCatHistDExtr_inABC.APL_SK").alias("APL_SK"),
    F.col("Ref_AplSKLkup.EDW_RCRD_STRT_DT_SK").alias("EDW_RCRD_STRT_DT_SK"),
    F.col("Ink_IdsEdwAplCatHistDExtr_inABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Ref_AplSKLkup.SRC_SYS_CD").alias("SRC_SYS_CD_PREV"),
    F.col("Ink_IdsEdwAplCatHistDExtr_inABC.APL_ID").alias("APL_ID"),
    F.col("Ref_AplSKLkup.APL_ID").alias("APL_ID_PREV"),
    F.col("Ref_AplCatCdSKLkup.TRGT_CD").alias("APL_CAT_CD"),
    F.col("Ref_AplCatCdSKLkup.TRGT_CD_NM").alias("APL_CAT_NM"),
    F.col("Ref_AplSKLkup.APL_CAT_CD").alias("APL_CAT_CD_PREV"),
    F.col("Ref_AplSKLkup.APL_CAT_NM").alias("APL_CAT_NM_PREV"),
    F.col("Ref_AplSKLkup.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK_PREV"),
    F.col("Ref_AplSKLkup.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK_PREV"),
    F.col("Ref_AplSKLkup.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Ref_AplSKLkup.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Ref_AplSKLkup.DUMMY").alias("DUMMY")
)

df_xfm = (
    df_lkp_CDMA_Codes
    .withColumn(
        "svNewRecordInd",
        F.when(F.col("DUMMY").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "svCreateNewRecord",
        F.when(F.col("APL_CAT_CD") != F.col("APL_CAT_CD_PREV"), F.lit("Y")).otherwise(F.lit("N"))
    )
)

df_xfm2 = df_xfm.withColumn("_row_num", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))

df_new = df_xfm2.filter(
    (F.col("svNewRecordInd") == 'Y') &
    (F.col("APL_SK") != 0) &
    (F.col("APL_SK") != 1)
).select(
    F.col("APL_SK").alias("APL_SK"),
    F.lit("1753-01-01").alias("EDW_RCRD_STRT_DT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("APL_ID").alias("APL_ID"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("Y").alias("EDW_CUR_RCRD_IN"),
    F.lit("2199-12-31").alias("EDW_RCRD_END_DT_SK"),
    F.when(trim(F.col("APL_CAT_CD")) == '', F.lit("NA")).otherwise(F.col("APL_CAT_CD")).alias("APL_CAT_CD"),
    F.when(trim(F.col("APL_CAT_NM")) == '', F.lit("NA")).otherwise(F.col("APL_CAT_NM")).alias("APL_CAT_NM"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_old = df_xfm2.filter(
    (F.col("svNewRecordInd") == 'N') &
    (F.col("svCreateNewRecord") == 'Y') &
    (F.col("APL_SK") != 0) &
    (F.col("APL_SK") != 1)
).select(
    F.col("APL_SK").alias("APL_SK"),
    F.col("EDW_RCRD_STRT_DT_SK").alias("EDW_RCRD_STRT_DT_SK"),
    F.col("SRC_SYS_CD_PREV").alias("SRC_SYS_CD"),
    F.col("APL_ID_PREV").alias("APL_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("N").alias("EDW_CUR_RCRD_IN"),
    FIND.DATE.EE(EDWRunCycleDate, -1, 'D', 'X', 'CCYY-MM-DD').alias("EDW_RCRD_END_DT_SK"),
    F.col("APL_CAT_CD_PREV").alias("APL_CAT_CD"),
    F.col("APL_CAT_NM_PREV").alias("APL_CAT_NM"),
    F.col("CRT_RUN_CYC_EXCTN_SK_PREV").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_updated = df_xfm2.filter(
    (F.col("svNewRecordInd") == 'N') &
    (F.col("svCreateNewRecord") == 'Y') &
    (F.col("APL_SK") != 0) &
    (F.col("APL_SK") != 1)
).select(
    F.col("APL_SK").alias("APL_SK"),
    F.lit(EDWRunCycleDate).alias("EDW_RCRD_STRT_DT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("APL_ID").alias("APL_ID"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("Y").alias("EDW_CUR_RCRD_IN"),
    F.lit("2199-12-31").alias("EDW_RCRD_END_DT_SK"),
    F.when(trim(F.col("APL_CAT_CD")) == '', F.lit("NA")).otherwise(F.col("APL_CAT_CD")).alias("APL_CAT_CD"),
    F.when(trim(F.col("APL_CAT_NM")) == '', F.lit("NA")).otherwise(F.col("APL_CAT_NM")).alias("APL_CAT_NM"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_unk = df_xfm2.filter(F.col("_row_num") == 1).select(
    F.lit(0).alias("APL_SK"),
    F.lit("1753-01-01").alias("EDW_RCRD_STRT_DT_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("APL_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("N").alias("EDW_CUR_RCRD_IN"),
    F.lit("1753-01-01").alias("EDW_RCRD_END_DT_SK"),
    F.lit("UNK").alias("APL_CAT_CD"),
    F.lit("UNK").alias("APL_CAT_NM"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_na = df_xfm2.filter(F.col("_row_num") == 1).select(
    F.lit(1).alias("APL_SK"),
    F.lit("1753-01-01").alias("EDW_RCRD_STRT_DT_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("APL_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("N").alias("EDW_CUR_RCRD_IN"),
    F.lit("1753-01-01").alias("EDW_RCRD_END_DT_SK"),
    F.lit("NA").alias("APL_CAT_CD"),
    F.lit("NA").alias("APL_CAT_NM"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_funnel = (
    df_na
    .unionByName(df_new)
    .unionByName(df_old)
    .unionByName(df_updated)
    .unionByName(df_unk)
)

df_funnel = df_funnel.withColumn(
    "EDW_RCRD_STRT_DT_SK", rpad(F.col("EDW_RCRD_STRT_DT_SK"), 10, " ")
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "EDW_CUR_RCRD_IN", rpad(F.col("EDW_CUR_RCRD_IN"), 1, " ")
).withColumn(
    "EDW_RCRD_END_DT_SK", rpad(F.col("EDW_RCRD_END_DT_SK"), 10, " ")
)

df_final = df_funnel.select(
    "APL_SK",
    "EDW_RCRD_STRT_DT_SK",
    "SRC_SYS_CD",
    "APL_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "EDW_CUR_RCRD_IN",
    "EDW_RCRD_END_DT_SK",
    "APL_CAT_CD",
    "APL_CAT_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/APL_CAT_HIST_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)