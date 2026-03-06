# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: EdwProductExtrSeq
# MAGIC 
# MAGIC PROCESSING: Extracts all IDS records and loads in to EDW EVT_LOC_D table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                    Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------              ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 08/14/2009         4113                          Originally Programmed                                    devlEDWnew                Steph Goddard             08/19/2009
# MAGIC 
# MAGIC Archana Palivela             06/04/2013        5114                           Originally Programmed (In Parallel)                  EnterpriseWhseDevl     Jag Yelavarthi              2014-01-16

# MAGIC Job name:IdsEdwEvtLocDExtr
# MAGIC 
# MAGIC 
# MAGIC Job Extract data from IDS table and apply Fkey and Code denormalization, create a load ready file.
# MAGIC Read data from source table EVT_LOC
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC EVT_LOC_ST_CD
# MAGIC Write EVT_LOC_D Data into a Sequential file for Load Ready Job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IdsRunCycle = get_widget_value('IdsRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_EVT_LOC_Extr = f"""
SELECT 
EVT_LOC.EVT_LOC_SK,
EVT_LOC.EVT_LOC_ID,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
EVT_LOC.LAST_UPDT_RUN_CYC_EXCTN_SK,
EVT_LOC.EFF_DT_SK,
EVT_LOC.TERM_DT_SK,
EVT_LOC.EVT_LOC_CNTCT_NM,
EVT_LOC.EVT_LOC_NM,
EVT_LOC.EVT_LOC_ADDR_LN_1,
EVT_LOC.EVT_LOC_ROOM_ID,
EVT_LOC.EVT_LOC_CITY_NM,
EVT_LOC.EVT_LOC_ST_CD_SK,
EVT_LOC.EVT_LOC_ZIP_CD,
EVT_LOC.EVT_LOC_PHN_NO,
EVT_LOC.LAST_UPDT_DTM,
EVT_LOC.LAST_UPDT_USER_ID
FROM {IDSOwner}.EVT_LOC EVT_LOC
LEFT JOIN {IDSOwner}.CD_MPPNG CD
  ON EVT_LOC.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE EVT_LOC.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IdsRunCycle}
"""

df_db2_EVT_LOC_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_EVT_LOC_Extr)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_lkp_Codes = (
    df_db2_EVT_LOC_Extr.alias("Ink_IdsEdwEvtLocDExtr_InABC")
    .join(
        df_db2_CD_MPPNG_Extr.alias("Ref_BnfVndrTyp"),
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.EVT_LOC_ST_CD_SK") == F.col("Ref_BnfVndrTyp.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.EVT_LOC_SK").alias("EVT_LOC_SK"),
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.EVT_LOC_ID").alias("EVT_LOC_ID"),
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.EFF_DT_SK").alias("EVT_LOC_EFF_DT_SK"),
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.LAST_UPDT_DTM").alias("EVT_LOC_LAST_UPDT_DTM"),
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.TERM_DT_SK").alias("EVT_LOC_TERM_DT_SK"),
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.EVT_LOC_CNTCT_NM").alias("EVT_LOC_CNTCT_NM"),
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.EVT_LOC_NM").alias("EVT_LOC_NM"),
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.EVT_LOC_ADDR_LN_1").alias("EVT_LOC_ADDR_LN_1"),
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.EVT_LOC_ROOM_ID").alias("EVT_LOC_ROOM_ID"),
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.EVT_LOC_CITY_NM").alias("EVT_LOC_CITY_NM"),
        F.col("Ref_BnfVndrTyp.TRGT_CD").alias("EVT_LOC_ST_CD"),
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.EVT_LOC_ZIP_CD").alias("EVT_LOC_ZIP_CD"),
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.EVT_LOC_PHN_NO").alias("EVT_LOC_PHN_NO"),
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.LAST_UPDT_USER_ID").alias("EVT_LOC_LAST_UPDT_USER_ID"),
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.EVT_LOC_ST_CD_SK").alias("EVT_LOC_ST_CD_SK"),
        F.col("Ink_IdsEdwEvtLocDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_businessLogic_in = df_lkp_Codes.withColumnRenamed("LAST_UPDT_RUN_CYC_EXCTN_SK", "ORIG_LAST_UPDT_RUN_CYC_EXCTN_SK")

df_Lnk_Main_OutABC = (
    df_businessLogic_in
    .where((F.col("EVT_LOC_SK") != 0) & (F.col("EVT_LOC_SK") != 1))
    .select(
        F.col("EVT_LOC_SK").alias("EVT_LOC_SK"),
        F.col("EVT_LOC_ID").alias("EVT_LOC_ID"),
        F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("EVT_LOC_EFF_DT_SK").alias("EVT_LOC_EFF_DT_SK"),
        F.col("EVT_LOC_LAST_UPDT_DTM").alias("EVT_LOC_LAST_UPDT_DTM"),
        F.col("EVT_LOC_TERM_DT_SK").alias("EVT_LOC_TERM_DT_SK"),
        F.col("EVT_LOC_CNTCT_NM").alias("EVT_LOC_CNTCT_NM"),
        F.col("EVT_LOC_NM").alias("EVT_LOC_NM"),
        F.col("EVT_LOC_ADDR_LN_1").alias("EVT_LOC_ADDR_LN_1"),
        F.col("EVT_LOC_ROOM_ID").alias("EVT_LOC_ROOM_ID"),
        F.col("EVT_LOC_CITY_NM").alias("EVT_LOC_CITY_NM"),
        F.when(F.trim(F.col("EVT_LOC_ST_CD")) == "", F.lit("UNK")).otherwise(F.col("EVT_LOC_ST_CD")).alias("EVT_LOC_ST_CD"),
        F.col("EVT_LOC_ZIP_CD").alias("EVT_LOC_ZIP_CD"),
        F.col("EVT_LOC_PHN_NO").alias("EVT_LOC_PHN_NO"),
        F.col("EVT_LOC_LAST_UPDT_USER_ID").alias("EVT_LOC_LAST_UPDT_USER_ID"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ORIG_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("EVT_LOC_ST_CD_SK").alias("EVT_LOC_ST_CD_SK")
    )
)

df_Lnk_UNK_Out = spark.createDataFrame(
    [
        (
            0, 'UNK', 'UNK', '1753-01-01', '1753-01-01', '1753-01-01',
            '1753-01-01 00:00:00.000', '1753-01-01', 'UNK', 'UNK', '',
            'UNK', 'UNK', 'UNK', 'UNK', 'UNK', '', 'UNK', 100, 100, 100, 0
        )
    ],
    StructType([
        StructField("EVT_LOC_SK", IntegerType(), True),
        StructField("EVT_LOC_ID", StringType(), True),
        StructField("SRC_SYS_CD", StringType(), True),
        StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
        StructField("EVT_LOC_EFF_DT_SK", StringType(), True),
        StructField("EVT_LOC_LAST_UPDT_DTM", StringType(), True),
        StructField("EVT_LOC_TERM_DT_SK", StringType(), True),
        StructField("EVT_LOC_CNTCT_NM", StringType(), True),
        StructField("EVT_LOC_NM", StringType(), True),
        StructField("EVT_LOC_ADDR_LN_1", StringType(), True),
        StructField("EVT_LOC_ROOM_ID", StringType(), True),
        StructField("EVT_LOC_CITY_NM", StringType(), True),
        StructField("EVT_LOC_ST_CD", StringType(), True),
        StructField("EVT_LOC_ZIP_CD", StringType(), True),
        StructField("EVT_LOC_PHN_NO", StringType(), True),
        StructField("EVT_LOC_LAST_UPDT_USER_ID", StringType(), True),
        StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("EVT_LOC_ST_CD_SK", IntegerType(), True)
    ])
)

df_Lnk_NA_Out = spark.createDataFrame(
    [
        (
            1, 'NA', 'NA', '1753-01-01', '1753-01-01', '1753-01-01',
            '1753-01-01 00:00:00.000', '1753-01-01', 'NA', 'NA', '',
            'NA', 'NA', 'NA', 'NA', 'NA', '', 'NA', 100, 100, 100, 1
        )
    ],
    StructType([
        StructField("EVT_LOC_SK", IntegerType(), True),
        StructField("EVT_LOC_ID", StringType(), True),
        StructField("SRC_SYS_CD", StringType(), True),
        StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
        StructField("EVT_LOC_EFF_DT_SK", StringType(), True),
        StructField("EVT_LOC_LAST_UPDT_DTM", StringType(), True),
        StructField("EVT_LOC_TERM_DT_SK", StringType(), True),
        StructField("EVT_LOC_CNTCT_NM", StringType(), True),
        StructField("EVT_LOC_NM", StringType(), True),
        StructField("EVT_LOC_ADDR_LN_1", StringType(), True),
        StructField("EVT_LOC_ROOM_ID", StringType(), True),
        StructField("EVT_LOC_CITY_NM", StringType(), True),
        StructField("EVT_LOC_ST_CD", StringType(), True),
        StructField("EVT_LOC_ZIP_CD", StringType(), True),
        StructField("EVT_LOC_PHN_NO", StringType(), True),
        StructField("EVT_LOC_LAST_UPDT_USER_ID", StringType(), True),
        StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("EVT_LOC_ST_CD_SK", IntegerType(), True)
    ])
)

df_Funnel_37 = df_Lnk_Main_OutABC.unionByName(df_Lnk_UNK_Out).unionByName(df_Lnk_NA_Out)

df_final = df_Funnel_37.select(
    F.col("EVT_LOC_SK"),
    F.col("EVT_LOC_ID"),
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("EVT_LOC_EFF_DT_SK"), 10, " ").alias("EVT_LOC_EFF_DT_SK"),
    F.col("EVT_LOC_LAST_UPDT_DTM"),
    F.rpad(F.col("EVT_LOC_TERM_DT_SK"), 10, " ").alias("EVT_LOC_TERM_DT_SK"),
    F.col("EVT_LOC_CNTCT_NM"),
    F.col("EVT_LOC_NM"),
    F.col("EVT_LOC_ADDR_LN_1"),
    F.col("EVT_LOC_ROOM_ID"),
    F.col("EVT_LOC_CITY_NM"),
    F.col("EVT_LOC_ST_CD"),
    F.rpad(F.col("EVT_LOC_ZIP_CD"), 5, " ").alias("EVT_LOC_ZIP_CD"),
    F.col("EVT_LOC_PHN_NO"),
    F.col("EVT_LOC_LAST_UPDT_USER_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("EVT_LOC_ST_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/EVT_LOC_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)