# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                                               Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------     -----------------------------------    ---------------------------------------------------------                                                            ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               03/27/2008          3255                         Original Programming                                                                                    devlEDWcur                   Steph Goddard           03/30/2008
# MAGIC 
# MAGIC Bhupinder Kaur                 09/27/2013   EDWefficiencies(5114)     To create a loadfile for EDW target table ITS_CLM_MSG_D                     EnterpriseWrhsDevl        Jag Yelavarthi             2013-12-22

# MAGIC JOB NAME: IdsEdwItsClmMsgDExtr
# MAGIC DB2 connector brings in data from IDS tables
# MAGIC ITS_CLM_MSG
# MAGIC CLM, 
# MAGIC W_EDW_ETL_DRVR
# MAGIC Data is written into a load ready SF file named as ITS_CLM_MSG_D.dat
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Key:
# MAGIC CLM_MSG_FMT_CD
# MAGIC Add Defaults and null handling
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_db2_ITS_CLM_MSG_In = f"""
SELECT
ITS_CLM_MSG.ITS_CLM_MSG_SK,
ITS_CLM_MSG.SRC_SYS_CD_SK,
ITS_CLM_MSG.CLM_ID,
ITS_CLM_MSG.ITS_CLM_MSG_FMT_CD_SK,
ITS_CLM_MSG.ITS_CLM_MSG_ID,
ITS_CLM_MSG.CRT_RUN_CYC_EXCTN_SK,
ITS_CLM_MSG.LAST_UPDT_RUN_CYC_EXCTN_SK,
ITS_CLM_MSG.ITS_CLM_SK,
ITS_CLM_MSG.ITS_CLM_MSG_DESC,
CLM.CLM_SK,
COALESCE(CM.TRGT_CD,'UNK') as SRC_SYS_CD
FROM
 {IDSOwner}.ITS_CLM_MSG ITS_CLM_MSG
LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CM
ON
ITS_CLM_MSG.SRC_SYS_CD_SK=CM.CD_MPPNG_SK
JOIN {IDSOwner}.CLM CLM
ON 
ITS_CLM_MSG.CLM_ID = CLM.CLM_ID  and ITS_CLM_MSG.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK
JOIN {IDSOwner}.W_EDW_ETL_DRVR DRVR
ON 
CLM.CLM_ID =DRVR.CLM_ID and CLM.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
"""

df_db2_ITS_CLM_MSG_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_ITS_CLM_MSG_In)
    .load()
)

extract_query_db2_CdMppng_In = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
 from {IDSOwner}.CD_MPPNG
"""

df_db2_CdMppng_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CdMppng_In)
    .load()
)

df_lkp_Codes = (
    df_db2_ITS_CLM_MSG_In.alias("lnk_IdsEdwItsClmMsgDExtr_InABC")
    .join(
        df_db2_CdMppng_In.alias("refClmMsgFmtCd"),
        F.col("lnk_IdsEdwItsClmMsgDExtr_InABC.ITS_CLM_MSG_FMT_CD_SK") == F.col("refClmMsgFmtCd.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwItsClmMsgDExtr_InABC.ITS_CLM_MSG_SK").alias("ITS_CLM_MSG_SK"),
        F.col("lnk_IdsEdwItsClmMsgDExtr_InABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnk_IdsEdwItsClmMsgDExtr_InABC.CLM_ID").alias("CLM_ID"),
        F.col("lnk_IdsEdwItsClmMsgDExtr_InABC.ITS_CLM_MSG_FMT_CD_SK").alias("ITS_CLM_MSG_FMT_CD_SK"),
        F.col("lnk_IdsEdwItsClmMsgDExtr_InABC.ITS_CLM_MSG_ID").alias("ITS_CLM_MSG_ID"),
        F.col("lnk_IdsEdwItsClmMsgDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwItsClmMsgDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwItsClmMsgDExtr_InABC.ITS_CLM_SK").alias("ITS_CLM_SK"),
        F.col("lnk_IdsEdwItsClmMsgDExtr_InABC.ITS_CLM_MSG_DESC").alias("ITS_CLM_MSG_DESC"),
        F.col("lnk_IdsEdwItsClmMsgDExtr_InABC.CLM_SK").alias("CLM_SK"),
        F.col("lnk_IdsEdwItsClmMsgDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("refClmMsgFmtCd.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.col("refClmMsgFmtCd.TRGT_CD").alias("TRGT_CD"),
        F.col("refClmMsgFmtCd.TRGT_CD_NM").alias("TRGT_CD_NM")
    )
)

df_lnk_FullData_OutABC = (
    df_lkp_Codes
    .filter((F.col("ITS_CLM_MSG_SK") != 0) & (F.col("ITS_CLM_MSG_SK") != 1))
    .select(
        F.col("ITS_CLM_MSG_SK").alias("ITS_CLM_MSG_SK"),
        F.when(F.col("SRC_SYS_CD").isNull() | (F.col("SRC_SYS_CD") == ""), "NA").otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.when(F.col("TRGT_CD").isNull() | (F.col("TRGT_CD") == ""), "NA").otherwise(F.col("TRGT_CD")).alias("ITS_CLM_MSG_FMT_CD"),
        F.col("ITS_CLM_MSG_ID").alias("ITS_CLM_MSG_ID"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("ITS_CLM_SK").alias("ITS_CLAIM_SK"),
        F.col("ITS_CLM_MSG_DESC").alias("ITS_CLM_MSG_DESC"),
        F.when(F.col("TRGT_CD_NM").isNull() | (F.col("TRGT_CD_NM") == ""), "NA").otherwise(F.col("TRGT_CD_NM")).alias("ITS_CLM_MSG_FMT_NM"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ITS_CLM_MSG_FMT_CD_SK").alias("ITS_CLM_MSG_FMT_CD_SK")
    )
)

df_lnk_NA_Out = spark.createDataFrame(
    [
        (1, 'NA', 'NA', 'NA', 'NA', '1753-01-01', '1753-01-01', 1, 1, None, 'NA', 100, 100, 1)
    ],
    T.StructType([
        T.StructField("ITS_CLM_MSG_SK", T.IntegerType(), True),
        T.StructField("SRC_SYS_CD", T.StringType(), True),
        T.StructField("CLM_ID", T.StringType(), True),
        T.StructField("ITS_CLM_MSG_FMT_CD", T.StringType(), True),
        T.StructField("ITS_CLM_MSG_ID", T.StringType(), True),
        T.StructField("CRT_RUN_CYC_EXCTN_DT_SK", T.StringType(), True),
        T.StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", T.StringType(), True),
        T.StructField("CLM_SK", T.IntegerType(), True),
        T.StructField("ITS_CLAIM_SK", T.IntegerType(), True),
        T.StructField("ITS_CLM_MSG_DESC", T.StringType(), True),
        T.StructField("ITS_CLM_MSG_FMT_NM", T.StringType(), True),
        T.StructField("CRT_RUN_CYC_EXCTN_SK", T.IntegerType(), True),
        T.StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", T.IntegerType(), True),
        T.StructField("ITS_CLM_MSG_FMT_CD_SK", T.IntegerType(), True)
    ])
)

df_lnk_UNK_Out = spark.createDataFrame(
    [
        (0, 'UNK', 'UNK', 'UNK', 'UNK', '1753-01-01', '1753-01-01', 0, 0, None, 'UNK', 100, 100, 0)
    ],
    T.StructType([
        T.StructField("ITS_CLM_MSG_SK", T.IntegerType(), True),
        T.StructField("SRC_SYS_CD", T.StringType(), True),
        T.StructField("CLM_ID", T.StringType(), True),
        T.StructField("ITS_CLM_MSG_FMT_CD", T.StringType(), True),
        T.StructField("ITS_CLM_MSG_ID", T.StringType(), True),
        T.StructField("CRT_RUN_CYC_EXCTN_DT_SK", T.StringType(), True),
        T.StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", T.StringType(), True),
        T.StructField("CLM_SK", T.IntegerType(), True),
        T.StructField("ITS_CLAIM_SK", T.IntegerType(), True),
        T.StructField("ITS_CLM_MSG_DESC", T.StringType(), True),
        T.StructField("ITS_CLM_MSG_FMT_NM", T.StringType(), True),
        T.StructField("CRT_RUN_CYC_EXCTN_SK", T.IntegerType(), True),
        T.StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", T.IntegerType(), True),
        T.StructField("ITS_CLM_MSG_FMT_CD_SK", T.IntegerType(), True)
    ])
)

df_Fnl_UNK_NA_data = df_lnk_NA_Out.unionByName(df_lnk_UNK_Out).unionByName(df_lnk_FullData_OutABC)

df_final = df_Fnl_UNK_NA_data.select(
    "ITS_CLM_MSG_SK",
    "SRC_SYS_CD",
    "CLM_ID",
    "ITS_CLM_MSG_FMT_CD",
    "ITS_CLM_MSG_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CLM_SK",
    "ITS_CLAIM_SK",
    "ITS_CLM_MSG_DESC",
    "ITS_CLM_MSG_FMT_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ITS_CLM_MSG_FMT_CD_SK"
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad("CRT_RUN_CYC_EXCTN_DT_SK", 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " ")
)

write_files(
    df_final,
    f"{adls_path}/load/ITS_CLM_MSG_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)