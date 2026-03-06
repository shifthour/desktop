# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                   Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------              ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              07/02/2007         3264                              Originally Programmed                                devlEDW10            
# MAGIC 
# MAGIC Parikshith Chada              08/29/2007         3264                            Changed the Snapshot file Extract to            devlEDW10                 Steph Goddard            10/21/2007
# MAGIC                                                                                                          Snapshot table extract                  
# MAGIC Judy Reynolds                  11/18/2010       TTR_883                      Modified to assign default values to             EnterpriseWrhsDevl       Steph Goddard            11/22/2010
# MAGIC                                                                                                          datetime and date fields             
# MAGIC 
# MAGIC Rama Kamjula                 01/02/2013          5114                            Rewritten to Parallel version                      EnterpriseWrhsDevl         Bhoomi Dasari             2/26/2014

# MAGIC Loading Balancing Snapshot Table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_seq_IdsCustSvcTaskCstmDtl = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CUST_SVC_ID", StringType(), nullable=False),
    StructField("TASK_SEQ_NO", IntegerType(), nullable=False),
    StructField("CUST_SVC_TASK_CSTM_DTL_CD_SK", IntegerType(), nullable=False),
    StructField("CSTM_DTL_UNIQ_ID", TimestampType(), nullable=False),
    StructField("CSTM_DTL_SEQ_NO", IntegerType(), nullable=False),
    StructField("CSTM_DTL_DT_1_SK", StringType(), nullable=False),
    StructField("CSTM_DTL_MNY_1", DecimalType(38, 10), nullable=False),
    StructField("CSTM_DTL_DESC", StringType(), nullable=True)
])

df_seq_IdsCustSvcTaskCstmDtl = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_seq_IdsCustSvcTaskCstmDtl)
    .load(f"{adls_path}/balancing/snapshot/IDS_CUST_SVC_TASK_CSTM_DTL.uniq")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_Cd_MPPNG = f"SELECT  CD_MPPNG_SK, TRGT_CD, TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG"
df_db2_Cd_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Cd_MPPNG)
    .load()
)

df_lnk_SrcSysCd = df_db2_Cd_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lnk_CstmDtlCdLkup = df_db2_Cd_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Codes = (
    df_seq_IdsCustSvcTaskCstmDtl.alias("lnk_IdsCustSvcTaskCstmDtl")
    .join(
        df_lnk_SrcSysCd.alias("lnk_SrcSysCd"),
        F.col("lnk_IdsCustSvcTaskCstmDtl.SRC_SYS_CD_SK") == F.col("lnk_SrcSysCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_lnk_CstmDtlCdLkup.alias("lnk_CstmDtlCdLkup"),
        F.col("lnk_IdsCustSvcTaskCstmDtl.CUST_SVC_TASK_CSTM_DTL_CD_SK") == F.col("lnk_CstmDtlCdLkup.CD_MPPNG_SK"),
        "left"
    )
)

df_lnk_CustSvcTaskBckdtD = df_lkp_Codes.select(
    F.col("lnk_IdsCustSvcTaskCstmDtl.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnk_IdsCustSvcTaskCstmDtl.CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("lnk_IdsCustSvcTaskCstmDtl.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.col("lnk_IdsCustSvcTaskCstmDtl.CUST_SVC_TASK_CSTM_DTL_CD_SK").alias("CUST_SVC_TASK_CSTM_DTL_CD_SK"),
    F.col("lnk_IdsCustSvcTaskCstmDtl.CSTM_DTL_UNIQ_ID").alias("CSTM_DTL_UNIQ_ID"),
    F.col("lnk_IdsCustSvcTaskCstmDtl.CSTM_DTL_SEQ_NO").alias("CSTM_DTL_SEQ_NO"),
    F.col("lnk_SrcSysCd.TRGT_CD").alias("TRGT_CD_SrcSysCd"),
    F.col("lnk_CstmDtlCdLkup.TRGT_CD").alias("TRGT_CD"),
    F.col("lnk_IdsCustSvcTaskCstmDtl.CSTM_DTL_DT_1_SK").alias("CSTM_DTL_DT_1_SK")
)

df_xfm_BusinessLogic = df_lnk_CustSvcTaskBckdtD.select(
    F.when(
        (F.col("TRGT_CD_SrcSysCd").isNull()) | (F.length(F.trim(F.col("TRGT_CD_SrcSysCd"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("TRGT_CD_SrcSysCd")).alias("SRC_SYS_CD"),
    F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
    F.col("CSTM_DTL_SEQ_NO").alias("CUST_SVC_TASK_BCKDT_SEQ_NO"),
    F.when(
        (F.col("TRGT_CD").isNull()) | (F.length(F.trim(F.col("TRGT_CD"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("TRGT_CD")).alias("CUST_SVC_TASK_CSTM_DTL_CD"),
    F.when(
        (F.col("CSTM_DTL_UNIQ_ID").isNull())
        | (F.length(F.trim(F.col("CSTM_DTL_UNIQ_ID")).cast(StringType())) == 0)
        | (F.trim(F.col("CSTM_DTL_UNIQ_ID")).cast(StringType()) == F.lit("UNK"))
        | (F.trim(F.col("CSTM_DTL_UNIQ_ID")).cast(StringType()) == F.lit("UNK")),
        F.lit("1753-01-01 00:00:00.000000")
    ).otherwise(F.col("CSTM_DTL_UNIQ_ID")).alias("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID"),
    F.when(
        (F.col("CSTM_DTL_DT_1_SK").isNull())
        | (F.length(F.trim(F.col("CSTM_DTL_DT_1_SK"))) == 0)
        | (F.trim(F.col("CSTM_DTL_DT_1_SK")) == F.lit("UNK"))
        | (F.trim(F.col("CSTM_DTL_DT_1_SK")) == F.lit("NA")),
        F.lit("1753-01-01")
    ).otherwise(F.col("CSTM_DTL_DT_1_SK")).alias("BCKDT_DT")
)

df_seq_BCustSvcTaskBckdtD = df_xfm_BusinessLogic.select(
    F.col("SRC_SYS_CD"),
    F.col("CUST_SVC_ID"),
    F.col("CUST_SVC_TASK_SEQ_NO"),
    F.col("CUST_SVC_TASK_BCKDT_SEQ_NO"),
    F.col("CUST_SVC_TASK_CSTM_DTL_CD"),
    F.col("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID"),
    F.col("BCKDT_DT")
)

df_seq_BCustSvcTaskBckdtD = df_seq_BCustSvcTaskBckdtD.withColumn(
    "SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 255, " ")
).withColumn(
    "CUST_SVC_ID", F.rpad(F.col("CUST_SVC_ID"), 255, " ")
).withColumn(
    "CUST_SVC_TASK_CSTM_DTL_CD", F.rpad(F.col("CUST_SVC_TASK_CSTM_DTL_CD"), 255, " ")
).withColumn(
    "BCKDT_DT", F.rpad(F.col("BCKDT_DT"), 10, " ")
)

write_files(
    df_seq_BCustSvcTaskBckdtD,
    f"{adls_path}/load/B_CUST_SVC_TASK_BCKDT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)