# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                 Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------            ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              07/02/2007         3264                              Originally Programmed                             devlEDW10     
# MAGIC 
# MAGIC Parikshith Chada              08/29/2007         3264                            Changed the Snapshot file Extract to        devlEDW10                   Steph Goddard            10/21/2007
# MAGIC                                                                                                          Snapshot table extract                                   
# MAGIC 
# MAGIC Rama Kamjula                 01/02/2013          5114                            Rewritten to Parallel version                      EnterpriseWrhsDevl     Bhoomi Dasari               2/26/2014

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
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CUST_SVC_ID", StringType(), False),
    StructField("TASK_SEQ_NO", IntegerType(), False),
    StructField("CUST_SVC_TASK_CSTM_DTL_CD_SK", IntegerType(), False),
    StructField("CSTM_DTL_UNIQ_ID", TimestampType(), False),
    StructField("CSTM_DTL_SEQ_NO", IntegerType(), False),
    StructField("CSTM_DTL_DT_1_SK", StringType(), False),
    StructField("CSTM_DTL_MNY_1", DecimalType(10,2), False),
    StructField("CSTM_DTL_DESC", StringType(), True)
])

df_lnk_IdsCustSvcTaskCstmDtl = (
    spark.read
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_seq_IdsCustSvcTaskCstmDtl)
    .csv(f"{adls_path}/balancing/snapshot/IDS_CUST_SVC_TASK_CSTM_DTL.uniq")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids = f"SELECT CD_MPPNG_SK, TRGT_CD, TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG"
df_lnk_CdMppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids)
    .load()
)

df_lnk_SrcSysCd = df_lnk_CdMppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lnk_CstmDtlCdLkup = df_lnk_CdMppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lnk_CustSvcTaskDocD = (
    df_lnk_IdsCustSvcTaskCstmDtl.alias("p")
    .join(df_lnk_SrcSysCd.alias("ls"), F.col("p.SRC_SYS_CD_SK") == F.col("ls.CD_MPPNG_SK"), "left")
    .join(df_lnk_CstmDtlCdLkup.alias("lc"), F.col("p.CUST_SVC_TASK_CSTM_DTL_CD_SK") == F.col("lc.CD_MPPNG_SK"), "left")
    .select(
        F.col("p.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("p.CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("p.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
        F.col("p.CUST_SVC_TASK_CSTM_DTL_CD_SK").alias("CUST_SVC_TASK_CSTM_DTL_CD_SK"),
        F.col("p.CSTM_DTL_UNIQ_ID").alias("CSTM_DTL_UNIQ_ID"),
        F.col("p.CSTM_DTL_SEQ_NO").alias("CSTM_DTL_SEQ_NO"),
        F.col("ls.TRGT_CD").alias("TRGT_CD_SrcSysCd"),
        F.col("lc.TRGT_CD").alias("TRGT_CD"),
        F.col("p.CSTM_DTL_DESC").alias("CSTM_DTL_DESC")
    )
)

df_lnk_CustSvcTaskDocD_Out = (
    df_lnk_CustSvcTaskDocD
    .filter(F.col("TRGT_CD") == "DOCID")
    .withColumn("SRC_SYS_CD", F.col("TRGT_CD_SrcSysCd"))
    .withColumn("CUST_SVC_ID", F.col("CUST_SVC_ID"))
    .withColumn("CUST_SVC_TASK_SEQ_NO", F.col("TASK_SEQ_NO"))
    .withColumn(
        "CUST_SVC_TASK_CSTM_DTL_CD",
        F.when((F.col("TRGT_CD").isNull()) | (F.col("TRGT_CD") == ''), "NA").otherwise(F.col("TRGT_CD"))
    )
    .withColumn("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID", F.col("CSTM_DTL_UNIQ_ID"))
    .withColumn("CUST_SVC_TASK_DOC_SEQ_NO", F.col("CSTM_DTL_SEQ_NO"))
    .withColumn(
        "DOC_ID",
        F.when(
            (F.col("CSTM_DTL_DESC").isNull()) | (F.col("CSTM_DTL_DESC") == ''),
            "NA"
        ).otherwise(
            trim(F.col("CSTM_DTL_DESC"))
        )
    )
)

df_lnk_CustSvcTaskDocD_Out_final = df_lnk_CustSvcTaskDocD_Out.select(
    F.rpad(F.col("SRC_SYS_CD"), 50, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CUST_SVC_ID"), 50, " ").alias("CUST_SVC_ID"),
    F.col("CUST_SVC_TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
    F.rpad(F.col("CUST_SVC_TASK_CSTM_DTL_CD"), 50, " ").alias("CUST_SVC_TASK_CSTM_DTL_CD"),
    F.col("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID").alias("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID"),
    F.col("CUST_SVC_TASK_DOC_SEQ_NO").alias("CUST_SVC_TASK_DOC_SEQ_NO"),
    F.rpad(F.substring(F.col("DOC_ID"), 1, 20), 20, " ").alias("DOC_ID")
)

write_files(
    df_lnk_CustSvcTaskDocD_Out_final,
    f"{adls_path}/load/B_CUST_SVC_TASK_DOC_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)