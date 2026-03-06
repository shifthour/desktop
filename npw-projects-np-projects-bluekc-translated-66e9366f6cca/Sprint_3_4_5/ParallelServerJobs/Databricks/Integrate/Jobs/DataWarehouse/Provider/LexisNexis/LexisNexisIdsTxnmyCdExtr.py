# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Calling Job Name :  LexisNexisTxnmyCdExtrSeq
# MAGIC 
# MAGIC                            
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                           Development Project               Code Reviewer                Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                    ----------------------------------              ---------------------------------       -------------------------
# MAGIC Ravi Abburi                      2017-11-07            5781 HEDIS                       Initial Programming                                                                    IntegrateDev2                   Kalyan Neelam                 2018-01-30

# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC JobName: LexisNexisIdsProvTxnmyCdXfm
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, substring, when, length, lit, upper, to_date
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
LexisNexis_ProvTxnmyCdFile = get_widget_value('LexisNexis_ProvTxnmyCdFile','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_Unstrcturd_TxnmyCd = StructType([
    StructField("CD", StringType(), True),
    StructField("GRPNG", StringType(), True),
    StructField("CLS", StringType(), True),
    StructField("SPEC", StringType(), True),
    StructField("DEFN", StringType(), True),
    StructField("EFF_DT", StringType(), True),
    StructField("DCTVTN_DT", StringType(), True),
    StructField("LAST_MOD_DT", StringType(), True),
    StructField("NOTE", StringType(), True)
])

df_Unstrcturd_TxnmyCd = (
    spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .schema(schema_Unstrcturd_TxnmyCd)
    .load(f"{adls_path_raw}/landing/{LexisNexis_ProvTxnmyCdFile}")
)

df_BusinessLogic = df_Unstrcturd_TxnmyCd.withColumn(
    "svTxnmyCdChk",
    when(length(trim(col("CD"))) < 10, lit("N")).otherwise(lit("Y"))
)

df_TxnmyCd_Y = (
    df_BusinessLogic
    .filter(col("svTxnmyCdChk") == "Y")
    .select(
        upper(trim(col("CD"))).alias("TXNMY_CD"),
        to_date(
            when(
                col("EFF_DT").isNull() | (trim(col("EFF_DT")) == ""), 
                lit("1753-01-01")
            ).otherwise(col("EFF_DT")),
            "yyyy-MM-dd"
        ).alias("EFF_DT"),
        to_date(
            when(
                col("DCTVTN_DT").isNull() | (trim(col("DCTVTN_DT")) == ""), 
                lit("2199-12-31")
            ).otherwise(col("DCTVTN_DT")),
            "yyyy-MM-dd"
        ).alias("DCTVTN_DT"),
        to_date(
            when(
                col("LAST_MOD_DT").isNull() | (trim(col("LAST_MOD_DT")) == ""), 
                lit("1753-01-01")
            ).otherwise(col("LAST_MOD_DT")),
            "yyyy-MM-dd"
        ).alias("LAST_UPDT_DT"),
        when(
            col("DEFN").isNull(), 
            lit("UNKNOWN DESCRIPTION")
        ).otherwise(upper(trim(col("DEFN")))).alias("TXNMY_DESC"),
        substring(upper(trim(col("CD"))), 1, 2).alias("TXNMY_PROV_TYP_CD"),
        when(
            col("GRPNG").isNull() | (trim(col("GRPNG")) == ""), 
            lit("UNKNOWN DESCRIPTION")
        ).otherwise(upper(trim(col("GRPNG")))).alias("TXNMY_PROV_TYP_DESC"),
        substring(upper(trim(col("CD"))), 1, 4).alias("TXNMY_CLS_CD"),
        when(
            col("CLS").isNull() | (trim(col("CLS")) == ""), 
            lit("UNKNOWN DESCRIPTION")
        ).otherwise(upper(trim(col("CLS")))).alias("TXNMY_CLS_DESC"),
        substring(upper(trim(col("CD"))), 5, 5).alias("TXNMY_SPCLIZATION_CD"),
        when(
            col("SPEC").isNull() | (trim(col("SPEC")) == ""), 
            lit("UNKNOWN DESCRIPTION")
        ).otherwise(upper(trim(col("SPEC")))).alias("TXNMY_SPCLIZATION_DESC"),
        lit(SrcSysCd).alias("CRT_SRC_SYS_CD"),
        lit(SrcSysCd).alias("LAST_UPDT_SRC_SYS_CD")
    )
)

df_TxnmyCd_N = (
    df_BusinessLogic
    .filter(col("svTxnmyCdChk") == "N")
    .select(
        col("CD"),
        col("GRPNG"),
        col("CLS"),
        col("SPEC"),
        col("DEFN"),
        col("EFF_DT"),
        col("DCTVTN_DT"),
        col("LAST_MOD_DT")
    )
)

write_files(
    df_TxnmyCd_N,
    f"{adls_path_raw}/landing/LexisNexisIdsTxnmyCdExtr.{SrcSysCd}.{RunID}_Rej.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT TXNMY_CD, PROV_SPEC_CD, PROV_FCLTY_TYP_CD FROM {IDSOwner}.P_TXNMY_CD_XREF"
df_db2_PTxnmyCdXref_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_lkp_Xref = df_TxnmyCd_Y.alias("Lnk_TxnmyCd_EE_In").join(
    df_db2_PTxnmyCdXref_Lkp.alias("PTxnmyCdXref_lkp"),
    col("Lnk_TxnmyCd_EE_In.TXNMY_CD") == col("PTxnmyCdXref_lkp.TXNMY_CD"),
    "left"
)

df_Xref_out = df_lkp_Xref.select(
    col("Lnk_TxnmyCd_EE_In.TXNMY_CD").alias("TXNMY_CD"),
    col("Lnk_TxnmyCd_EE_In.EFF_DT").alias("EFF_DT"),
    col("Lnk_TxnmyCd_EE_In.DCTVTN_DT").alias("DCTVTN_DT"),
    col("Lnk_TxnmyCd_EE_In.LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    col("Lnk_TxnmyCd_EE_In.TXNMY_DESC").alias("TXNMY_DESC"),
    col("Lnk_TxnmyCd_EE_In.TXNMY_PROV_TYP_CD").alias("TXNMY_PROV_TYP_CD"),
    col("Lnk_TxnmyCd_EE_In.TXNMY_PROV_TYP_DESC").alias("TXNMY_PROV_TYP_DESC"),
    col("Lnk_TxnmyCd_EE_In.TXNMY_CLS_CD").alias("TXNMY_CLS_CD"),
    col("Lnk_TxnmyCd_EE_In.TXNMY_CLS_DESC").alias("TXNMY_CLS_DESC"),
    col("Lnk_TxnmyCd_EE_In.TXNMY_SPCLIZATION_CD").alias("TXNMY_SPCLIZATION_CD"),
    col("Lnk_TxnmyCd_EE_In.TXNMY_SPCLIZATION_DESC").alias("TXNMY_SPCLIZATION_DESC"),
    col("Lnk_TxnmyCd_EE_In.CRT_SRC_SYS_CD").alias("CRT_SRC_SYS_CD"),
    col("Lnk_TxnmyCd_EE_In.LAST_UPDT_SRC_SYS_CD").alias("LAST_UPDT_SRC_SYS_CD"),
    col("PTxnmyCdXref_lkp.PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    col("PTxnmyCdXref_lkp.PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYP_CD")
)

write_files(
    df_Xref_out.select(
        "TXNMY_CD",
        "EFF_DT",
        "DCTVTN_DT",
        "LAST_UPDT_DT",
        "TXNMY_DESC",
        "TXNMY_PROV_TYP_CD",
        "TXNMY_PROV_TYP_DESC",
        "TXNMY_CLS_CD",
        "TXNMY_CLS_DESC",
        "TXNMY_SPCLIZATION_CD",
        "TXNMY_SPCLIZATION_DESC",
        "CRT_SRC_SYS_CD",
        "LAST_UPDT_SRC_SYS_CD",
        "PROV_SPEC_CD",
        "PROV_FCLTY_TYP_CD"
    ),
    f"{adls_path}/ds/TXNMY_CD.{SrcSysCd}.extr.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)