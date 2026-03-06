# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Ravi Abburi                      11/02/2017          5781                            Originally Programmed                           IntegrateDev2             Kalyan Neelam               2018-01-31


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, rpad
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
UWSOwner = get_widget_value("UWSOwner","")
uws_secret_name = get_widget_value("uws_secret_name","")

# Stage: UWS_TXNMY_CD_XREF_Out (ODBCConnectorPX)
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
df_UWS_TXNMY_CD_XREF_Out = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", f"SELECT TXNMY_CD, PROV_SPEC_CD, PROV_FCLTY_TYP_CD FROM {UWSOwner}.TXNMY_CD_XREF")
    .load()
)

# Stage: Xfrm_Business (CTransformerStage)
df_Xfrm_Business = df_UWS_TXNMY_CD_XREF_Out.select(
    col("TXNMY_CD").alias("TXNMY_CD"),
    when(
        (col("PROV_SPEC_CD").isNull()) | (trim(col("PROV_SPEC_CD")) == ""),
        lit("UNK")
    ).otherwise(col("PROV_SPEC_CD")).alias("PROV_SPEC_CD"),
    when(
        (col("PROV_FCLTY_TYP_CD").isNull()) | (trim(col("PROV_FCLTY_TYP_CD")) == ""),
        lit("UNK")
    ).otherwise(col("PROV_FCLTY_TYP_CD")).alias("PROV_FCLTY_TYP_CD")
)

# Prepare final DataFrame for DB write (adding rpad for char/varchar columns)
df_DB2_P_TXNMY_CD_XREF_Out_final = df_Xfrm_Business.select(
    rpad(col("TXNMY_CD"), <...>, " ").alias("TXNMY_CD"),
    rpad(col("PROV_SPEC_CD"), <...>, " ").alias("PROV_SPEC_CD"),
    rpad(col("PROV_FCLTY_TYP_CD"), <...>, " ").alias("PROV_FCLTY_TYP_CD")
)

# Stage: DB2_P_TXNMY_CD_XREF_Out (DB2ConnectorPX) - Merge into IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.UWSIDSPTxnmyCdXrefLoad_DB2_P_TXNMY_CD_XREF_Out_temp", jdbc_url_ids, jdbc_props_ids)

df_DB2_P_TXNMY_CD_XREF_Out_final.write.format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.UWSIDSPTxnmyCdXrefLoad_DB2_P_TXNMY_CD_XREF_Out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {IDSOwner}.P_TXNMY_CD_XREF AS T
USING STAGING.UWSIDSPTxnmyCdXrefLoad_DB2_P_TXNMY_CD_XREF_Out_temp AS S
ON T.TXNMY_CD = S.TXNMY_CD
WHEN MATCHED THEN UPDATE SET
    T.PROV_SPEC_CD = S.PROV_SPEC_CD,
    T.PROV_FCLTY_TYP_CD = S.PROV_FCLTY_TYP_CD
WHEN NOT MATCHED THEN
INSERT (TXNMY_CD, PROV_SPEC_CD, PROV_FCLTY_TYP_CD)
VALUES (S.TXNMY_CD, S.PROV_SPEC_CD, S.PROV_FCLTY_TYP_CD);
"""
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)

# Stage: seq_PROV_rej (PxSequentialFile) - Create reject DataFrame schema
schema_rejects = StructType([
    StructField("TXNMY_CD", StringType(), True),
    StructField("PROV_SPEC_CD", StringType(), True),
    StructField("PROV_FCLTY_TYP_CD", StringType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

# Emulate reject handling
df_seq_PROV_rej = spark.createDataFrame([], schema_rejects)

df_seq_PROV_rej_final = df_seq_PROV_rej.select(
    rpad(col("TXNMY_CD"), <...>, " ").alias("TXNMY_CD"),
    rpad(col("PROV_SPEC_CD"), <...>, " ").alias("PROV_SPEC_CD"),
    rpad(col("PROV_FCLTY_TYP_CD"), <...>, " ").alias("PROV_FCLTY_TYP_CD"),
    rpad(col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_PROV_rej_final,
    f"{adls_path}/load/P_TXNMY_CD_XREF.CMS_Rej.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)