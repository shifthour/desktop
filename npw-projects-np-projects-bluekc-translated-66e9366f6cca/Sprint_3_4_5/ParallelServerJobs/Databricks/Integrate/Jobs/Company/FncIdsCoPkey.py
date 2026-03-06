# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME : FncIdsCoPkey
# MAGIC 
# MAGIC CALLED BY: FncIdsCoCntl
# MAGIC 
# MAGIC PROCESSING: generates primary keys from the extracts of workday company file.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC     
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Raj Kommineni                 2020-05-26         5879                            Initial Programming                                               IntegrateDev2          Jaideep Mankala               06/11/2020

# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Read K_CO Table to pull the Natural Keys and the Skey.
# MAGIC Inner Join primary key info with table info
# MAGIC Left Join on Natural Keys
# MAGIC Transformed Data will land into a sequential file for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_db2_K_CO_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT CO_ID,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,CO_SK FROM {IDSOwner}.K_CO")
    .load()
)

schema_CO_PKEY = StructType([
    StructField("CO_ID", StringType(), False),
    StructField("SRC_SYS_CD", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CO_LGL_NM", StringType(), False),
    StructField("FULL_CO_MAIL_ADDR_TX", StringType(), False),
    StructField("CO_MAIL_ADDR_LN_1", StringType(), True),
    StructField("CO_MAIL_ADDR_LN_2", StringType(), True),
    StructField("CO_MAIL_ADDR_CITY_NM", StringType(), True),
    StructField("CO_MAIL_ADDR_ST_CD_SK", IntegerType(), True),
    StructField("CO_MAIL_ADDR_ZIP_CD_5", StringType(), True),
    StructField("CO_MAIL_ADDR_ZIP_CD_4", StringType(), True),
    StructField("NAIC_ID", StringType(), False),
    StructField("TAX_ID", StringType(), False)
])

df_CO_PKEY = (
    spark.read.format("csv")
    .option("header", "true")
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_CO_PKEY)
    .load(f"{adls_path}/key/IDS.CO.Extr.dat")
)

df_cp_pk_in = df_CO_PKEY
df_lnk_Transforms_Out = df_cp_pk_in.select(
    col("CO_ID").alias("CO_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)
df_Lnk_cp_Out = df_cp_pk_in.select(
    col("CO_ID").alias("CO_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CO_LGL_NM").alias("CO_LGL_NM"),
    col("FULL_CO_MAIL_ADDR_TX").alias("FULL_CO_MAIL_ADDR_TX"),
    col("CO_MAIL_ADDR_LN_1").alias("CO_MAIL_ADDR_LN_1"),
    col("CO_MAIL_ADDR_LN_2").alias("CO_MAIL_ADDR_LN_2"),
    col("CO_MAIL_ADDR_CITY_NM").alias("CO_MAIL_ADDR_CITY_NM"),
    col("CO_MAIL_ADDR_ST_CD_SK").alias("CO_MAIL_ADDR_ST_CD_SK"),
    col("CO_MAIL_ADDR_ZIP_CD_5").alias("CO_MAIL_ADDR_ZIP_CD_5"),
    col("CO_MAIL_ADDR_ZIP_CD_4").alias("CO_MAIL_ADDR_ZIP_CD_4"),
    col("NAIC_ID").alias("NAIC_ID"),
    col("TAX_ID").alias("TAX_ID")
)

df_rdup_Natural_Keys = dedup_sort(df_lnk_Transforms_Out, ["CO_ID", "SRC_SYS_CD"], [])

df_jn_Co = (
    df_rdup_Natural_Keys.alias("lnk_Natural_Keys_out")
    .join(
        df_db2_K_CO_in.alias("lnk_KCoPkey_out"),
        [
            col("lnk_Natural_Keys_out.CO_ID") == col("lnk_KCoPkey_out.CO_ID"),
            col("lnk_Natural_Keys_out.SRC_SYS_CD") == col("lnk_KCoPkey_out.SRC_SYS_CD")
        ],
        "left"
    )
    .select(
        col("lnk_Natural_Keys_out.CO_ID").alias("CO_ID"),
        col("lnk_Natural_Keys_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_KCoPkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnk_KCoPkey_out.CO_SK").alias("CO_SK")
    )
)

df_enriched = df_jn_Co.withColumn("is_co_sk_null", col("CO_SK").isNull())
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"CO_SK",<schema>,<secret_name>)
df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_SK_SK",
    when(col("is_co_sk_null"), lit(IDSRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
)
df_enriched = df_enriched.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK_SK", lit(IDSRunCycle))

df_lnk_Pkey_out = df_enriched.select(
    col("CO_ID"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK_SK"),
    col("CO_SK")
)

df_lnk_KCo_Out = (
    df_enriched.filter(col("is_co_sk_null"))
    .select(
        col("CO_ID"),
        col("SRC_SYS_CD"),
        col("CRT_RUN_CYC_EXCTN_SK_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("CO_SK")
    )
)

df_db2_K_CO_Load = df_lnk_KCo_Out
temp_table_db2_K_CO_Load = "STAGING.FncIdsCoPkey_db2_K_CO_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_db2_K_CO_Load}", jdbc_url, jdbc_props)
(
    df_db2_K_CO_Load.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_db2_K_CO_Load)
    .mode("overwrite")
    .save()
)
merge_sql_db2_K_CO_Load = f"""
MERGE INTO {IDSOwner}.K_CO AS T
USING {temp_table_db2_K_CO_Load} AS S
ON T.CO_ID = S.CO_ID AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.CO_SK = S.CO_SK
WHEN NOT MATCHED THEN
  INSERT (CO_ID,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,CO_SK)
  VALUES (S.CO_ID,S.SRC_SYS_CD,S.CRT_RUN_CYC_EXCTN_SK,S.CO_SK);
"""
execute_dml(merge_sql_db2_K_CO_Load, jdbc_url, jdbc_props)

df_jn_PKey = (
    df_Lnk_cp_Out.alias("Lnk_cp_Out")
    .join(
        df_lnk_Pkey_out.alias("lnk_Pkey_out"),
        [
            col("Lnk_cp_Out.CO_ID") == col("lnk_Pkey_out.CO_ID"),
            col("Lnk_cp_Out.SRC_SYS_CD") == col("lnk_Pkey_out.SRC_SYS_CD")
        ],
        "inner"
    )
    .select(
        col("lnk_Pkey_out.CO_SK").alias("CO_SK"),
        col("Lnk_cp_Out.CO_ID").alias("CO_ID"),
        col("Lnk_cp_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_Pkey_out.CRT_RUN_CYC_EXCTN_SK_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnk_Pkey_out.LAST_UPDT_RUN_CYC_EXCTN_SK_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("Lnk_cp_Out.CO_LGL_NM").alias("CO_LGL_NM"),
        col("Lnk_cp_Out.FULL_CO_MAIL_ADDR_TX").alias("FULL_CO_MAIL_ADDR_TX"),
        col("Lnk_cp_Out.CO_MAIL_ADDR_LN_1").alias("CO_MAIL_ADDR_LN_1"),
        col("Lnk_cp_Out.CO_MAIL_ADDR_LN_2").alias("CO_MAIL_ADDR_LN_2"),
        col("Lnk_cp_Out.CO_MAIL_ADDR_CITY_NM").alias("CO_MAIL_ADDR_CITY_NM"),
        col("Lnk_cp_Out.CO_MAIL_ADDR_ST_CD_SK").alias("CO_MAIL_ADDR_ST_CD_SK"),
        col("Lnk_cp_Out.CO_MAIL_ADDR_ZIP_CD_5").alias("CO_MAIL_ADDR_ZIP_CD_5"),
        col("Lnk_cp_Out.CO_MAIL_ADDR_ZIP_CD_4").alias("CO_MAIL_ADDR_ZIP_CD_4"),
        col("Lnk_cp_Out.NAIC_ID").alias("NAIC_ID"),
        col("Lnk_cp_Out.TAX_ID").alias("TAX_ID")
    )
)

df_seq_CO_LOAD = df_jn_PKey
df_seq_CO_LOAD = df_seq_CO_LOAD.withColumn("CO_ID", rpad("CO_ID", 255, " "))
df_seq_CO_LOAD = df_seq_CO_LOAD.withColumn("CO_LGL_NM", rpad("CO_LGL_NM", 255, " "))
df_seq_CO_LOAD = df_seq_CO_LOAD.withColumn("FULL_CO_MAIL_ADDR_TX", rpad("FULL_CO_MAIL_ADDR_TX", 255, " "))
df_seq_CO_LOAD = df_seq_CO_LOAD.withColumn("CO_MAIL_ADDR_LN_1", rpad("CO_MAIL_ADDR_LN_1", 255, " "))
df_seq_CO_LOAD = df_seq_CO_LOAD.withColumn("CO_MAIL_ADDR_LN_2", rpad("CO_MAIL_ADDR_LN_2", 255, " "))
df_seq_CO_LOAD = df_seq_CO_LOAD.withColumn("CO_MAIL_ADDR_CITY_NM", rpad("CO_MAIL_ADDR_CITY_NM", 255, " "))
df_seq_CO_LOAD = df_seq_CO_LOAD.withColumn("CO_MAIL_ADDR_ZIP_CD_5", rpad("CO_MAIL_ADDR_ZIP_CD_5", 5, " "))
df_seq_CO_LOAD = df_seq_CO_LOAD.withColumn("CO_MAIL_ADDR_ZIP_CD_4", rpad("CO_MAIL_ADDR_ZIP_CD_4", 4, " "))
df_seq_CO_LOAD = df_seq_CO_LOAD.withColumn("NAIC_ID", rpad("NAIC_ID", 255, " "))
df_seq_CO_LOAD = df_seq_CO_LOAD.withColumn("TAX_ID", rpad("TAX_ID", 255, " "))

final_select = df_seq_CO_LOAD.select(
    "CO_SK",
    "CO_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CO_LGL_NM",
    "FULL_CO_MAIL_ADDR_TX",
    "CO_MAIL_ADDR_LN_1",
    "CO_MAIL_ADDR_LN_2",
    "CO_MAIL_ADDR_CITY_NM",
    "CO_MAIL_ADDR_ST_CD_SK",
    "CO_MAIL_ADDR_ZIP_CD_5",
    "CO_MAIL_ADDR_ZIP_CD_4",
    "NAIC_ID",
    "TAX_ID"
)

write_files(
    final_select,
    f"{adls_path}/load/IDS.CO.Load.{RunID}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)