# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Sudeep Beereddy     2018-08- 17          5884                            Original Programming                                                                             IntegrateDev2            Abhiram Dasarathy        2018-09-12

# MAGIC get ntnl_prov_id and ntnl_prov_sk records from IDS.NTNL_PROV table.
# MAGIC get ntnl_prov_id and prov_sk from IDS.PROV table.
# MAGIC get ntnl_prov_id and prov_dea_sk from IDS.PROV_DEA table.
# MAGIC update PROV table with new ntnl_prov_sk value
# MAGIC update PROV_DEA table with new ntnl_prov_sk value
# MAGIC update CLM_PROV table with new ntnl_prov_sk value
# MAGIC For CLM_PROV table ntnl_prov_sk in (0,1) records - join CLM_PROV and PROV tables based on prov_id and get ntnl_prov_id from PROV table and clm_prov_sk from CLM_PROV table.
# MAGIC Drop ntntl_prov_id  in (UNK,NA)
# MAGIC Drop ntntl_prov_id  in (0,1)
# MAGIC Drop ntntl_prov_sk  in (0,1)
# MAGIC Drop ntntl_prov_sk  in (0,1)
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_PROV_In = f"SELECT NTNL_PROV_ID, PROV_SK FROM {IDSOwner}.PROV WHERE NTNL_PROV_SK IN (0,1)"
df_db2_PROV_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROV_In)
    .load()
)

extract_query_db2_PROV_DEA_In = f"SELECT NTNL_PROV_ID, PROV_DEA_SK FROM {IDSOwner}.PROV_DEA WHERE NTNL_PROV_SK IN (0,1)"
df_db2_PROV_DEA_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROV_DEA_In)
    .load()
)

extract_query_db2_NTNL_PROV_In = f"SELECT NTNL_PROV_ID, NTNL_PROV_SK FROM {IDSOwner}.NTNL_PROV WHERE NTNL_PROV_SK NOT IN (1,0)"
df_db2_NTNL_PROV_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_NTNL_PROV_In)
    .load()
)

df_cpy_npi_id_prov_join = df_db2_NTNL_PROV_In.select(
    col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("NTNL_PROV_SK").alias("NTNL_PROV_SK")
)

df_cpy_npi_id_prov_dea_join = df_db2_NTNL_PROV_In.select(
    col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("NTNL_PROV_SK").alias("NTNL_PROV_SK")
)

df_cpy_npi_id_clm_prov_join = df_db2_NTNL_PROV_In.select(
    col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("NTNL_PROV_SK").alias("NTNL_PROV_SK")
)

df_join_20 = df_db2_PROV_In.alias("input_Drvr").join(
    df_cpy_npi_id_prov_join.alias("prov_join"),
    on=["NTNL_PROV_ID"],
    how="left"
)
df_join_20_out = df_join_20.select(
    col("input_Drvr.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("input_Drvr.PROV_SK").alias("PROV_SK"),
    col("prov_join.NTNL_PROV_SK").alias("NTNL_PROV_SK")
)

df_xfmProvNpi_in = df_join_20_out
df_xfmProvNpi_stagevar = df_xfmProvNpi_in.withColumn(
    "svNtnlProvSk",
    when(col("NTNL_PROV_SK").isNull() | (col("NTNL_PROV_SK") == lit("0")), lit("0"))
    .otherwise(col("NTNL_PROV_SK"))
)
df_xfmProvNpi_out = df_xfmProvNpi_stagevar.filter(
    (col("svNtnlProvSk") != lit("0")) & (col("svNtnlProvSk") != lit("1"))
)
df_xfmProvNpi_out_select = df_xfmProvNpi_out.select(
    col("PROV_SK").alias("PROV_SK"),
    lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svNtnlProvSk").alias("NTNL_PROV_SK")
)
df_db2_PROV_Load_in = df_xfmProvNpi_out_select.select(
    rpad(col("PROV_SK").cast("string"), <...>, " ").alias("PROV_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast("string"), <...>, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("NTNL_PROV_SK").cast("string"), <...>, " ").alias("NTNL_PROV_SK")
)
execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsNtnlProvSkey_db2_PROV_Load_temp", jdbc_url, jdbc_props)
df_db2_PROV_Load_in.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable","STAGING.IdsNtnlProvSkey_db2_PROV_Load_temp").mode("overwrite").save()
merge_sql_db2_PROV_Load = f"""
MERGE INTO {IDSOwner}.PROV AS T
USING STAGING.IdsNtnlProvSkey_db2_PROV_Load_temp AS S
ON T.PROV_SK = S.PROV_SK
WHEN MATCHED THEN UPDATE SET
 T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
 T.NTNL_PROV_SK = S.NTNL_PROV_SK
WHEN NOT MATCHED THEN
INSERT (PROV_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, NTNL_PROV_SK)
VALUES (S.PROV_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.NTNL_PROV_SK);
"""
execute_dml(merge_sql_db2_PROV_Load, jdbc_url, jdbc_props)

df_join_30 = df_db2_PROV_DEA_In.alias("input_Drvr").join(
    df_cpy_npi_id_prov_dea_join.alias("prov_dea_join"),
    on=["NTNL_PROV_ID"],
    how="left"
)
df_join_30_out = df_join_30.select(
    col("input_Drvr.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("input_Drvr.PROV_DEA_SK").alias("PROV_DEA_SK"),
    col("prov_dea_join.NTNL_PROV_SK").alias("NTNL_PROV_SK")
)

df_provDea_xfmProvNpi_in = df_join_30_out
df_provDea_xfmProvNpi_stagevar = df_provDea_xfmProvNpi_in.withColumn(
    "svNtnlProvSk",
    when(col("NTNL_PROV_SK").isNull() | (col("NTNL_PROV_SK") == lit("0")), lit("0"))
    .otherwise(col("NTNL_PROV_SK"))
)
df_provDea_xfmProvNpi_out = df_provDea_xfmProvNpi_stagevar.filter(
    (col("svNtnlProvSk") != lit("0")) & (col("svNtnlProvSk") != lit("1"))
)
df_provDea_xfmProvNpi_out_select = df_provDea_xfmProvNpi_out.select(
    col("PROV_DEA_SK").alias("PROV_DEA_SK"),
    lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svNtnlProvSk").alias("NTNL_PROV_SK")
)
df_db2_PROV_dea_Load_in = df_provDea_xfmProvNpi_out_select.select(
    rpad(col("PROV_DEA_SK").cast("string"), <...>, " ").alias("PROV_DEA_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast("string"), <...>, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("NTNL_PROV_SK").cast("string"), <...>, " ").alias("NTNL_PROV_SK")
)
execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsNtnlProvSkey_db2_PROV_dea_Load_temp", jdbc_url, jdbc_props)
df_db2_PROV_dea_Load_in.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable","STAGING.IdsNtnlProvSkey_db2_PROV_dea_Load_temp").mode("overwrite").save()
merge_sql_db2_PROV_dea_Load = f"""
MERGE INTO {IDSOwner}.PROV_DEA AS T
USING STAGING.IdsNtnlProvSkey_db2_PROV_dea_Load_temp AS S
ON T.PROV_DEA_SK = S.PROV_DEA_SK
WHEN MATCHED THEN UPDATE SET
 T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
 T.NTNL_PROV_SK = S.NTNL_PROV_SK
WHEN NOT MATCHED THEN
INSERT (PROV_DEA_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, NTNL_PROV_SK)
VALUES (S.PROV_DEA_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.NTNL_PROV_SK);
"""
execute_dml(merge_sql_db2_PROV_dea_Load, jdbc_url, jdbc_props)

extract_query_db2_CLM_PROV = f"SELECT PROV.NTNL_PROV_ID, CLM.CLM_PROV_SK FROM {IDSOwner}.CLM_PROV CLM, {IDSOwner}.PROV PROV WHERE CLM.PROV_ID = PROV.PROV_ID AND CLM.NTNL_PROV_SK IN (1,0)"
df_db2_CLM_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_PROV)
    .load()
)

df_transformer_34_out = df_db2_CLM_PROV.select(
    col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("CLM_PROV_SK").alias("CLM_PROV_SK")
)

df_join_40 = df_transformer_34_out.alias("clm_drvr").join(
    df_cpy_npi_id_clm_prov_join.alias("clm_prov_Join"),
    on=["NTNL_PROV_ID"],
    how="left"
)
df_join_40_out = df_join_40.select(
    col("clm_drvr.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("clm_drvr.CLM_PROV_SK").alias("CLM_PROV_SK"),
    col("clm_prov_Join.NTNL_PROV_SK").alias("NTNL_PROV_SK")
)

df_clm_prov_xfmProvNpi_in = df_join_40_out
df_clm_prov_xfmProvNpi_stagevar = df_clm_prov_xfmProvNpi_in.withColumn(
    "svNtnlProvSk",
    when(col("NTNL_PROV_SK").isNull() | (col("NTNL_PROV_SK") == lit("0")), lit("0"))
    .otherwise(col("NTNL_PROV_SK"))
)
df_clm_prov_xfmProvNpi_out = df_clm_prov_xfmProvNpi_stagevar.filter(
    (col("svNtnlProvSk") != lit("0")) & (col("svNtnlProvSk") != lit("1"))
)
df_clm_prov_xfmProvNpi_out_select = df_clm_prov_xfmProvNpi_out.select(
    col("CLM_PROV_SK").alias("CLM_PROV_SK"),
    lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svNtnlProvSk").alias("NTNL_PROV_SK")
)
df_db2_CLM_PROV_Load_in = df_clm_prov_xfmProvNpi_out_select.select(
    rpad(col("CLM_PROV_SK").cast("string"), <...>, " ").alias("CLM_PROV_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast("string"), <...>, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("NTNL_PROV_SK").cast("string"), <...>, " ").alias("NTNL_PROV_SK")
)
execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsNtnlProvSkey_db2_CLM_PROV_Load_temp", jdbc_url, jdbc_props)
df_db2_CLM_PROV_Load_in.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable","STAGING.IdsNtnlProvSkey_db2_CLM_PROV_Load_temp").mode("overwrite").save()
merge_sql_db2_CLM_PROV_Load = f"""
MERGE INTO {IDSOwner}.CLM_PROV AS T
USING STAGING.IdsNtnlProvSkey_db2_CLM_PROV_Load_temp AS S
ON T.CLM_PROV_SK = S.CLM_PROV_SK
WHEN MATCHED THEN UPDATE SET
 T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
 T.NTNL_PROV_SK = S.NTNL_PROV_SK
WHEN NOT MATCHED THEN
INSERT (CLM_PROV_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, NTNL_PROV_SK)
VALUES (S.CLM_PROV_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.NTNL_PROV_SK);
"""
execute_dml(merge_sql_db2_CLM_PROV_Load, jdbc_url, jdbc_props)