# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela     05/30/2013        5114                              Originally Programmed  (In Parallel)                                                       EnterpriseWhseDevl      Bhoomi Dasari           8/11/2013
# MAGIC 
# MAGIC Aishwarya                03/15/2016        5600                                        DRUG_NPRFR_SPEC_COINS_PCT                                        EnterpriseDevl                Jag Yelavarthi           2016-04-06
# MAGIC                                                                                                            columnn added in the target

# MAGIC Left Outer Join Here
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC if there is no FKEY needed, just land it to Seq File for the load job follows.
# MAGIC Table K_PROD_COINS_SUM_F.               Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

# Stage: ds_ProdCoinsSumFExtr (PxDataSet)
df_ds_ProdCoinsSumFExtr = spark.read.parquet(f"{adls_path}/ds/PROD_COINS_SUM_F.parquet")
df_ds_ProdCoinsSumFExtr = df_ds_ProdCoinsSumFExtr.select(
    "PROD_COINS_SUM_SK",
    "SRC_SYS_CD",
    "PROD_ID",
    "PROD_CMPNT_EFF_DT_SK",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "PROD_SK",
    "PROD_CMPNT_TERM_DT_SK",
    "DRUG_GNRC_COINS_PCT",
    "DRUG_NM_BRND_COINS_PCT",
    "DRUG_PRFRD_COINS_PCT",
    "DRUG_NPRFR_COINS_PCT",
    "IN_HOSP_IN_OUT_NTWK_COINS_PCT",
    "IN_NTWK_COINS_PCT",
    "OUT_NTWK_COINS_PCT",
    "RTN_IN_NTWK_COINS_PCT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "DRUG_NPRFR_SPEC_COINS_PCT"
)

# Stage: cpy_MultiStreams (PxCopy)
df_lnkRemDupDataIn = df_ds_ProdCoinsSumFExtr.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK")
)

df_lnkFullDataJnIn = df_ds_ProdCoinsSumFExtr.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
    F.col("DRUG_GNRC_COINS_PCT").alias("DRUG_GNRC_COINS_PCT"),
    F.col("DRUG_NM_BRND_COINS_PCT").alias("DRUG_NM_BRND_COINS_PCT"),
    F.col("DRUG_PRFRD_COINS_PCT").alias("DRUG_PRFRD_COINS_PCT"),
    F.col("DRUG_NPRFR_COINS_PCT").alias("DRUG_NPRFR_COINS_PCT"),
    F.col("IN_HOSP_IN_OUT_NTWK_COINS_PCT").alias("IN_HOSP_IN_OUT_NTWK_COINS_PCT"),
    F.col("IN_NTWK_COINS_PCT").alias("IN_NTWK_COINS_PCT"),
    F.col("OUT_NTWK_COINS_PCT").alias("OUT_NTWK_COINS_PCT"),
    F.col("RTN_IN_NTWK_COINS_PCT").alias("RTN_IN_NTWK_COINS_PCT"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DRUG_NPRFR_SPEC_COINS_PCT").alias("DRUG_NPRFR_SPEC_COINS_PCT")
)

# Stage: db2_KProdCoinsSumFExt (DB2ConnectorPX) - Read from EDW
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = f"SELECT PROD_ID, SRC_SYS_CD, PROD_CMPNT_EFF_DT_SK, CRT_RUN_CYC_EXCTN_DT_SK, PROD_COINS_SUM_SK, CRT_RUN_CYC_EXCTN_SK FROM {EDWOwner}.K_PROD_COINS_SUM_F"
df_db2_KProdCoinsSumFExt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: rdp_NaturalKeys (PxRemDup)
df_rdp_NaturalKeys_temp = dedup_sort(
    df_lnkRemDupDataIn,
    ["SRC_SYS_CD", "PROD_ID", "PROD_CMPNT_EFF_DT_SK"],
    []
)
df_rdp_NaturalKeys = df_rdp_NaturalKeys_temp.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK")
)

# Stage: jn_ProdCoinsSumF (PxJoin, left outer join)
df_jn_ProdCoinsSumF = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_KProdCoinsSumFExt.alias("lnkKProdCopaySumFIExt"),
    on=[
        F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("lnkKProdCopaySumFIExt.SRC_SYS_CD"),
        F.col("lnkRemDupDataOut.PROD_ID") == F.col("lnkKProdCopaySumFIExt.PROD_ID"),
        F.col("lnkRemDupDataOut.PROD_CMPNT_EFF_DT_SK") == F.col("lnkKProdCopaySumFIExt.PROD_CMPNT_EFF_DT_SK")
    ],
    how="left"
)
df_jn_ProdCoinsSumF = df_jn_ProdCoinsSumF.select(
    F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkRemDupDataOut.PROD_ID").alias("PROD_ID"),
    F.col("lnkRemDupDataOut.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.col("lnkKProdCopaySumFIExt.PROD_COINS_SUM_SK").alias("PROD_COINS_SUM_SK"),
    F.col("lnkKProdCopaySumFIExt.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnkKProdCopaySumFIExt.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

# Stage: xfm_PKEYgen (CTransformerStage)
df_tr = df_jn_ProdCoinsSumF.withColumn(
    "col_new_insert",
    F.col("PROD_COINS_SUM_SK").isNull()
)
df_enriched = df_tr
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PROD_COINS_SUM_SK",<schema>,<secret_name>)

df_lnk_IdsEdwKProdCoinsSumF_Out = df_enriched.filter(
    F.col("col_new_insert") == True
).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PROD_COINS_SUM_SK").alias("PROD_COINS_SUM_SK"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

df_lnkPKEYxfmOut = df_enriched.select(
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.col("PROD_COINS_SUM_SK").alias("PROD_COINS_SUM_SK"),
    F.when(F.col("col_new_insert"), F.lit(EDWRunCycleDate)).otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK")).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.when(F.col("col_new_insert"), F.lit(EDWRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK")
)

# Stage: db2_ProdCoinsSumFLoad (DB2ConnectorPX) - Upsert into EDW
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsEdwProdCoinsSumFPky_db2_ProdCoinsSumFLoad_temp",
    jdbc_url,
    jdbc_props
)
df_lnk_IdsEdwKProdCoinsSumF_Out.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsEdwProdCoinsSumFPky_db2_ProdCoinsSumFLoad_temp") \
    .mode("error") \
    .save()

merge_sql = f"""
MERGE INTO {EDWOwner}.K_PROD_COINS_SUM_F AS T
USING STAGING.IdsEdwProdCoinsSumFPky_db2_ProdCoinsSumFLoad_temp AS S
ON (
  T.SRC_SYS_CD=S.SRC_SYS_CD
  AND T.PROD_ID=S.PROD_ID
  AND T.PROD_CMPNT_EFF_DT_SK=S.PROD_CMPNT_EFF_DT_SK
)
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_DT_SK=S.CRT_RUN_CYC_EXCTN_DT_SK,
    T.PROD_COINS_SUM_SK=S.PROD_COINS_SUM_SK,
    T.CRT_RUN_CYC_EXCTN_SK=S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    PROD_ID,
    PROD_CMPNT_EFF_DT_SK,
    CRT_RUN_CYC_EXCTN_DT_SK,
    PROD_COINS_SUM_SK,
    CRT_RUN_CYC_EXCTN_SK
  )
  VALUES (
    S.SRC_SYS_CD,
    S.PROD_ID,
    S.PROD_CMPNT_EFF_DT_SK,
    S.CRT_RUN_CYC_EXCTN_DT_SK,
    S.PROD_COINS_SUM_SK,
    S.CRT_RUN_CYC_EXCTN_SK
  );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Stage: jn_PKEYs (PxJoin, left outer join)
df_jn_PKEYs = df_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    on=[
        F.col("lnkFullDataJnIn.SRC_SYS_CD") == F.col("lnkPKEYxfmOut.SRC_SYS_CD"),
        F.col("lnkFullDataJnIn.PROD_ID") == F.col("lnkPKEYxfmOut.PROD_ID"),
        F.col("lnkFullDataJnIn.PROD_CMPNT_EFF_DT_SK") == F.col("lnkPKEYxfmOut.PROD_CMPNT_EFF_DT_SK")
    ],
    how="left"
)
df_jn_PKEYs = df_jn_PKEYs.select(
    F.col("lnkPKEYxfmOut.PROD_COINS_SUM_SK").alias("PROD_COINS_SUM_SK"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkFullDataJnIn.PROD_ID").alias("PROD_ID"),
    F.col("lnkFullDataJnIn.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnkFullDataJnIn.PROD_SK").alias("PROD_SK"),
    F.col("lnkFullDataJnIn.PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
    F.col("lnkFullDataJnIn.DRUG_GNRC_COINS_PCT").alias("DRUG_GNRC_COINS_PCT"),
    F.col("lnkFullDataJnIn.DRUG_NM_BRND_COINS_PCT").alias("DRUG_NM_BRND_COINS_PCT"),
    F.col("lnkFullDataJnIn.DRUG_PRFRD_COINS_PCT").alias("DRUG_PRFRD_COINS_PCT"),
    F.col("lnkFullDataJnIn.DRUG_NPRFR_COINS_PCT").alias("DRUG_NPRFR_COINS_PCT"),
    F.col("lnkFullDataJnIn.IN_HOSP_IN_OUT_NTWK_COINS_PCT").alias("IN_HOSP_IN_OUT_NTWK_COINS_PCT"),
    F.col("lnkFullDataJnIn.IN_NTWK_COINS_PCT").alias("IN_NTWK_COINS_PCT"),
    F.col("lnkFullDataJnIn.OUT_NTWK_COINS_PCT").alias("OUT_NTWK_COINS_PCT"),
    F.col("lnkFullDataJnIn.RTN_IN_NTWK_COINS_PCT").alias("RTN_IN_NTWK_COINS_PCT"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.DRUG_NPRFR_SPEC_COINS_PCT").alias("DRUG_NPRFR_SPEC_COINS_PCT")
)

# Stage: seq_ProdCoinsSumFPKey (PxSequentialFile)
df_seq_ProdCoinsSumFPKey = df_jn_PKEYs.select(
    F.col("PROD_COINS_SUM_SK").alias("PROD_COINS_SUM_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.rpad(F.col("PROD_CMPNT_EFF_DT_SK"), 10, " ").alias("PROD_CMPNT_EFF_DT_SK"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.rpad(F.col("PROD_CMPNT_TERM_DT_SK"), 10, " ").alias("PROD_CMPNT_TERM_DT_SK"),
    F.col("DRUG_GNRC_COINS_PCT").alias("DRUG_GNRC_COINS_PCT"),
    F.col("DRUG_NM_BRND_COINS_PCT").alias("DRUG_NM_BRND_COINS_PCT"),
    F.col("DRUG_PRFRD_COINS_PCT").alias("DRUG_PRFRD_COINS_PCT"),
    F.col("DRUG_NPRFR_COINS_PCT").alias("DRUG_NPRFR_COINS_PCT"),
    F.col("IN_HOSP_IN_OUT_NTWK_COINS_PCT").alias("IN_HOSP_IN_OUT_NTWK_COINS_PCT"),
    F.col("IN_NTWK_COINS_PCT").alias("IN_NTWK_COINS_PCT"),
    F.col("OUT_NTWK_COINS_PCT").alias("OUT_NTWK_COINS_PCT"),
    F.col("RTN_IN_NTWK_COINS_PCT").alias("RTN_IN_NTWK_COINS_PCT"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DRUG_NPRFR_SPEC_COINS_PCT").alias("DRUG_NPRFR_SPEC_COINS_PCT")
)

write_files(
    df_seq_ProdCoinsSumFPKey,
    f"{adls_path}/load/PROD_COINS_SUM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="^",
    nullValue=None
)