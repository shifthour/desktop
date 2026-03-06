# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Ravi Abburi             2017-11- 01          5781                              Original Programming                                                                             IntegrateDev2           Kalyan Neelam             2018-01-30

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_NTNL_PROV.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Land into Seq File for the FKEY job
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')

# Read from ds_NtnlProv_Xfrm (PxDataSet => read as Parquet)
df_ds_NtnlProv_Xfrm = spark.read.parquet(f"{adls_path}/ds/NTNL_PROV_TXNMY.{SrcSysCd}.extr.{RunID}.parquet")

# cpy_MultiStreams (PxCopy)
df_cpy_MultiStreams = df_ds_NtnlProv_Xfrm

df_lnk_KNtnlProvTxnmyPkey_All = df_cpy_MultiStreams.select(
    col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("TXNMY_CD").alias("TXNMY_CD"),
    col("ENTY_LIC_ST_CD").alias("ENTY_LIC_ST_CD"),
    col("LIC_NO").alias("LIC_NO"),
    col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP"),
    col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH").alias("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH"),
    col("CUR_RCRD_IN").alias("CUR_RCRD_IN"),
    col("NTNL_PROV_TXNMY_ACTV_IN").alias("NTNL_PROV_TXNMY_ACTV_IN"),
    col("NTNL_PROV_TXNMY_PRI_IN").alias("NTNL_PROV_TXNMY_PRI_IN")
)

df_lnk_NtnlProvTxnmyPkey_Dedup = df_cpy_MultiStreams.select(
    col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("TXNMY_CD").alias("TXNMY_CD"),
    col("ENTY_LIC_ST_CD").alias("ENTY_LIC_ST_CD"),
    col("LIC_NO").alias("LIC_NO")
)

# rdp_NaturalKeys (PxRemDup)
df_lnkRemDupDataOut = dedup_sort(
    df_lnk_NtnlProvTxnmyPkey_Dedup,
    ["NTNL_PROV_ID", "TXNMY_CD", "ENTY_LIC_ST_CD", "LIC_NO"],
    [("NTNL_PROV_ID","A"),("TXNMY_CD","A"),("ENTY_LIC_ST_CD","A"),("LIC_NO","A")]
)

# db2_K_NTNL_PROV_TXNMY_In (DB2ConnectorPX => read via JDBC)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids = f"SELECT NTNL_PROV_ID, TXNMY_CD, ENTY_LIC_ST_CD, LIC_NO, NTNL_PROV_TXNMY_SK, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.K_NTNL_PROV_TXNMY"
df_db2_K_NTNL_PROV_TXNMY_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids)
    .load()
)

# jn_Prov (PxJoin => left outer join on 4 keys)
df_lnkRemDupDataOut_alias = df_lnkRemDupDataOut.alias("lnkRemDupDataOut")
df_db2_K_NTNL_PROV_TXNMY_In_alias = df_db2_K_NTNL_PROV_TXNMY_In.alias("lnk_KNtnlProvTxnmy_extr")
df_jn_Prov = df_lnkRemDupDataOut_alias.join(
    df_db2_K_NTNL_PROV_TXNMY_In_alias,
    on=["NTNL_PROV_ID", "TXNMY_CD", "ENTY_LIC_ST_CD", "LIC_NO"],
    how="left"
).select(
    col("lnkRemDupDataOut.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("lnkRemDupDataOut.TXNMY_CD").alias("TXNMY_CD"),
    col("lnkRemDupDataOut.ENTY_LIC_ST_CD").alias("ENTY_LIC_ST_CD"),
    col("lnkRemDupDataOut.LIC_NO").alias("LIC_NO"),
    col("lnk_KNtnlProvTxnmy_extr.NTNL_PROV_TXNMY_SK").alias("NTNL_PROV_TXNMY_SK"),
    col("lnk_KNtnlProvTxnmy_extr.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_KNtnlProvTxnmy_extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

# xfm_PKEYgen (CTransformerStage)
# Build an enriched DataFrame with a surrogate column "svProvSK" replacing NextSurrogateKey()
df_enriched = df_jn_Prov.withColumn(
    "svProvSK",
    when(col("NTNL_PROV_TXNMY_SK").isNull(), lit(None)).otherwise(col("NTNL_PROV_TXNMY_SK"))
)

# SurrogateKeyGen to fill in null "svProvSK"
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svProvSK",<schema>,<secret_name>)

# lnk_KNtnlProvTxnmy_New => Filter where NTNL_PROV_TXNMY_SK is null
df_lnk_KNtnlProvTxnmy_New = df_enriched.filter(col("NTNL_PROV_TXNMY_SK").isNull()).select(
    col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("TXNMY_CD").alias("TXNMY_CD"),
    col("ENTY_LIC_ST_CD").alias("ENTY_LIC_ST_CD"),
    col("LIC_NO").alias("LIC_NO"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    col("svProvSK").alias("NTNL_PROV_TXNMY_SK")
)

# lnkPKEYxfmOut => All rows (no filter) with additional transformations in columns
df_lnkPKEYxfmOut = df_enriched.select(
    col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("TXNMY_CD").alias("TXNMY_CD"),
    col("ENTY_LIC_ST_CD").alias("ENTY_LIC_ST_CD"),
    col("LIC_NO").alias("LIC_NO"),
    when(col("NTNL_PROV_TXNMY_SK").isNull(), lit(SrcSysCd)).otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    when(col("NTNL_PROV_TXNMY_SK").isNull(), lit(IDSRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svProvSK").alias("NTNL_PROV_TXNMY_SK")
)

# db2_K_NTNL_PROV_TXNMY_Load (DB2ConnectorPX => Insert-only => implement as a merge with do-nothing on match)
df_db2_K_NTNL_PROV_TXNMY_Load = df_lnk_KNtnlProvTxnmy_New

temp_table_name_load = f"STAGING.IdsNtnlProvTxnmyPkey_db2_K_NTNL_PROV_TXNMY_Load_temp"
merge_target_table_load = f"{IDSOwner}.K_NTNL_PROV_TXNMY"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_name_load}", jdbc_url_ids, jdbc_props_ids)

# Write lnk_KNtnlProvTxnmy_New to staging table
(
    df_db2_K_NTNL_PROV_TXNMY_Load.write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", temp_table_name_load)
    .mode("overwrite")
    .save()
)

merge_sql_load = f"""
MERGE INTO {merge_target_table_load} AS T
USING {temp_table_name_load} AS S
ON (
    T.NTNL_PROV_ID = S.NTNL_PROV_ID
    AND T.TXNMY_CD = S.TXNMY_CD
    AND T.ENTY_LIC_ST_CD = S.ENTY_LIC_ST_CD
    AND T.LIC_NO = S.LIC_NO
)
WHEN MATCHED THEN
  UPDATE SET T.NTNL_PROV_ID = T.NTNL_PROV_ID
WHEN NOT MATCHED THEN
  INSERT (
    NTNL_PROV_ID,
    TXNMY_CD,
    ENTY_LIC_ST_CD,
    LIC_NO,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    NTNL_PROV_TXNMY_SK
  )
  VALUES (
    S.NTNL_PROV_ID,
    S.TXNMY_CD,
    S.ENTY_LIC_ST_CD,
    S.LIC_NO,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.NTNL_PROV_TXNMY_SK
  );
"""

execute_dml(merge_sql_load, jdbc_url_ids, jdbc_props_ids)

# jn_PKEYs (PxJoin => inner join)
df_lnk_KNtnlProvTxnmyPkey_All_alias = df_lnk_KNtnlProvTxnmyPkey_All.alias("lnk_KNtnlProvTxnmyPkey_All")
df_lnkPKEYxfmOut_alias = df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut")
df_jn_PKEYs = df_lnk_KNtnlProvTxnmyPkey_All_alias.join(
    df_lnkPKEYxfmOut_alias,
    on=["NTNL_PROV_ID", "TXNMY_CD", "ENTY_LIC_ST_CD", "LIC_NO"],
    how="inner"
).select(
    col("lnkPKEYxfmOut.NTNL_PROV_TXNMY_SK").alias("NTNL_PROV_TXNMY_SK"),
    col("lnk_KNtnlProvTxnmyPkey_All.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("lnk_KNtnlProvTxnmyPkey_All.TXNMY_CD").alias("TXNMY_CD"),
    col("lnk_KNtnlProvTxnmyPkey_All.ENTY_LIC_ST_CD").alias("ENTY_LIC_ST_CD"),
    col("lnk_KNtnlProvTxnmyPkey_All.LIC_NO").alias("LIC_NO"),
    col("lnkPKEYxfmOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnk_KNtnlProvTxnmyPkey_All.HEALTHCARE_PROVIDER_TAXONOMY_GROUP").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP"),
    col("lnk_KNtnlProvTxnmyPkey_All.HLTHCARE_PROV_PRI_TAXONOMY_SWITCH").alias("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH"),
    col("lnk_KNtnlProvTxnmyPkey_All.CUR_RCRD_IN").alias("CUR_RCRD_IN"),
    col("lnk_KNtnlProvTxnmyPkey_All.NTNL_PROV_TXNMY_ACTV_IN").alias("NTNL_PROV_TXNMY_ACTV_IN"),
    col("lnk_KNtnlProvTxnmyPkey_All.NTNL_PROV_TXNMY_PRI_IN").alias("NTNL_PROV_TXNMY_PRI_IN")
)

# seq_NTNL_PROV_TXNMY_Pkey (PxSequentialFile => write .dat)
df_seq_NTNL_PROV_TXNMY_Pkey = df_jn_PKEYs.select(
    "NTNL_PROV_TXNMY_SK",
    "NTNL_PROV_ID",
    "TXNMY_CD",
    "ENTY_LIC_ST_CD",
    "LIC_NO",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "HEALTHCARE_PROVIDER_TAXONOMY_GROUP",
    "HLTHCARE_PROV_PRI_TAXONOMY_SWITCH",
    "CUR_RCRD_IN",
    "NTNL_PROV_TXNMY_ACTV_IN",
    "NTNL_PROV_TXNMY_PRI_IN"
).withColumn(
    "CUR_RCRD_IN", rpad(col("CUR_RCRD_IN"), 1, " ")
).withColumn(
    "NTNL_PROV_TXNMY_ACTV_IN", rpad(col("NTNL_PROV_TXNMY_ACTV_IN"), 1, " ")
).withColumn(
    "NTNL_PROV_TXNMY_PRI_IN", rpad(col("NTNL_PROV_TXNMY_PRI_IN"), 1, " ")
)

write_files(
    df_seq_NTNL_PROV_TXNMY_Pkey,
    f"{adls_path}/key/NTNL_PROV_TXNMY.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)