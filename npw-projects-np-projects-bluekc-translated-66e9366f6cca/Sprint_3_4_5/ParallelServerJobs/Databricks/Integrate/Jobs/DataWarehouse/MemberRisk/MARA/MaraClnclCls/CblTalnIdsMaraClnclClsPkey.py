# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Raja Gummadi                  03/04/2015      5460                                Originally Programmed                            IntegrateNewDevl

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_MARA_CLNCL_CLS.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Land into Seq File for the FKEY job
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, rpad, when, isnull
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')

schema_ds_MARA_CLNCL_CLS_Xfrm = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), True),
    StructField("FIRST_RECYC_TS", StringType(), True),
    StructField("MARA_CLNCL_CLS_SK", StringType(), True),
    StructField("CLNCL_CLS_ID", StringType(), True),
    StructField("EFF_DT_SK", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("SRC_SYS_CD_SK", StringType(), True),
    StructField("MARA_SUM_GRP_CD", StringType(), True),
    StructField("MARA_SUM_GRP_DESC", StringType(), True),
    StructField("PRPSD_BODY_SYS_CD", StringType(), True),
    StructField("PRPSD_BODY_SYS", StringType(), True),
    StructField("SEC_BODY_SYS", StringType(), True),
    StructField("CLNCL_LABEL", StringType(), True),
    StructField("COND_CD", StringType(), True),
    StructField("AHRQ_CHRNC", StringType(), True),
    StructField("AHRQ_MOST_CMN_CHRNC", StringType(), True),
    StructField("TERM_DT_SK", StringType(), True)
])

df_ds_MARA_CLNCL_CLS_Xfrm = spark.read.schema(schema_ds_MARA_CLNCL_CLS_Xfrm).parquet(
    f"{adls_path}/ds/MARA_CLNCL_CLS.{SrcSysCd}.xfrm.{RunID}.parquet"
)

df_cpy_MultiStreams_lnkRemDupDataIn = df_ds_MARA_CLNCL_CLS_Xfrm.select(
    col("SRC_SYS_CD"),
    col("CLNCL_CLS_ID"),
    col("EFF_DT_SK")
)

df_cpy_MultiStreams_lnkFullDataJnIn = df_ds_MARA_CLNCL_CLS_Xfrm.select(
    col("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("CLNCL_CLS_ID"),
    col("EFF_DT_SK"),
    col("SRC_SYS_CD"),
    col("SRC_SYS_CD_SK"),
    col("MARA_SUM_GRP_CD"),
    col("MARA_SUM_GRP_DESC"),
    col("PRPSD_BODY_SYS_CD"),
    col("PRPSD_BODY_SYS"),
    col("SEC_BODY_SYS"),
    col("CLNCL_LABEL"),
    col("COND_CD"),
    col("AHRQ_CHRNC"),
    col("AHRQ_MOST_CMN_CHRNC"),
    col("TERM_DT_SK")
)

df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_lnkRemDupDataIn,
    partition_cols=["CLNCL_CLS_ID","EFF_DT_SK","SRC_SYS_CD"],
    sort_cols=[]
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_K_MARA_CLNCL_CLS_In = f"SELECT SRC_SYS_CD, CLNCL_CLS_ID, EFF_DT_SK, CRT_RUN_CYC_EXCTN_SK, MARA_CLNCL_CLS_SK FROM {IDSOwner}.K_MARA_CLNCL_CLS"
df_db2_K_MARA_CLNCL_CLS_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_K_MARA_CLNCL_CLS_In)
    .load()
)

df_jn_MaraClnclCls = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_K_MARA_CLNCL_CLS_In.alias("Extr"),
    on=["CLNCL_CLS_ID","EFF_DT_SK","SRC_SYS_CD"],
    how="left"
).select(
    col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkRemDupDataOut.CLNCL_CLS_ID").alias("CLNCL_CLS_ID"),
    col("lnkRemDupDataOut.EFF_DT_SK").alias("EFF_DT_SK"),
    col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Extr.MARA_CLNCL_CLS_SK").alias("MARA_CLNCL_CLS_SK")
)

df_xfm_PKEYgen_in = df_jn_MaraClnclCls

df_xfm_PKEYgen_New = df_xfm_PKEYgen_in.filter(col("MARA_CLNCL_CLS_SK").isNull()).select(
    col("SRC_SYS_CD"),
    col("CLNCL_CLS_ID"),
    col("EFF_DT_SK"),
    lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(None).cast(StringType()).alias("MARA_CLNCL_CLS_SK")
)
df_enriched = df_xfm_PKEYgen_New
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MARA_CLNCL_CLS_SK",<schema>,<secret_name>)
df_xfm_PKEYgen_New = df_enriched.select(
    col("SRC_SYS_CD"),
    col("CLNCL_CLS_ID"),
    col("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("MARA_CLNCL_CLS_SK")
)

df_xfm_PKEYgen_lnkPKEYxfmOut = df_xfm_PKEYgen_in.filter(col("MARA_CLNCL_CLS_SK").isNotNull()).select(
    col("SRC_SYS_CD"),
    col("CLNCL_CLS_ID"),
    col("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MARA_CLNCL_CLS_SK")
)

temp_table_name = "STAGING.CblTalnIdsMaraClnclClsPkey_db2_K_MARA_CLNCL_CLS_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url_ids, jdbc_props_ids)
df_xfm_PKEYgen_New.write.jdbc(
    url=jdbc_url_ids,
    table=temp_table_name,
    mode="overwrite",
    properties=jdbc_props_ids
)
merge_sql_db2_K_MARA_CLNCL_CLS_Load = f"""
MERGE INTO {IDSOwner}.K_MARA_CLNCL_CLS AS T
USING {temp_table_name} AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.CLNCL_CLS_ID = S.CLNCL_CLS_ID
  AND T.EFF_DT_SK = S.EFF_DT_SK
WHEN MATCHED THEN UPDATE SET
  T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
  T.MARA_CLNCL_CLS_SK = S.MARA_CLNCL_CLS_SK
WHEN NOT MATCHED THEN INSERT (
  SRC_SYS_CD,
  CLNCL_CLS_ID,
  EFF_DT_SK,
  CRT_RUN_CYC_EXCTN_SK,
  MARA_CLNCL_CLS_SK
)
VALUES (
  S.SRC_SYS_CD,
  S.CLNCL_CLS_ID,
  S.EFF_DT_SK,
  S.CRT_RUN_CYC_EXCTN_SK,
  S.MARA_CLNCL_CLS_SK
)
;
"""
execute_dml(merge_sql_db2_K_MARA_CLNCL_CLS_Load, jdbc_url_ids, jdbc_props_ids)

df_jn_PKEYs = df_cpy_MultiStreams_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_xfm_PKEYgen_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    on=["SRC_SYS_CD","CLNCL_CLS_ID","EFF_DT_SK"],
    how="inner"
).select(
    col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("lnkPKEYxfmOut.MARA_CLNCL_CLS_SK").alias("MARA_CLNCL_CLS_SK"),
    col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkFullDataJnIn.CLNCL_CLS_ID").alias("CLNCL_CLS_ID"),
    col("lnkFullDataJnIn.EFF_DT_SK").alias("EFF_DT_SK"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("lnkFullDataJnIn.MARA_SUM_GRP_CD").alias("MARA_SUM_GRP_CD"),
    col("lnkFullDataJnIn.MARA_SUM_GRP_DESC").alias("MARA_SUM_GRP_DESC"),
    col("lnkFullDataJnIn.PRPSD_BODY_SYS_CD").alias("PRPSD_BODY_SYS_CD"),
    col("lnkFullDataJnIn.PRPSD_BODY_SYS").alias("PRPSD_BODY_SYS"),
    col("lnkFullDataJnIn.SEC_BODY_SYS").alias("SEC_BODY_SYS"),
    col("lnkFullDataJnIn.CLNCL_LABEL").alias("CLNCL_LABEL"),
    col("lnkFullDataJnIn.COND_CD").alias("COND_CD"),
    col("lnkFullDataJnIn.AHRQ_CHRNC").alias("AHRQ_CHRNC"),
    col("lnkFullDataJnIn.AHRQ_MOST_CMN_CHRNC").alias("AHRQ_MOST_CMN_CHRNC"),
    col("lnkFullDataJnIn.TERM_DT_SK").alias("TERM_DT_SK")
)

df_seq_MARA_CLNCL_CLS_Pkey = df_jn_PKEYs.select(
    col("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("MARA_CLNCL_CLS_SK"),
    col("SRC_SYS_CD"),
    col("CLNCL_CLS_ID"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SRC_SYS_CD_SK"),
    rpad(col("MARA_SUM_GRP_CD"), 6, " ").alias("MARA_SUM_GRP_CD"),
    col("MARA_SUM_GRP_DESC"),
    rpad(col("PRPSD_BODY_SYS_CD"), 6, " ").alias("PRPSD_BODY_SYS_CD"),
    col("PRPSD_BODY_SYS"),
    col("SEC_BODY_SYS"),
    col("CLNCL_LABEL"),
    rpad(col("COND_CD"), 6, " ").alias("COND_CD"),
    col("AHRQ_CHRNC"),
    col("AHRQ_MOST_CMN_CHRNC"),
    rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK")
)

write_files(
    df_seq_MARA_CLNCL_CLS_Pkey,
    f"{adls_path}/key/MARA_CLNCL_CLS.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)