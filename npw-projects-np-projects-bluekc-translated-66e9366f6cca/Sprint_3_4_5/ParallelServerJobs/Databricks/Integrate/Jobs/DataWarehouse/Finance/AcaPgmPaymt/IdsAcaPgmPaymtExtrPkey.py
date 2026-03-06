# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING: Generates Primary Key for ACA_PGM_PAYMT table
# MAGIC     
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                 2015-10-22         5128                            Initial Programming                                               IntegrateDev1              Bhoomi Dasari              10/23/2015

# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Read K_ACA_PGM_PAYMT Table to pull the Natural Keys and the Skey.
# MAGIC Inner Join primary key info with table info
# MAGIC Left Join on Natural Keys
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import rpad
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

SrcSysCd = get_widget_value('SrcSysCd','VALENCIA')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','100')
RunID = get_widget_value('RunID','100')

df_ds_ACA_PGM_PAYMT_Xfm = spark.read.parquet(
  f"{adls_path}/ds/ACA_PGM_PAYMT.{SrcSysCd}.xfrm.{RunID}.parquet"
).select(
  "PRI_NAT_KEY_STRING",
  "FIRST_RECYC_TS",
  "ACA_PGM_PAYMT_SK",
  "ACTVTY_YR_MO",
  "PAYMT_COV_YR_MO",
  "PAYMT_TYP_CD",
  "ST_CD",
  "QHP_ID",
  "ACA_PGM_PAYMT_SEQ_NO",
  "SRC_SYS_CD",
  "SRC_SYS_CD_SK",
  "COV_STRT_DT_SK",
  "COV_END_DT_SK",
  "EFT_EFF_DT_SK",
  "ACA_PGM_TRANS_AMT",
  "ACA_PGM_PAYMT_UNIQ_KEY",
  "EFT_TRACE_ID",
  "EXCH_RPT_DOC_CTL_ID",
  "EXCH_RPT_NM",
  "EFF_DT_SK"
)

df1_cp_pk_out = df_ds_ACA_PGM_PAYMT_Xfm.select(
  F.col("ACTVTY_YR_MO").alias("ACTVTY_YR_MO"),
  F.col("PAYMT_COV_YR_MO").alias("PAYMT_COV_YR_MO"),
  F.col("PAYMT_TYP_CD").alias("PAYMT_TYP_CD"),
  F.col("ST_CD").alias("ST_CD"),
  F.col("QHP_ID").alias("QHP_ID"),
  F.col("ACA_PGM_PAYMT_SEQ_NO").alias("ACA_PGM_PAYMT_SEQ_NO"),
  F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df2_cp_pk_out = df_ds_ACA_PGM_PAYMT_Xfm.select(
  F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
  F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
  F.col("ACTVTY_YR_MO").alias("ACTVTY_YR_MO"),
  F.col("PAYMT_COV_YR_MO").alias("PAYMT_COV_YR_MO"),
  F.col("PAYMT_TYP_CD").alias("PAYMT_TYP_CD"),
  F.col("ST_CD").alias("ST_CD"),
  F.col("QHP_ID").alias("QHP_ID"),
  F.col("ACA_PGM_PAYMT_SEQ_NO").alias("ACA_PGM_PAYMT_SEQ_NO"),
  F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
  F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
  F.col("COV_STRT_DT_SK").alias("COV_STRT_DT_SK"),
  F.col("COV_END_DT_SK").alias("COV_END_DT_SK"),
  F.col("EFT_EFF_DT_SK").alias("EFT_EFF_DT_SK"),
  F.col("ACA_PGM_TRANS_AMT").alias("ACA_PGM_TRANS_AMT"),
  F.col("ACA_PGM_PAYMT_UNIQ_KEY").alias("ACA_PGM_PAYMT_UNIQ_KEY"),
  F.col("EFT_TRACE_ID").alias("EFT_TRACE_ID"),
  F.col("EXCH_RPT_DOC_CTL_ID").alias("EXCH_RPT_DOC_CTL_ID"),
  F.col("EXCH_RPT_NM").alias("EXCH_RPT_NM"),
  F.col("EFF_DT_SK").alias("EFF_DT_SK")
)

df_rdup_Natural_Keys = dedup_sort(
  df1_cp_pk_out,
  ["ACTVTY_YR_MO","PAYMT_COV_YR_MO","PAYMT_TYP_CD","ST_CD","QHP_ID","ACA_PGM_PAYMT_SEQ_NO","SRC_SYS_CD"],
  []
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
  f"SELECT ACTVTY_YR_MO, PAYMT_COV_YR_MO, PAYMT_TYP_CD, "
  f"ST_CD, QHP_ID, ACA_PGM_PAYMT_SEQ_NO, SRC_SYS_CD, "
  f"CRT_RUN_CYC_EXCTN_SK, ACA_PGM_PAYMT_SK "
  f"FROM {IDSOwner}.K_ACA_PGM_PAYMT"
)
df_db2_K_ACA_PGM_PAYMT_in = (
  spark.read.format("jdbc")
  .option("url", jdbc_url)
  .options(**jdbc_props)
  .option("query", extract_query)
  .load()
)

df_jn_AcaPgmPaymt = df_rdup_Natural_Keys.alias("lnk_Natural_Keys_out").join(
  df_db2_K_ACA_PGM_PAYMT_in.alias("lnk_KAcaPgmPaymtPkey_out"),
  (
    (F.col("lnk_Natural_Keys_out.ACTVTY_YR_MO") == F.col("lnk_KAcaPgmPaymtPkey_out.ACTVTY_YR_MO")) &
    (F.col("lnk_Natural_Keys_out.PAYMT_COV_YR_MO") == F.col("lnk_KAcaPgmPaymtPkey_out.PAYMT_COV_YR_MO")) &
    (F.col("lnk_Natural_Keys_out.PAYMT_TYP_CD") == F.col("lnk_KAcaPgmPaymtPkey_out.PAYMT_TYP_CD")) &
    (F.col("lnk_Natural_Keys_out.ST_CD") == F.col("lnk_KAcaPgmPaymtPkey_out.ST_CD")) &
    (F.col("lnk_Natural_Keys_out.QHP_ID") == F.col("lnk_KAcaPgmPaymtPkey_out.QHP_ID")) &
    (F.col("lnk_Natural_Keys_out.ACA_PGM_PAYMT_SEQ_NO") == F.col("lnk_KAcaPgmPaymtPkey_out.ACA_PGM_PAYMT_SEQ_NO")) &
    (F.col("lnk_Natural_Keys_out.SRC_SYS_CD") == F.col("lnk_KAcaPgmPaymtPkey_out.SRC_SYS_CD"))
  ),
  "left"
).select(
  F.col("lnk_Natural_Keys_out.ACTVTY_YR_MO").alias("ACTVTY_YR_MO"),
  F.col("lnk_Natural_Keys_out.PAYMT_COV_YR_MO").alias("PAYMT_COV_YR_MO"),
  F.col("lnk_Natural_Keys_out.PAYMT_TYP_CD").alias("PAYMT_TYP_CD"),
  F.col("lnk_Natural_Keys_out.ST_CD").alias("ST_CD"),
  F.col("lnk_Natural_Keys_out.QHP_ID").alias("QHP_ID"),
  F.col("lnk_Natural_Keys_out.ACA_PGM_PAYMT_SEQ_NO").alias("ACA_PGM_PAYMT_SEQ_NO"),
  F.col("lnk_Natural_Keys_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
  F.col("lnk_KAcaPgmPaymtPkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
  F.col("lnk_KAcaPgmPaymtPkey_out.ACA_PGM_PAYMT_SK").alias("ACA_PGM_PAYMT_SK")
)

df_xfrm_in = df_jn_AcaPgmPaymt.withColumn("IDSRunCycle", F.lit(IDSRunCycle))
df_new = df_xfrm_in.filter(F.col("ACA_PGM_PAYMT_SK").isNull())
df_existing = df_xfrm_in.filter(F.col("ACA_PGM_PAYMT_SK").isNotNull())

df_new = SurrogateKeyGen(df_new,<DB sequence name>,"ACA_PGM_PAYMT_SK",<schema>,<secret_name>)
df_new = df_new.withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("IDSRunCycle"))
df_new = df_new.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("IDSRunCycle"))

df_existing = df_existing.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("IDSRunCycle"))

df_new = df_new.select(
  "ACTVTY_YR_MO","PAYMT_COV_YR_MO","PAYMT_TYP_CD","ST_CD","QHP_ID",
  "ACA_PGM_PAYMT_SEQ_NO","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_SK","ACA_PGM_PAYMT_SK","IDSRunCycle",
  "LAST_UPDT_RUN_CYC_EXCTN_SK"
)
df_existing = df_existing.select(
  "ACTVTY_YR_MO","PAYMT_COV_YR_MO","PAYMT_TYP_CD","ST_CD","QHP_ID",
  "ACA_PGM_PAYMT_SEQ_NO","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_SK","ACA_PGM_PAYMT_SK","IDSRunCycle",
  "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

df_enriched = df_new.unionByName(df_existing)

df_lnk_Pkey_out = df_enriched.select(
  "ACTVTY_YR_MO","PAYMT_COV_YR_MO","PAYMT_TYP_CD","ST_CD","QHP_ID",
  "ACA_PGM_PAYMT_SEQ_NO","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_SK",
  "LAST_UPDT_RUN_CYC_EXCTN_SK","ACA_PGM_PAYMT_SK"
)

df_lnk_KAcaPgmPaymt_Out = df_new.select(
  "ACTVTY_YR_MO","PAYMT_COV_YR_MO","PAYMT_TYP_CD","ST_CD","QHP_ID",
  "ACA_PGM_PAYMT_SEQ_NO","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_SK","ACA_PGM_PAYMT_SK"
)

df_db2_K_ACA_PGM_PAYMT_Load = df_lnk_KAcaPgmPaymt_Out

temp_table_name = "STAGING.IdsAcaPgmPaymtExtrPkey_db2_K_ACA_PGM_PAYMT_Load_temp"
spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")
df_db2_K_ACA_PGM_PAYMT_Load.write.jdbc(
  url=jdbc_url,
  table=temp_table_name,
  mode="overwrite",
  properties=jdbc_props
)
merge_sql = (
  f"MERGE INTO {IDSOwner}.K_ACA_PGM_PAYMT AS T "
  f"USING {temp_table_name} AS S "
  f"ON (T.ACTVTY_YR_MO=S.ACTVTY_YR_MO AND "
  f"T.PAYMT_COV_YR_MO=S.PAYMT_COV_YR_MO AND "
  f"T.PAYMT_TYP_CD=S.PAYMT_TYP_CD AND "
  f"T.ST_CD=S.ST_CD AND "
  f"T.QHP_ID=S.QHP_ID AND "
  f"T.ACA_PGM_PAYMT_SEQ_NO=S.ACA_PGM_PAYMT_SEQ_NO AND "
  f"T.SRC_SYS_CD=S.SRC_SYS_CD) "
  f"WHEN MATCHED THEN UPDATE SET "
  f"T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK, "
  f"T.ACA_PGM_PAYMT_SK = S.ACA_PGM_PAYMT_SK "
  f"WHEN NOT MATCHED THEN INSERT "
  f"(ACTVTY_YR_MO, PAYMT_COV_YR_MO, PAYMT_TYP_CD, ST_CD, QHP_ID, "
  f"ACA_PGM_PAYMT_SEQ_NO, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, ACA_PGM_PAYMT_SK) "
  f"VALUES "
  f"(S.ACTVTY_YR_MO, S.PAYMT_COV_YR_MO, S.PAYMT_TYP_CD, S.ST_CD, S.QHP_ID, "
  f"S.ACA_PGM_PAYMT_SEQ_NO, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.ACA_PGM_PAYMT_SK);"
)
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_jn_PKey = df2_cp_pk_out.alias("Lnk_cp_Out").join(
  df_lnk_Pkey_out.alias("lnk_Pkey_out"),
  [
    F.col("Lnk_cp_Out.ACTVTY_YR_MO") == F.col("lnk_Pkey_out.ACTVTY_YR_MO"),
    F.col("Lnk_cp_Out.PAYMT_COV_YR_MO") == F.col("lnk_Pkey_out.PAYMT_COV_YR_MO"),
    F.col("Lnk_cp_Out.PAYMT_TYP_CD") == F.col("lnk_Pkey_out.PAYMT_TYP_CD"),
    F.col("Lnk_cp_Out.ST_CD") == F.col("lnk_Pkey_out.ST_CD"),
    F.col("Lnk_cp_Out.QHP_ID") == F.col("lnk_Pkey_out.QHP_ID"),
    F.col("Lnk_cp_Out.ACA_PGM_PAYMT_SEQ_NO") == F.col("lnk_Pkey_out.ACA_PGM_PAYMT_SEQ_NO"),
    F.col("Lnk_cp_Out.SRC_SYS_CD") == F.col("lnk_Pkey_out.SRC_SYS_CD")
  ],
  "inner"
).select(
  F.col("Lnk_cp_Out.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
  F.col("Lnk_cp_Out.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
  F.col("lnk_Pkey_out.ACA_PGM_PAYMT_SK").alias("ACA_PGM_PAYMT_SK"),
  F.col("Lnk_cp_Out.ACTVTY_YR_MO").alias("ACTVTY_YR_MO"),
  F.col("Lnk_cp_Out.PAYMT_COV_YR_MO").alias("PAYMT_COV_YR_MO"),
  F.col("Lnk_cp_Out.PAYMT_TYP_CD").alias("PAYMT_TYP_CD"),
  F.col("Lnk_cp_Out.ST_CD").alias("ST_CD"),
  F.col("Lnk_cp_Out.QHP_ID").alias("QHP_ID"),
  F.col("Lnk_cp_Out.ACA_PGM_PAYMT_SEQ_NO").alias("ACA_PGM_PAYMT_SEQ_NO"),
  F.col("Lnk_cp_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
  F.col("Lnk_cp_Out.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
  F.col("lnk_Pkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
  F.col("lnk_Pkey_out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
  F.col("Lnk_cp_Out.COV_STRT_DT_SK").alias("COV_STRT_DT_SK"),
  F.col("Lnk_cp_Out.COV_END_DT_SK").alias("COV_END_DT_SK"),
  F.col("Lnk_cp_Out.EFT_EFF_DT_SK").alias("EFT_EFF_DT_SK"),
  F.col("Lnk_cp_Out.ACA_PGM_TRANS_AMT").alias("ACA_PGM_TRANS_AMT"),
  F.col("Lnk_cp_Out.ACA_PGM_PAYMT_UNIQ_KEY").alias("ACA_PGM_PAYMT_UNIQ_KEY"),
  F.col("Lnk_cp_Out.EFT_TRACE_ID").alias("EFT_TRACE_ID"),
  F.col("Lnk_cp_Out.EXCH_RPT_DOC_CTL_ID").alias("EXCH_RPT_DOC_CTL_ID"),
  F.col("Lnk_cp_Out.EXCH_RPT_NM").alias("EXCH_RPT_NM"),
  F.col("Lnk_cp_Out.EFF_DT_SK").alias("EFF_DT_SK")
)

df_seq_ACA_PGM_PAYMT_PKEY = (
  df_jn_PKey
  .withColumn("ACTVTY_YR_MO", rpad(F.col("ACTVTY_YR_MO"), 6, " "))
  .withColumn("PAYMT_COV_YR_MO", rpad(F.col("PAYMT_COV_YR_MO"), 6, " "))
  .withColumn("COV_STRT_DT_SK", rpad(F.col("COV_STRT_DT_SK"), 10, " "))
  .withColumn("COV_END_DT_SK", rpad(F.col("COV_END_DT_SK"), 10, " "))
  .withColumn("EFT_EFF_DT_SK", rpad(F.col("EFT_EFF_DT_SK"), 10, " "))
  .withColumn("EFF_DT_SK", rpad(F.col("EFF_DT_SK"), 10, " "))
  .select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "ACA_PGM_PAYMT_SK",
    "ACTVTY_YR_MO",
    "PAYMT_COV_YR_MO",
    "PAYMT_TYP_CD",
    "ST_CD",
    "QHP_ID",
    "ACA_PGM_PAYMT_SEQ_NO",
    "SRC_SYS_CD",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "COV_STRT_DT_SK",
    "COV_END_DT_SK",
    "EFT_EFF_DT_SK",
    "ACA_PGM_TRANS_AMT",
    "ACA_PGM_PAYMT_UNIQ_KEY",
    "EFT_TRACE_ID",
    "EXCH_RPT_DOC_CTL_ID",
    "EXCH_RPT_NM",
    "EFF_DT_SK"
  )
)

write_files(
  df_seq_ACA_PGM_PAYMT_PKEY,
  f"{adls_path}/key/ACA_PGM_PAYMT.{SrcSysCd}.pkey.{RunID}.dat",
  delimiter=",",
  mode="overwrite",
  is_parquet=False,
  header=False,
  quote="^",
  nullValue=None
)