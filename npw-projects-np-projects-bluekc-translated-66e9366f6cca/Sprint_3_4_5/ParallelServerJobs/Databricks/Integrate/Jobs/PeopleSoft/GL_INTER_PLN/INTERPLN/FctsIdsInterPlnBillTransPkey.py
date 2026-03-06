# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By:FctsBcbsExtInterPlnBillLoadSeq
# MAGIC 
# MAGIC 
# MAGIC Process Description: Extract Data from GL_INTER_PLN_BILL_DTL and Load it to IDS 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                               Project/                                                                                                                        Code                  Date
# MAGIC Developer                    Date                   Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -----------------------              -------------------    -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC sudheer champati        10/12/2015       5212       Originally Programmed                                                                                 Kalyan Neelam    2015-12-31


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, regexp_replace, rpad
from pyspark.sql.types import DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

APT_NO_SORT_INSERTION = get_widget_value("APT_NO_SORT_INSERTION","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
IDSRunCycle = get_widget_value("IDSRunCycle","")
SrcSysCd = get_widget_value("SrcSysCd","")
RunID = get_widget_value("RunID","")

jdbc_url_db2_K_InterPln, jdbc_props_db2_K_InterPln = get_db_config(ids_secret_name)
extract_query_db2_K_InterPln = f"""
SELECT
CAST(INTER_PLN_BILL_TRANS_CK AS INT) AS INTER_PLN_BILL_TRANS_CK,
ACCTG_DT_SK,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_SK,
INTER_PLN_BILL_TRANS_SK
FROM {IDSOwner}.K_INTER_PLN_BILL_TRANS
"""
df_db2_K_InterPln = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_K_InterPln)
    .options(**jdbc_props_db2_K_InterPln)
    .option("query", extract_query_db2_K_InterPln)
    .load()
)

df_ds_InterPln_Xfrm = spark.read.parquet(f"{adls_path}/ds/INTER_PLN_BILL_TRANS.{SrcSysCd}.xfrm.{RunID}.parquet")

df_Transformer_7 = df_ds_InterPln_Xfrm.select(
    col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("INTER_PLN_BILL_TRANS_CK").cast(DecimalType(38,10)).alias("INTER_PLN_BILL_TRANS_CK"),
    col("ACCTG_DT_SK").alias("ACCTG_DT_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INTER_PLN_BILL_TRANS_LOB_CD_SK").alias("INTER_PLN_BILL_TRANS_LOB_CD_SK"),
    col("INTER_PLN_BILL_TRANS_PAYMT_METH_CD_SK").alias("INTER_PLN_BILL_TRANS_PAYMT_METH_CD_SK"),
    col("PD_FROM_DT_SK").alias("PD_FROM_DT_SK"),
    col("PD_THRU_DT_SK").alias("PD_THRU_DT_SK"),
    col("CALC_FUND_AMT").alias("CALC_FUND_AMT"),
    col("FUND_RATE_AMT").alias("FUND_RATE_AMT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("LOBD_ID").alias("LOBD_ID"),
    col("CRME_PYMT_METH_IND").alias("CRME_PYMT_METH_IND"),
    col("BILL_DTL_SRL_NO").alias("BILL_DTL_SRL_NO"),
    col("PDPD_ID").alias("PDPD_ID"),
    col("MBR_SK").alias("MBR_SK"),
    col("GRGR_ID").alias("GRGR_ID"),
    col("PDBL_ACCT_CAT").alias("PDBL_ACCT_CAT")
)

df_lnkRemDupDataIn = df_Transformer_7.select(
    col("INTER_PLN_BILL_TRANS_CK"),
    col("ACCTG_DT_SK"),
    col("SRC_SYS_CD")
)

df_lnkFullDataJnIn = df_Transformer_7.select(
    col("INTER_PLN_BILL_TRANS_CK"),
    col("ACCTG_DT_SK"),
    col("SRC_SYS_CD"),
    col("SRC_SYS_CD_SK"),
    col("INTER_PLN_BILL_TRANS_LOB_CD_SK"),
    col("INTER_PLN_BILL_TRANS_PAYMT_METH_CD_SK"),
    col("PD_FROM_DT_SK"),
    col("PD_THRU_DT_SK"),
    col("CALC_FUND_AMT"),
    col("FUND_RATE_AMT"),
    col("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("LOBD_ID"),
    col("CRME_PYMT_METH_IND"),
    col("BILL_DTL_SRL_NO"),
    col("PDPD_ID"),
    col("MBR_SK"),
    col("GRGR_ID"),
    col("PDBL_ACCT_CAT")
)

df_rdp_NaturalKeys = dedup_sort(
    df_lnkRemDupDataIn,
    partition_cols=["INTER_PLN_BILL_TRANS_CK","ACCTG_DT_SK","SRC_SYS_CD"],
    sort_cols=[]
)

df_jn_InterPln = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_K_InterPln.alias("lnk_KInterPlnBill_Extr"),
    [
        col("lnkRemDupDataOut.INTER_PLN_BILL_TRANS_CK") == col("lnk_KInterPlnBill_Extr.INTER_PLN_BILL_TRANS_CK"),
        col("lnkRemDupDataOut.ACCTG_DT_SK") == col("lnk_KInterPlnBill_Extr.ACCTG_DT_SK"),
        col("lnkRemDupDataOut.SRC_SYS_CD") == col("lnk_KInterPlnBill_Extr.SRC_SYS_CD")
    ],
    how="left"
).select(
    col("lnkRemDupDataOut.INTER_PLN_BILL_TRANS_CK").alias("INTER_PLN_BILL_TRANS_CK"),
    col("lnkRemDupDataOut.ACCTG_DT_SK").alias("ACCTG_DT_SK"),
    col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_KInterPlnBill_Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_KInterPlnBill_Extr.INTER_PLN_BILL_TRANS_SK").alias("INTER_PLN_BILL_TRANS_SK")
)

df_temp = df_jn_InterPln.withColumn(
    "isNullKey",
    col("INTER_PLN_BILL_TRANS_SK").isNull()
).withColumn(
    "INTER_PLN_BILL_TRANS_SK_temp",
    when(col("INTER_PLN_BILL_TRANS_SK").isNull(), lit(None)).otherwise(col("INTER_PLN_BILL_TRANS_SK"))
).withColumn(
    "IDSRunCycle",
    lit(IDSRunCycle)
)

df_enriched = SurrogateKeyGen(df_temp,<DB sequence name>,"INTER_PLN_BILL_TRANS_SK_temp",<schema>,<secret_name>)

df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_SK_enriched",
    when(col("CRT_RUN_CYC_EXCTN_SK").isNull(), lit(IDSRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK_enriched",
    lit(IDSRunCycle)
)

df_lnk_KAccum_new = df_enriched.filter(col("isNullKey") == True).select(
    regexp_replace(col("INTER_PLN_BILL_TRANS_CK"), "\\.", "").alias("INTER_PLN_BILL_TRANS_CK"),
    col("ACCTG_DT_SK"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK_enriched").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("INTER_PLN_BILL_TRANS_SK_temp").alias("INTER_PLN_BILL_TRANS_SK")
)

df_lnkPKEYxfmOut = df_enriched.select(
    col("INTER_PLN_BILL_TRANS_CK"),
    col("ACCTG_DT_SK"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK_enriched").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK_enriched").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INTER_PLN_BILL_TRANS_SK_temp").alias("INTER_PLN_BILL_TRANS_SK")
)

jdbc_url_db2_K_InterPlnBill_Load, jdbc_props_db2_K_InterPlnBill_Load = get_db_config(ids_secret_name)
temp_table_db2_K_InterPlnBill_Load = "STAGING.FctsIdsInterPlnBillTransPkey_db2_K_InterPlnBill_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_db2_K_InterPlnBill_Load}", jdbc_url_db2_K_InterPlnBill_Load, jdbc_props_db2_K_InterPlnBill_Load)
df_lnk_KAccum_new.write \
    .format("jdbc") \
    .option("url", jdbc_url_db2_K_InterPlnBill_Load) \
    .options(**jdbc_props_db2_K_InterPlnBill_Load) \
    .option("dbtable", temp_table_db2_K_InterPlnBill_Load) \
    .mode("overwrite") \
    .save()

merge_sql_db2_K_InterPlnBill_Load = f"""
MERGE INTO {IDSOwner}.K_INTER_PLN_BILL_TRANS AS target
USING {temp_table_db2_K_InterPlnBill_Load} AS source
ON
(
  target.INTER_PLN_BILL_TRANS_CK = source.INTER_PLN_BILL_TRANS_CK
  AND target.ACCTG_DT_SK = source.ACCTG_DT_SK
  AND target.SRC_SYS_CD = source.SRC_SYS_CD
)
WHEN MATCHED THEN UPDATE SET
  target.CRT_RUN_CYC_EXCTN_SK = source.CRT_RUN_CYC_EXCTN_SK,
  target.INTER_PLN_BILL_TRANS_SK = source.INTER_PLN_BILL_TRANS_SK
WHEN NOT MATCHED THEN
  INSERT
  (
    INTER_PLN_BILL_TRANS_CK,
    ACCTG_DT_SK,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    INTER_PLN_BILL_TRANS_SK
  )
  VALUES
  (
    source.INTER_PLN_BILL_TRANS_CK,
    source.ACCTG_DT_SK,
    source.SRC_SYS_CD,
    source.CRT_RUN_CYC_EXCTN_SK,
    source.INTER_PLN_BILL_TRANS_SK
  );
"""
execute_dml(merge_sql_db2_K_InterPlnBill_Load, jdbc_url_db2_K_InterPlnBill_Load, jdbc_props_db2_K_InterPlnBill_Load)

df_jn_pk = df_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    [
        col("lnkFullDataJnIn.INTER_PLN_BILL_TRANS_CK") == col("lnkPKEYxfmOut.INTER_PLN_BILL_TRANS_CK"),
        col("lnkFullDataJnIn.ACCTG_DT_SK") == col("lnkPKEYxfmOut.ACCTG_DT_SK"),
        col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD")
    ],
    "inner"
).select(
    col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("lnkPKEYxfmOut.INTER_PLN_BILL_TRANS_SK").alias("INTER_PLN_BILL_TRANS_SK"),
    col("lnkFullDataJnIn.INTER_PLN_BILL_TRANS_CK").alias("INTER_PLN_BILL_TRANS_CK"),
    col("lnkFullDataJnIn.ACCTG_DT_SK").alias("ACCTG_DT_SK"),
    col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnkFullDataJnIn.INTER_PLN_BILL_TRANS_LOB_CD_SK").alias("INTER_PLN_BILL_TRANS_LOB_CD_SK"),
    col("lnkFullDataJnIn.INTER_PLN_BILL_TRANS_PAYMT_METH_CD_SK").alias("INTER_PLN_BILL_TRANS_PAYMT_METH_CD_SK"),
    col("lnkFullDataJnIn.PD_FROM_DT_SK").alias("PD_FROM_DT_SK"),
    col("lnkFullDataJnIn.PD_THRU_DT_SK").alias("PD_THRU_DT_SK"),
    col("lnkFullDataJnIn.CALC_FUND_AMT").alias("CALC_FUND_AMT"),
    col("lnkFullDataJnIn.FUND_RATE_AMT").alias("FUND_RATE_AMT"),
    col("lnkFullDataJnIn.LOBD_ID").alias("LOBD_ID"),
    col("lnkFullDataJnIn.CRME_PYMT_METH_IND").alias("CRME_PYMT_METH_IND"),
    col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkFullDataJnIn.BILL_DTL_SRL_NO").alias("BILL_DTL_SRL_NO"),
    col("lnkFullDataJnIn.PDPD_ID").alias("PDPD_ID"),
    col("lnkFullDataJnIn.MBR_SK").alias("MBR_SK"),
    col("lnkFullDataJnIn.GRGR_ID").alias("GRGR_ID"),
    col("lnkFullDataJnIn.PDBL_ACCT_CAT").alias("PDBL_ACCT_CAT")
)

df_seq_InterPln_PKEY = df_jn_pk.withColumn(
    "ACCTG_DT_SK", rpad(col("ACCTG_DT_SK"), 10, " ")
).withColumn(
    "PD_FROM_DT_SK", rpad(col("PD_FROM_DT_SK"), 10, " ")
).withColumn(
    "PD_THRU_DT_SK", rpad(col("PD_THRU_DT_SK"), 10, " ")
).withColumn(
    "LOBD_ID", rpad(col("LOBD_ID"), 4, " ")
).withColumn(
    "CRME_PYMT_METH_IND", rpad(col("CRME_PYMT_METH_IND"), 1, " ")
).withColumn(
    "PDPD_ID", rpad(col("PDPD_ID"), 8, " ")
).withColumn(
    "PDBL_ACCT_CAT", rpad(col("PDBL_ACCT_CAT"), 4, " ")
).select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "INTER_PLN_BILL_TRANS_SK",
    "INTER_PLN_BILL_TRANS_CK",
    "ACCTG_DT_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "INTER_PLN_BILL_TRANS_LOB_CD_SK",
    "INTER_PLN_BILL_TRANS_PAYMT_METH_CD_SK",
    "PD_FROM_DT_SK",
    "PD_THRU_DT_SK",
    "CALC_FUND_AMT",
    "FUND_RATE_AMT",
    "LOBD_ID",
    "CRME_PYMT_METH_IND",
    "SRC_SYS_CD",
    "BILL_DTL_SRL_NO",
    "PDPD_ID",
    "MBR_SK",
    "GRGR_ID",
    "PDBL_ACCT_CAT"
)

write_files(
    df_seq_InterPln_PKEY,
    f"{adls_path}/key/INTER_PLN_BILL_TRANS.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)