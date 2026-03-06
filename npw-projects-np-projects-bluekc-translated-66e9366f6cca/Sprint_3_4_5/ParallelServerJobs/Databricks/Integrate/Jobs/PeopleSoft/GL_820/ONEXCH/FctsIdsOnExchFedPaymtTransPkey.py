# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By:FctsBcbsExtOnExchExtrLoadSeq
# MAGIC 
# MAGIC 
# MAGIC Process Description: Extract Data from (GL_ON_EXCH_FED_PYMT_DTL) and Load it to IDS 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                               Project/                                                                                                                        Code                  Date
# MAGIC Developer                    Date                   Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -----------------------              -------------------    -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC sudheer champati        06/10/2014       5128       Originally Programmed                                                                                 Kalyan Neelam   2015-06-17
# MAGIC 
# MAGIC Aishwarya                    01/19/2016       5128       ON_EXCH_FED_PAYMT_TRANS_SK & CRT_RUN_CYC_EXCTN_SK    Jag Yelavarthi    2016-01-19
# MAGIC                                                                              conditions updated in xfm_PKEYgen


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

jdbc_url, jdbc_props = get_db_config(get_widget_value('ids_secret_name',''))
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')

df_db2_K_Onex_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT ON_EXCH_FED_PAYMT_TRANS_CK, ACCTG_DT_SK, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, ON_EXCH_FED_PAYMT_TRANS_SK FROM {IDSOwner}.K_ON_EXCH_FED_PAYMT_TRANS"
    )
    .load()
)

df_ds_Onex_Xfrm = spark.read.parquet(
    f"{adls_path}/ds/ON_EXCH_FED_PYMT_TRANS.{SrcSysCd}.xfrm.{RunID}.parquet"
)

df_Transformer_1 = df_ds_Onex_Xfrm.select(
    col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("ON_EXCH_FED_PAYMT_TRANS_CK").cast("decimal(38,10)").alias("ON_EXCH_FED_PAYMT_TRANS_CK"),
    col("ACCTG_DT_SK").alias("ACCTG_DT_SK"),
    col("SOURCE_SYSTEM_CODE_SK").alias("SOURCE_SYSTEM_CODE_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    col("GRP_SK").alias("GRP_SK"),
    col("PROD_SK").alias("PROD_SK"),
    col("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK"),
    col("COV_STRT_DT_SK").alias("COV_STRT_DT_SK"),
    col("COV_END_DT_SK").alias("COV_END_DT_SK"),
    col("PROD_LOB_NO").alias("PROD_LOB_NO"),
    col("EXCH_MBR_ID").alias("EXCH_MBR_ID"),
    col("SUB_ID").alias("SUB_ID"),
    col("GRGR_ID").alias("GRGR_ID"),
    col("PDPD_ID").alias("PDPD_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("GRGR_NAME").alias("GRGR_NAME")
)

df_cpy_MultiStreams_lnkRemDupDataIn = df_Transformer_1.select(
    col("ON_EXCH_FED_PAYMT_TRANS_CK").alias("ON_EXCH_FED_PAYMT_TRANS_CK"),
    col("ACCTG_DT_SK").alias("ACCTG_DT_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_cpy_MultiStreams_lnkFullDataJnIn = df_Transformer_1.select(
    col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("ON_EXCH_FED_PAYMT_TRANS_CK").alias("ON_EXCH_FED_PAYMT_TRANS_CK"),
    col("ACCTG_DT_SK").alias("ACCTG_DT_SK"),
    col("SOURCE_SYSTEM_CODE_SK").alias("SOURCE_SYSTEM_CODE_SK"),
    col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    col("GRP_SK").alias("GRP_SK"),
    col("PROD_SK").alias("PROD_SK"),
    col("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK"),
    col("COV_STRT_DT_SK").alias("COV_STRT_DT_SK"),
    col("COV_END_DT_SK").alias("COV_END_DT_SK"),
    col("PROD_LOB_NO").alias("PROD_LOB_NO"),
    col("EXCH_MBR_ID").alias("EXCH_MBR_ID"),
    col("SUB_ID").alias("SUB_ID"),
    col("GRGR_ID").alias("GRGR_ID"),
    col("PDPD_ID").alias("PDPD_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("GRGR_NAME").alias("GRGR_NAME")
)

df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_lnkRemDupDataIn,
    ["ON_EXCH_FED_PAYMT_TRANS_CK", "ACCTG_DT_SK", "SRC_SYS_CD"],
    []
)

df_jn_Onex = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_K_Onex_In.alias("lnk_KOnex_Extr"),
    (
        (col("lnkRemDupDataOut.ON_EXCH_FED_PAYMT_TRANS_CK") == col("lnk_KOnex_Extr.ON_EXCH_FED_PAYMT_TRANS_CK"))
        & (col("lnkRemDupDataOut.ACCTG_DT_SK") == col("lnk_KOnex_Extr.ACCTG_DT_SK"))
        & (col("lnkRemDupDataOut.SRC_SYS_CD") == col("lnk_KOnex_Extr.SRC_SYS_CD"))
    ),
    "left"
).select(
    col("lnkRemDupDataOut.ON_EXCH_FED_PAYMT_TRANS_CK").alias("ON_EXCH_FED_PAYMT_TRANS_CK"),
    col("lnkRemDupDataOut.ACCTG_DT_SK").alias("ACCTG_DT_SK"),
    col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_KOnex_Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_KOnex_Extr.ON_EXCH_FED_PAYMT_TRANS_SK").alias("ON_EXCH_FED_PAYMT_TRANS_SK")
)

df_xfm_PKEYgen = df_jn_Onex.withColumn(
    "original_ON_EXCH_FED_PAYMT_TRANS_SK",
    col("ON_EXCH_FED_PAYMT_TRANS_SK")
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    when(
        (col("CRT_RUN_CYC_EXCTN_SK").isNull()) | (col("CRT_RUN_CYC_EXCTN_SK") == 0),
        lit(IDSRunCycle)
    ).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "ON_EXCH_FED_PAYMT_TRANS_SK",
    when(
        (col("ON_EXCH_FED_PAYMT_TRANS_SK").isNull()) | (col("ON_EXCH_FED_PAYMT_TRANS_SK") == 0),
        lit(None).cast("long")
    ).otherwise(col("ON_EXCH_FED_PAYMT_TRANS_SK"))
)

df_enriched = SurrogateKeyGen(
    df_xfm_PKEYgen,
    <DB sequence name>,
    "ON_EXCH_FED_PAYMT_TRANS_SK",
    <schema>,
    <secret_name>
)

df_lnkPKEYxfmOut = df_enriched.withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    lit(IDSRunCycle)
)

df_lnk_KAccum_new = df_lnkPKEYxfmOut.filter(
    (col("original_ON_EXCH_FED_PAYMT_TRANS_SK").isNull())
    | (col("original_ON_EXCH_FED_PAYMT_TRANS_SK") == 0)
).select(
    col("ON_EXCH_FED_PAYMT_TRANS_CK"),
    col("ACCTG_DT_SK"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("ON_EXCH_FED_PAYMT_TRANS_SK")
)

df_lnkPKEYxfmOut = df_lnkPKEYxfmOut.select(
    col("ON_EXCH_FED_PAYMT_TRANS_CK"),
    col("ACCTG_DT_SK"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ON_EXCH_FED_PAYMT_TRANS_SK"),
    col("original_ON_EXCH_FED_PAYMT_TRANS_SK")
)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.FctsIdsOnExchFedPaymtTransPkey_db2_K_Onexch_Load_temp",
    jdbc_url,
    jdbc_props
)

df_lnk_KAccum_new.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option(
    "dbtable", "STAGING.FctsIdsOnExchFedPaymtTransPkey_db2_K_Onexch_Load_temp"
).mode("overwrite").save()

merge_sql = f"""
MERGE {IDSOwner}.K_ON_EXCH_FED_PAYMT_TRANS AS T
USING STAGING.FctsIdsOnExchFedPaymtTransPkey_db2_K_Onexch_Load_temp AS S
ON 
(
  T.ON_EXCH_FED_PAYMT_TRANS_CK = S.ON_EXCH_FED_PAYMT_TRANS_CK
  AND T.ACCTG_DT_SK = S.ACCTG_DT_SK
  AND T.SRC_SYS_CD = S.SRC_SYS_CD
)
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.ON_EXCH_FED_PAYMT_TRANS_SK = S.ON_EXCH_FED_PAYMT_TRANS_SK
WHEN NOT MATCHED THEN
  INSERT (ON_EXCH_FED_PAYMT_TRANS_CK, ACCTG_DT_SK, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, ON_EXCH_FED_PAYMT_TRANS_SK)
  VALUES (S.ON_EXCH_FED_PAYMT_TRANS_CK, S.ACCTG_DT_SK, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.ON_EXCH_FED_PAYMT_TRANS_SK);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_jn_PKEYs = df_cpy_MultiStreams_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    (
        (col("lnkFullDataJnIn.ON_EXCH_FED_PAYMT_TRANS_CK") == col("lnkPKEYxfmOut.ON_EXCH_FED_PAYMT_TRANS_CK"))
        & (col("lnkFullDataJnIn.ACCTG_DT_SK") == col("lnkPKEYxfmOut.ACCTG_DT_SK"))
        & (col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD"))
    ),
    "inner"
).select(
    col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("lnkPKEYxfmOut.ON_EXCH_FED_PAYMT_TRANS_SK").alias("ON_EXCH_FED_PAYMT_TRANS_SK"),
    col("lnkFullDataJnIn.ON_EXCH_FED_PAYMT_TRANS_CK").alias("ON_EXCH_FED_PAYMT_TRANS_CK"),
    col("lnkFullDataJnIn.ACCTG_DT_SK").alias("ACCTG_DT_SK"),
    col("lnkFullDataJnIn.SOURCE_SYSTEM_CODE_SK").alias("SOURCE_SYSTEM_CODE_SK"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnkFullDataJnIn.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    col("lnkFullDataJnIn.GRP_SK").alias("GRP_SK"),
    col("lnkFullDataJnIn.PROD_SK").alias("PROD_SK"),
    col("lnkFullDataJnIn.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK"),
    col("lnkFullDataJnIn.COV_STRT_DT_SK").alias("COV_STRT_DT_SK"),
    col("lnkFullDataJnIn.COV_END_DT_SK").alias("COV_END_DT_SK"),
    col("lnkFullDataJnIn.PROD_LOB_NO").alias("PROD_LOB_NO"),
    col("lnkFullDataJnIn.EXCH_MBR_ID").alias("EXCH_MBR_ID"),
    col("lnkFullDataJnIn.SUB_ID").alias("SUB_ID"),
    col("lnkFullDataJnIn.GRGR_ID").alias("GRGR_ID"),
    col("lnkFullDataJnIn.PDPD_ID").alias("PDPD_ID"),
    col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkFullDataJnIn.GRGR_NAME").alias("GRGR_NAME")
)

df_seq_Onex_PKEY = df_jn_PKEYs.withColumn(
    "ACCTG_DT_SK", rpad(col("ACCTG_DT_SK"), 10, " ")
).withColumn(
    "COV_STRT_DT_SK", rpad(col("COV_STRT_DT_SK"), 10, " ")
).withColumn(
    "COV_END_DT_SK", rpad(col("COV_END_DT_SK"), 10, " ")
).withColumn(
    "PROD_LOB_NO", rpad(col("PROD_LOB_NO"), 4, " ")
).withColumn(
    "GRGR_ID", rpad(col("GRGR_ID"), 8, " ")
).withColumn(
    "PDPD_ID", rpad(col("PDPD_ID"), 8, " ")
).select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "ON_EXCH_FED_PAYMT_TRANS_SK",
    "ON_EXCH_FED_PAYMT_TRANS_CK",
    "ACCTG_DT_SK",
    "SOURCE_SYSTEM_CODE_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "FNCL_LOB_SK",
    "GRP_SK",
    "PROD_SK",
    "RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK",
    "COV_STRT_DT_SK",
    "COV_END_DT_SK",
    "PROD_LOB_NO",
    "EXCH_MBR_ID",
    "SUB_ID",
    "GRGR_ID",
    "PDPD_ID",
    "SRC_SYS_CD",
    "GRGR_NAME"
)

write_files(
    df_seq_Onex_PKEY,
    f"{adls_path}/key/ON_EXCH_FED_PYMT_TRANS.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)