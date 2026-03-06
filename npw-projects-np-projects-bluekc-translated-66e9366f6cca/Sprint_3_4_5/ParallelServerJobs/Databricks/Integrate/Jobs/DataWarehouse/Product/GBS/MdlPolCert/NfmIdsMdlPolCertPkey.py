# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC 
# MAGIC JOb Name : FctsIdsEobAccumPkey
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC DinakarS              2018-07-02               5205                   Original Programming                                                                             IntegrateWrhsDevl            Kalyan Neelam             2018-07-05

# MAGIC Audit fields are added into this File
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Audit fields are added into this File
# MAGIC Table K_MDL_POL_CERT               Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Job Name: IdsMdlPolCertPkey
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, rpad
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')

df_ds_MDL_CERT_DS_1 = (
    spark.read.format("parquet")
    .load(f"{adls_path}/ds/MDLPOLCERT.{SrcSysCd}.xfrm.{RunID}.parquet")
    .select(
        col("MDL_DOC_ID"),
        col("POL_NO"),
        col("POLICY_FORM_ID"),
        col("POLICY_FORM_DT"),
        col("CERT_ID"),
        col("MDL_DOC_EFF_DT"),
        col("CERT_EFF_DT"),
        col("MDL_DOC_TERM_DT"),
        col("SRC_SYS_CD"),
        col("CERT_EFF_ON_GRP_RNWL_IN"),
        col("CERT_STATUS_ID"),
        col("CERT_TERM_DT")
    )
)

df_cpy_MultiStreams_lnkRemDupDataIn = df_ds_MDL_CERT_DS_1.select(
    col("MDL_DOC_ID"),
    col("POL_NO"),
    col("CERT_ID"),
    col("CERT_EFF_DT"),
    col("SRC_SYS_CD")
)

df_cpy_MultiStreams_lnkFullDataJnIn = df_ds_MDL_CERT_DS_1.select(
    col("MDL_DOC_ID"),
    col("POL_NO"),
    col("POLICY_FORM_ID"),
    col("POLICY_FORM_DT"),
    col("CERT_ID"),
    col("MDL_DOC_EFF_DT"),
    col("CERT_EFF_DT"),
    col("MDL_DOC_TERM_DT"),
    col("SRC_SYS_CD"),
    col("CERT_EFF_ON_GRP_RNWL_IN"),
    col("CERT_STATUS_ID"),
    col("CERT_TERM_DT")
)

df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_lnkRemDupDataIn,
    ["MDL_DOC_ID","POL_NO","CERT_ID","CERT_EFF_DT","SRC_SYS_CD"],
    [("MDL_DOC_ID","A"),("POL_NO","A"),("CERT_ID","A"),("CERT_EFF_DT","A"),("SRC_SYS_CD","A")]
)

df_rdp_NaturalKeys_lnkRemDupDataOut = df_rdp_NaturalKeys.select(
    col("MDL_DOC_ID"),
    col("POL_NO"),
    col("CERT_ID"),
    col("CERT_EFF_DT"),
    col("SRC_SYS_CD")
)

jdbc_url_db2_KMdlPolCertExt, jdbc_props_db2_KMdlPolCertExt = get_db_config(ids_secret_name)
extract_query_db2_KMdlPolCertExt = f"SELECT MDL_DOC_ID,POL_NO,CERT_ID,CERT_EFF_DT,SRC_SYS_CD,MDL_POL_CERT_SK,CRT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.K_MDL_POL_CERT"
df_db2_KMdlPolCertExt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_KMdlPolCertExt)
    .options(**jdbc_props_db2_KMdlPolCertExt)
    .option("query", extract_query_db2_KMdlPolCertExt)
    .load()
    .select(
        col("MDL_DOC_ID").alias("MDL_DOC_ID"),
        col("POL_NO").alias("POL_NO"),
        col("CERT_ID").alias("CERT_ID"),
        col("CERT_EFF_DT").alias("CERT_EFF_DT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("MDL_POL_CERT_SK").alias("MDL_POL_CERT_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
    )
)

df_jn_MDL_POL_ACERT = df_rdp_NaturalKeys_lnkRemDupDataOut.alias("lnkRemDupDataOut").join(
    df_db2_KMdlPolCertExt.alias("lnkKMdllCertExt"),
    [
        col("lnkRemDupDataOut.MDL_DOC_ID") == col("lnkKMdllCertExt.MDL_DOC_ID"),
        col("lnkRemDupDataOut.POL_NO") == col("lnkKMdllCertExt.POL_NO"),
        col("lnkRemDupDataOut.CERT_ID") == col("lnkKMdllCertExt.CERT_ID"),
        col("lnkRemDupDataOut.CERT_EFF_DT") == col("lnkKMdllCertExt.CERT_EFF_DT"),
        col("lnkRemDupDataOut.SRC_SYS_CD") == col("lnkKMdllCertExt.SRC_SYS_CD")
    ],
    how="left"
)

df_jn_MDL_POL_ACERT_lnk_MdlPolCertJoinOut = df_jn_MDL_POL_ACERT.select(
    col("lnkRemDupDataOut.MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("lnkRemDupDataOut.POL_NO").alias("POL_NO"),
    col("lnkRemDupDataOut.CERT_ID").alias("CERT_ID"),
    col("lnkRemDupDataOut.CERT_EFF_DT").alias("CERT_EFF_DT"),
    col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkKMdllCertExt.MDL_POL_CERT_SK").alias("MDL_POL_CERT_SK"),
    col("lnkKMdllCertExt.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

df_xfm_PKEYgen = df_jn_MDL_POL_ACERT_lnk_MdlPolCertJoinOut.withColumn(
    "svMdlPolCertSK",
    when( (col("MDL_POL_CERT_SK").isNotNull()) & (col("MDL_POL_CERT_SK") != 0), col("MDL_POL_CERT_SK")).otherwise(F.lit(None))
)

df_xfm_PKEYgen = SurrogateKeyGen(df_xfm_PKEYgen,<DB sequence name>,"svMdlPolCertSK",<schema>,<secret_name>)

df_xfm_PKEYgen_lnk_KMdlPolCert_New = df_xfm_PKEYgen.where(
    (col("MDL_POL_CERT_SK").isNull()) | (col("MDL_POL_CERT_SK") == 0)
).select(
    col("MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("POL_NO").alias("POL_NO"),
    col("CERT_ID").alias("CERT_ID"),
    col("CERT_EFF_DT").alias("CERT_EFF_DT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("svMdlPolCertSK").alias("MDL_POL_CERT_SK"),
    lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

df_xfm_PKEYgen_lnkPKEYxfmOut = df_xfm_PKEYgen.select(
    col("svMdlPolCertSK").alias("MDL_POL_CERT_SK"),
    col("MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("POL_NO").alias("POL_NO"),
    col("CERT_ID").alias("CERT_ID"),
    col("CERT_EFF_DT").alias("CERT_EFF_DT"),
    when((col("MDL_POL_CERT_SK").isNull()) | (col("MDL_POL_CERT_SK") == 0), lit(IDSRunCycle))
    .otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
    .alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

jdbc_url_db2_K_MdlPolCertLoad, jdbc_props_db2_K_MdlPolCertLoad = get_db_config(ids_secret_name)
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.NfmIdsMdlPolCertPkey_db2_K_MdlPolCertLoad_temp",
    jdbc_url_db2_K_MdlPolCertLoad,
    jdbc_props_db2_K_MdlPolCertLoad
)

df_xfm_PKEYgen_lnk_KMdlPolCert_New.write \
  .jdbc(
    url=jdbc_url_db2_K_MdlPolCertLoad,
    table="STAGING.NfmIdsMdlPolCertPkey_db2_K_MdlPolCertLoad_temp",
    mode="overwrite",
    properties=jdbc_props_db2_K_MdlPolCertLoad
  )

merge_sql_db2_K_MdlPolCertLoad = f"""
MERGE INTO {IDSOwner}.K_MDL_POL_CERT AS T
USING STAGING.NfmIdsMdlPolCertPkey_db2_K_MdlPolCertLoad_temp AS S
ON
  T.MDL_DOC_ID=S.MDL_DOC_ID
  AND T.POL_NO=S.POL_NO
  AND T.CERT_ID=S.CERT_ID
  AND T.CERT_EFF_DT=S.CERT_EFF_DT
  AND T.SRC_SYS_CD=S.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET
    T.MDL_POL_CERT_SK = S.MDL_POL_CERT_SK,
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT
    (MDL_DOC_ID, POL_NO, CERT_ID, CERT_EFF_DT, SRC_SYS_CD, MDL_POL_CERT_SK, CRT_RUN_CYC_EXCTN_SK)
  VALUES
    (S.MDL_DOC_ID, S.POL_NO, S.CERT_ID, S.CERT_EFF_DT, S.SRC_SYS_CD, S.MDL_POL_CERT_SK, S.CRT_RUN_CYC_EXCTN_SK);
"""

execute_dml(merge_sql_db2_K_MdlPolCertLoad, jdbc_url_db2_K_MdlPolCertLoad, jdbc_props_db2_K_MdlPolCertLoad)

df_jn_PKEYs = df_cpy_MultiStreams_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_xfm_PKEYgen_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    [
        col("lnkFullDataJnIn.MDL_DOC_ID") == col("lnkPKEYxfmOut.MDL_DOC_ID"),
        col("lnkFullDataJnIn.POL_NO") == col("lnkPKEYxfmOut.POL_NO"),
        col("lnkFullDataJnIn.CERT_ID") == col("lnkPKEYxfmOut.CERT_ID"),
        col("lnkFullDataJnIn.CERT_EFF_DT") == col("lnkPKEYxfmOut.CERT_EFF_DT"),
        col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD")
    ],
    how="inner"
)

df_jn_PKEYs_Lnk_IdsMdlPolCertPkey_OutAbc = df_jn_PKEYs.select(
    col("lnkPKEYxfmOut.MDL_POL_CERT_SK").alias("MDL_POL_CERT_SK"),
    col("lnkFullDataJnIn.MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("lnkFullDataJnIn.POL_NO").alias("POL_NO"),
    col("lnkFullDataJnIn.CERT_ID").alias("CERT_ID"),
    col("lnkFullDataJnIn.CERT_EFF_DT").alias("CERT_EFF_DT"),
    col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnkFullDataJnIn.CERT_EFF_ON_GRP_RNWL_IN").alias("CERT_EFF_ON_GRP_RNWL_IN"),
    col("lnkFullDataJnIn.CERT_STATUS_ID").alias("CERT_STTUS_ID"),
    col("lnkFullDataJnIn.CERT_TERM_DT").alias("CERT_TERM_DT"),
    col("lnkFullDataJnIn.MDL_DOC_EFF_DT").alias("MDL_DOC_EFF_DT")
)

df_seq_MDL_POL_CERT_PKEY = df_jn_PKEYs_Lnk_IdsMdlPolCertPkey_OutAbc.select(
    rpad(col("MDL_POL_CERT_SK").cast("string"), <...>, " ").alias("MDL_POL_CERT_SK"),
    rpad(col("MDL_DOC_ID").cast("string"), <...>, " ").alias("MDL_DOC_ID"),
    rpad(col("POL_NO").cast("string"), <...>, " ").alias("POL_NO"),
    rpad(col("CERT_ID").cast("string"), <...>, " ").alias("CERT_ID"),
    rpad(col("CERT_EFF_DT").cast("string"), <...>, " ").alias("CERT_EFF_DT"),
    rpad(col("SRC_SYS_CD").cast("string"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("CRT_RUN_CYC_EXCTN_SK").cast("string"), <...>, " ").alias("CRT_RUN_CYC_EXCTN_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast("string"), <...>, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("CERT_EFF_ON_GRP_RNWL_IN").cast("string"), <...>, " ").alias("CERT_EFF_ON_GRP_RNWL_IN"),
    rpad(col("CERT_STTUS_ID").cast("string"), <...>, " ").alias("CERT_STTUS_ID"),
    rpad(col("CERT_TERM_DT").cast("string"), <...>, " ").alias("CERT_TERM_DT"),
    rpad(col("MDL_DOC_EFF_DT").cast("string"), <...>, " ").alias("MDL_DOC_EFF_DT")
)

write_files(
    df_seq_MDL_POL_CERT_PKEY,
    f"{adls_path}/key/MDLPOLCERT.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)