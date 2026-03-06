# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC 
# MAGIC JOb Name : NfmIdsGrpMdlPolProdPkey
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Dinakars           2018-07-02               5205                   Original Programming                                                                             IntegrateWrhsDevl            Kalyan Neelam            2018-07-05

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Job Name: IdsGrpMdlPolProdPkey
# MAGIC Audit fields are added into this File
# MAGIC Table K_GRP_MDL_POL_PROD               Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Audit fields are added into this File
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
from pyspark.sql.functions import col, when, lit, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSDB = get_widget_value('IDSDB','')
IDSAcct = get_widget_value('IDSAcct','')
IDSPW = get_widget_value('IDSPW','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_KGrpMdlPolProd = """SELECT 
GRP_ID,
MDL_DOC_ID,
POL_NO,
CNTR_ID,
CERT_ID,
AMNDMNT_ID,
PROD_ID,
GRP_MDL_POL_PROD_EFF_DT,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_SK,
GRP_MDL_POL_PROD_SK
FROM {}.K_GRP_MDL_POL_PROD 
""".format(IDSOwner)

df_db2_KGrpMdlPolProd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_KGrpMdlPolProd)
    .load()
)

df_jn_GRP_MDLPOL_PROD__lnkKGrpMdlPolProdExt = df_db2_KGrpMdlPolProd

df_ds_GRP_MDL_POL_PROD = spark.read.parquet(f"{adls_path}/ds/GRPMDLPOLPROD.{SrcSysCd}.xfrm.{RunID}.parquet")

df_cpy_MultiStreams__lnkRemDupDataIn = df_ds_GRP_MDL_POL_PROD.select(
    col("GRP_ID").alias("GRP_ID"),
    col("MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("POL_NO").alias("POL_NO"),
    col("CNTR_ID").alias("CNTR_ID"),
    col("CERT_ID").alias("CERT_ID"),
    col("AMNDMNT_ID").alias("AMNDMNT_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PROD_ID").alias("PROD_ID"),
    col("GRP_MDL_POL_PROD_EFF_DT").alias("GRP_MDL_POL_PROD_EFF_DT")
)

df_cpy_MultiStreams__lnkFullDataJnIn = df_ds_GRP_MDL_POL_PROD.select(
    col("GRP_ID").alias("GRP_ID"),
    col("MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("POL_NO").alias("POL_NO"),
    col("CNTR_ID").alias("CNTR_ID"),
    col("CERT_ID").alias("CERT_ID"),
    col("AMNDMNT_ID").alias("AMNDMNT_ID"),
    col("GRP_MDL_POL_PROD_EFF_DT").alias("GRP_MDL_POL_PROD_EFF_DT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("POLICY_FORM_ID").alias("POLICY_FORM_ID"),
    col("POLICY_FORM_DT").alias("POLICY_FORM_DT"),
    col("GRP_MDL_POL_PROD_TERM_DT").alias("GRP_MDL_POL_PROD_TERM_DT"),
    col("PROD_ID").alias("PROD_ID")
)

df_rdp_NaturalKeys__lnkRemDupDataOut = dedup_sort(
    df_cpy_MultiStreams__lnkRemDupDataIn,
    partition_cols=[
        "GRP_ID",
        "MDL_DOC_ID",
        "POL_NO",
        "CNTR_ID",
        "CERT_ID",
        "AMNDMNT_ID",
        "PROD_ID",
        "GRP_MDL_POL_PROD_EFF_DT",
        "SRC_SYS_CD"
    ],
    sort_cols=[]
)

df_rdp_NaturalKeys__lnkRemDupDataOut = df_rdp_NaturalKeys__lnkRemDupDataOut.select(
    col("GRP_ID").alias("GRP_ID"),
    col("MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("POL_NO").alias("POL_NO"),
    col("CNTR_ID").alias("CNTR_ID"),
    col("CERT_ID").alias("CERT_ID"),
    col("AMNDMNT_ID").alias("AMNDMNT_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PROD_ID").alias("PROD_ID"),
    col("GRP_MDL_POL_PROD_EFF_DT").alias("GRP_MDL_POL_PROD_EFF_DT")
)

df_jn_GRP_MDLPOL_PROD = df_rdp_NaturalKeys__lnkRemDupDataOut.alias("lnkRemDupDataOut").join(
    df_jn_GRP_MDLPOL_PROD__lnkKGrpMdlPolProdExt.alias("lnkKGrpMdlPolProdExt"),
    on=[
        col("lnkRemDupDataOut.GRP_ID") == col("lnkKGrpMdlPolProdExt.GRP_ID"),
        col("lnkRemDupDataOut.MDL_DOC_ID") == col("lnkKGrpMdlPolProdExt.MDL_DOC_ID"),
        col("lnkRemDupDataOut.POL_NO") == col("lnkKGrpMdlPolProdExt.POL_NO"),
        col("lnkRemDupDataOut.CNTR_ID") == col("lnkKGrpMdlPolProdExt.CNTR_ID"),
        col("lnkRemDupDataOut.CERT_ID") == col("lnkKGrpMdlPolProdExt.CERT_ID"),
        col("lnkRemDupDataOut.AMNDMNT_ID") == col("lnkKGrpMdlPolProdExt.AMNDMNT_ID"),
        col("lnkRemDupDataOut.PROD_ID") == col("lnkKGrpMdlPolProdExt.PROD_ID"),
        col("lnkRemDupDataOut.GRP_MDL_POL_PROD_EFF_DT") == col("lnkKGrpMdlPolProdExt.GRP_MDL_POL_PROD_EFF_DT"),
        col("lnkRemDupDataOut.SRC_SYS_CD") == col("lnkKGrpMdlPolProdExt.SRC_SYS_CD")
    ],
    how="left"
)

df_jn_GRP_MDLPOL_PROD__lnk_MdlPolCertJoinOut = df_jn_GRP_MDLPOL_PROD.select(
    col("lnkRemDupDataOut.GRP_ID").alias("GRP_ID"),
    col("lnkRemDupDataOut.MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("lnkRemDupDataOut.POL_NO").alias("POL_NO"),
    col("lnkRemDupDataOut.CNTR_ID").alias("CNTR_ID"),
    col("lnkRemDupDataOut.CERT_ID").alias("CERT_ID"),
    col("lnkRemDupDataOut.AMNDMNT_ID").alias("AMNDMNT_ID"),
    col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkRemDupDataOut.PROD_ID").alias("PROD_ID"),
    col("lnkRemDupDataOut.GRP_MDL_POL_PROD_EFF_DT").alias("GRP_MDL_POL_PROD_EFF_DT"),
    col("lnkKGrpMdlPolProdExt.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkKGrpMdlPolProdExt.GRP_MDL_POL_PROD_SK").alias("GRP_MDL_POL_PROD_SK")
)

df_xfm_PKEYgen_input = df_jn_GRP_MDLPOL_PROD__lnk_MdlPolCertJoinOut
df_xfm_PKEYgen_input_1 = df_xfm_PKEYgen_input.withColumn("orig_grp_mdl_pol_prod_sk", col("GRP_MDL_POL_PROD_SK"))

df_enriched = SurrogateKeyGen(df_xfm_PKEYgen_input_1,<DB sequence name>,"GRP_MDL_POL_PROD_SK",<schema>,<secret_name>)

df_enriched_2 = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    when(col("orig_grp_mdl_pol_prod_sk").isNull(), lit(IDSRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK", 
    lit(IDSRunCycle)
)

df_xfm_PKEYgen__lnk_KGrpMdlPolProdNew = df_enriched_2.filter(
    col("orig_grp_mdl_pol_prod_sk").isNull()
).select(
    col("GRP_ID").alias("GRP_ID"),
    col("MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("POL_NO").alias("POL_NO"),
    col("CNTR_ID").alias("CNTR_ID"),
    col("CERT_ID").alias("CERT_ID"),
    col("AMNDMNT_ID").alias("AMNDMNT_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PROD_ID").alias("PROD_ID"),
    col("GRP_MDL_POL_PROD_EFF_DT").alias("GRP_MDL_POL_PROD_EFF_DT"),
    col("GRP_MDL_POL_PROD_SK").alias("GRP_MDL_POL_PROD_SK"),
    lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

df_xfm_PKEYgen__lnkPKEYxfmOut = df_enriched_2.select(
    col("GRP_ID").alias("GRP_ID"),
    col("MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("POL_NO").alias("POL_NO"),
    col("CNTR_ID").alias("CNTR_ID"),
    col("CERT_ID").alias("CERT_ID"),
    col("AMNDMNT_ID").alias("AMNDMNT_ID"),
    col("PROD_ID").alias("PROD_ID"),
    col("GRP_MDL_POL_PROD_EFF_DT").alias("GRP_MDL_POL_PROD_EFF_DT"),
    col("GRP_MDL_POL_PROD_SK").alias("GRP_MDL_POL_PROD_SK"),
    when(col("orig_grp_mdl_pol_prod_sk").isNull(), lit(IDSRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_db2_K_MGrpMdlPolProdLoad_input = df_xfm_PKEYgen__lnk_KGrpMdlPolProdNew.select(
    col("GRP_ID"),
    col("MDL_DOC_ID"),
    col("POL_NO"),
    col("CNTR_ID"),
    col("CERT_ID"),
    col("AMNDMNT_ID"),
    col("SRC_SYS_CD"),
    col("PROD_ID"),
    col("GRP_MDL_POL_PROD_EFF_DT"),
    col("GRP_MDL_POL_PROD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK")
)

drop_temp_table_sql_db2_K_MGrpMdlPolProdLoad = (
    "DROP TABLE IF EXISTS STAGING.NfmIdsGrpMdlPolProdPkey_db2_K_MGrpMdlPolProdLoad_temp"
)
execute_dml(drop_temp_table_sql_db2_K_MGrpMdlPolProdLoad, jdbc_url, jdbc_props)

df_db2_K_MGrpMdlPolProdLoad_input.write.jdbc(
    url=jdbc_url,
    table="STAGING.NfmIdsGrpMdlPolProdPkey_db2_K_MGrpMdlPolProdLoad_temp",
    mode="overwrite",
    properties=jdbc_props
)

merge_sql_db2_K_MGrpMdlPolProdLoad = f"""
MERGE INTO {IDSOwner}.K_GRP_MDL_POL_PROD AS T
USING STAGING.NfmIdsGrpMdlPolProdPkey_db2_K_MGrpMdlPolProdLoad_temp AS S
ON T.GRP_MDL_POL_PROD_SK = S.GRP_MDL_POL_PROD_SK
WHEN MATCHED THEN
  --
WHEN NOT MATCHED THEN
  INSERT (GRP_ID, MDL_DOC_ID, POL_NO, CNTR_ID, CERT_ID, AMNDMNT_ID, SRC_SYS_CD, PROD_ID, GRP_MDL_POL_PROD_EFF_DT, GRP_MDL_POL_PROD_SK, CRT_RUN_CYC_EXCTN_SK)
  VALUES (S.GRP_ID, S.MDL_DOC_ID, S.POL_NO, S.CNTR_ID, S.CERT_ID, S.AMNDMNT_ID, S.SRC_SYS_CD, S.PROD_ID, S.GRP_MDL_POL_PROD_EFF_DT, S.GRP_MDL_POL_PROD_SK, S.CRT_RUN_CYC_EXCTN_SK);
"""
execute_dml(merge_sql_db2_K_MGrpMdlPolProdLoad, jdbc_url, jdbc_props)

df_jn_PKEYs = df_cpy_MultiStreams__lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_xfm_PKEYgen__lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    on=[
        col("lnkFullDataJnIn.GRP_ID") == col("lnkPKEYxfmOut.GRP_ID"),
        col("lnkFullDataJnIn.MDL_DOC_ID") == col("lnkPKEYxfmOut.MDL_DOC_ID"),
        col("lnkFullDataJnIn.POL_NO") == col("lnkPKEYxfmOut.POL_NO"),
        col("lnkFullDataJnIn.CNTR_ID") == col("lnkPKEYxfmOut.CNTR_ID"),
        col("lnkFullDataJnIn.CERT_ID") == col("lnkPKEYxfmOut.CERT_ID"),
        col("lnkFullDataJnIn.AMNDMNT_ID") == col("lnkPKEYxfmOut.AMNDMNT_ID"),
        col("lnkFullDataJnIn.PROD_ID") == col("lnkPKEYxfmOut.PROD_ID"),
        col("lnkFullDataJnIn.GRP_MDL_POL_PROD_EFF_DT") == col("lnkPKEYxfmOut.GRP_MDL_POL_PROD_EFF_DT"),
        col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD")
    ],
    how="inner"
)

df_jn_PKEYs__Lnk_IdsGrpMdlPolProdPkey_OutAbc = df_jn_PKEYs.select(
    col("lnkFullDataJnIn.GRP_ID").alias("GRP_ID"),
    col("lnkFullDataJnIn.MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("lnkFullDataJnIn.POL_NO").alias("POL_NO"),
    col("lnkFullDataJnIn.CNTR_ID").alias("CNTR_ID"),
    col("lnkFullDataJnIn.CERT_ID").alias("CERT_ID"),
    col("lnkFullDataJnIn.AMNDMNT_ID").alias("AMNDMNT_ID"),
    col("lnkFullDataJnIn.GRP_MDL_POL_PROD_EFF_DT").alias("GRP_MDL_POL_PROD_EFF_DT"),
    col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkFullDataJnIn.POLICY_FORM_ID").alias("POLICY_FORM_ID"),
    col("lnkFullDataJnIn.POLICY_FORM_DT").alias("POLICY_FORM_DT"),
    col("lnkFullDataJnIn.GRP_MDL_POL_PROD_TERM_DT").alias("GRP_MDL_POL_PROD_TERM_DT"),
    col("lnkFullDataJnIn.PROD_ID").alias("PROD_ID"),
    col("lnkPKEYxfmOut.GRP_MDL_POL_PROD_SK").alias("GRP_MDL_POL_PROD_SK"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_seq_GRP_MDL_POL_PROD_PKEY_out = df_jn_PKEYs__Lnk_IdsGrpMdlPolProdPkey_OutAbc.select(
    col("GRP_ID"),
    col("MDL_DOC_ID"),
    col("POL_NO"),
    col("CNTR_ID"),
    col("CERT_ID"),
    col("AMNDMNT_ID"),
    col("GRP_MDL_POL_PROD_EFF_DT"),
    col("SRC_SYS_CD"),
    col("POLICY_FORM_ID"),
    col("POLICY_FORM_DT"),
    col("GRP_MDL_POL_PROD_TERM_DT"),
    col("PROD_ID"),
    col("GRP_MDL_POL_PROD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_seq_GRP_MDL_POL_PROD_PKEY_out = (
    df_seq_GRP_MDL_POL_PROD_PKEY_out
    .withColumn("GRP_ID", rpad("GRP_ID", <...>, " "))
    .withColumn("MDL_DOC_ID", rpad("MDL_DOC_ID", <...>, " "))
    .withColumn("POL_NO", rpad("POL_NO", <...>, " "))
    .withColumn("CNTR_ID", rpad("CNTR_ID", <...>, " "))
    .withColumn("CERT_ID", rpad("CERT_ID", <...>, " "))
    .withColumn("AMNDMNT_ID", rpad("AMNDMNT_ID", <...>, " "))
    .withColumn("GRP_MDL_POL_PROD_EFF_DT", rpad("GRP_MDL_POL_PROD_EFF_DT", <...>, " "))
    .withColumn("SRC_SYS_CD", rpad("SRC_SYS_CD", <...>, " "))
    .withColumn("POLICY_FORM_ID", rpad("POLICY_FORM_ID", <...>, " "))
    .withColumn("POLICY_FORM_DT", rpad("POLICY_FORM_DT", <...>, " "))
    .withColumn("GRP_MDL_POL_PROD_TERM_DT", rpad("GRP_MDL_POL_PROD_TERM_DT", <...>, " "))
    .withColumn("PROD_ID", rpad("PROD_ID", <...>, " "))
    .withColumn("GRP_MDL_POL_PROD_SK", rpad("GRP_MDL_POL_PROD_SK", <...>, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", rpad("CRT_RUN_CYC_EXCTN_SK", <...>, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", rpad("LAST_UPDT_RUN_CYC_EXCTN_SK", <...>, " "))
)

write_files(
    df_seq_GRP_MDL_POL_PROD_PKEY_out,
    f"{adls_path}/key/GRP_MDL_POL_PROD.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)