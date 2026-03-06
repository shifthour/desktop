# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                                                        DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                                 PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Pooja Sunkara          2014-08-25                          5345                             Original Programming                                                                        IntegrateWrhsDevl      Kalyan Neelam           2015-01-02
# MAGIC 
# MAGIC Saikiran S                   2019-01-15                          5887                            Added the TXNMY_CD column                                                        IntegrateDev1            Kalyan Neelam           2019-02-11

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_PROV.Insert new SKEY rows generated as part of this run. This is a database insert operation.
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')

df_ds_PROV_Xfrm = spark.read.parquet(f"{adls_path}/ds/PROV.{SrcSysCd}.xfrm.{RunID}.parquet")

df_cpy_MultiStreams = df_ds_PROV_Xfrm

df_IdsProvPkey_All = df_cpy_MultiStreams.select(
    F.col("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("PROV_ID"),
    F.col("SRC_SYS_CD"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CMN_PRCT"),
    F.col("REL_GRP_PROV"),
    F.col("REL_IPA_PROV"),
    F.col("PROV_CAP_PAYMT_EFT_METH_CD"),
    F.col("PROV_CLM_PAYMT_EFT_METH_CD"),
    F.col("PROV_CLM_PAYMT_METH_CD"),
    F.col("PROV_ENTY_CD"),
    F.col("PROV_FCLTY_TYP_CD"),
    F.col("PROV_PRCTC_TYP_CD"),
    F.col("PROV_SVC_CAT_CD"),
    F.col("PROV_SPEC_CD"),
    F.col("PROV_STTUS_CD"),
    F.col("PROV_TERM_RSN_CD"),
    F.col("PROV_TYP_CD"),
    F.col("TERM_DT"),
    F.col("PAYMT_HOLD_DT"),
    F.col("CLRNGHOUSE_ID"),
    F.col("EDI_DEST_ID"),
    F.col("EDI_DEST_QUAL"),
    F.col("NTNL_PROV_ID"),
    F.col("PROV_ADDR_ID"),
    F.col("PROV_NM"),
    F.col("TAX_ID"),
    F.col("TXNMY_CD")
)

df_FctsIdsProvPkey_Dedup = df_cpy_MultiStreams.select(
    F.col("PROV_ID"),
    F.col("SRC_SYS_CD")
)

df_rdp_NaturalKeys = dedup_sort(
    df_FctsIdsProvPkey_Dedup,
    ["PROV_ID","SRC_SYS_CD"],
    []
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT PROV_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, PROV_SK FROM {IDSOwner}.K_PROV"
df_db2_K_PROV_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_jn_Prov = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_K_PROV_In.alias("lnk_KProv_extr"),
    [
        F.col("lnkRemDupDataOut.PROV_ID") == F.col("lnk_KProv_extr.PROV_ID"),
        F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("lnk_KProv_extr.SRC_SYS_CD")
    ],
    "left"
)

df_jn_Prov_select = df_jn_Prov.select(
    F.col("lnkRemDupDataOut.PROV_ID").alias("PROV_ID"),
    F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_KProv_extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_KProv_extr.PROV_SK").alias("PROV_SK")
)

df_xfm_PKEYgen_in = df_jn_Prov_select.withColumnRenamed("CRT_RUN_CYC_EXCTN_SK","orig_CRT_RUN_CYC_EXCTN_SK") \
    .withColumnRenamed("PROV_SK","orig_PROV_SK") \
    .withColumn("sk_is_null", F.col("orig_PROV_SK").isNull()) \
    .withColumn("PROV_SK", F.col("orig_PROV_SK")) \
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("orig_CRT_RUN_CYC_EXCTN_SK"))

df_enriched = SurrogateKeyGen(df_xfm_PKEYgen_in,<DB sequence name>,'PROV_SK',<schema>,<secret_name>)

df_lnk_KProv_New = df_enriched.filter(F.col("sk_is_null") == True).select(
    F.col("PROV_ID"),
    F.col("SRC_SYS_CD"),
    F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_SK")
)

df_lnkPKEYxfmOut = df_enriched.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.when(F.col("sk_is_null"), F.lit(IDSRunCycle)).otherwise(F.col("orig_CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_SK").alias("PROV_SK")
)

temp_table = "STAGING.IdsProvPkey_db2_K_PROV_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url, jdbc_props)

df_lnk_KProv_New.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.K_PROV AS T
USING {temp_table} AS S
ON T.PROV_ID = S.PROV_ID AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
  T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
  T.PROV_SK = S.PROV_SK
WHEN NOT MATCHED THEN INSERT (
  PROV_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, PROV_SK
) VALUES (
  S.PROV_ID, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.PROV_SK
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_jn_PKEYs = df_IdsProvPkey_All.alias("lnk_IdsProvPkey_All").join(
    df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    [
        F.col("lnk_IdsProvPkey_All.PROV_ID") == F.col("lnkPKEYxfmOut.PROV_ID"),
        F.col("lnk_IdsProvPkey_All.SRC_SYS_CD") == F.col("lnkPKEYxfmOut.SRC_SYS_CD")
    ],
    "inner"
)

df_jn_PKEYs_select = df_jn_PKEYs.select(
    F.col("lnk_IdsProvPkey_All.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("lnk_IdsProvPkey_All.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("lnkPKEYxfmOut.PROV_SK").alias("PROV_SK"),
    F.col("lnk_IdsProvPkey_All.PROV_ID").alias("PROV_ID"),
    F.col("lnk_IdsProvPkey_All.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_IdsProvPkey_All.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnk_IdsProvPkey_All.CMN_PRCT").alias("CMN_PRCT"),
    F.col("lnk_IdsProvPkey_All.REL_GRP_PROV").alias("REL_GRP_PROV"),
    F.col("lnk_IdsProvPkey_All.REL_IPA_PROV").alias("REL_IPA_PROV"),
    F.col("lnk_IdsProvPkey_All.PROV_CAP_PAYMT_EFT_METH_CD").alias("PROV_CAP_PAYMT_EFT_METH_CD"),
    F.col("lnk_IdsProvPkey_All.PROV_CLM_PAYMT_EFT_METH_CD").alias("PROV_CLM_PAYMT_EFT_METH_CD"),
    F.col("lnk_IdsProvPkey_All.PROV_CLM_PAYMT_METH_CD").alias("PROV_CLM_PAYMT_METH_CD"),
    F.col("lnk_IdsProvPkey_All.PROV_ENTY_CD").alias("PROV_ENTY_CD"),
    F.col("lnk_IdsProvPkey_All.PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYP_CD"),
    F.col("lnk_IdsProvPkey_All.PROV_PRCTC_TYP_CD").alias("PROV_PRCTC_TYP_CD"),
    F.col("lnk_IdsProvPkey_All.PROV_SVC_CAT_CD").alias("PROV_SVC_CAT_CD"),
    F.col("lnk_IdsProvPkey_All.PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    F.col("lnk_IdsProvPkey_All.PROV_STTUS_CD").alias("PROV_STTUS_CD"),
    F.col("lnk_IdsProvPkey_All.PROV_TERM_RSN_CD").alias("PROV_TERM_RSN_CD"),
    F.col("lnk_IdsProvPkey_All.PROV_TYP_CD").alias("PROV_TYP_CD"),
    F.col("lnk_IdsProvPkey_All.TERM_DT").alias("TERM_DT"),
    F.col("lnk_IdsProvPkey_All.PAYMT_HOLD_DT").alias("PAYMT_HOLD_DT"),
    F.col("lnk_IdsProvPkey_All.CLRNGHOUSE_ID").alias("CLRNGHOUSE_ID"),
    F.col("lnk_IdsProvPkey_All.EDI_DEST_ID").alias("EDI_DEST_ID"),
    F.col("lnk_IdsProvPkey_All.EDI_DEST_QUAL").alias("EDI_DEST_QUAL"),
    F.col("lnk_IdsProvPkey_All.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("lnk_IdsProvPkey_All.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("lnk_IdsProvPkey_All.PROV_NM").alias("PROV_NM"),
    F.col("lnk_IdsProvPkey_All.TAX_ID").alias("TAX_ID"),
    F.col("lnk_IdsProvPkey_All.TXNMY_CD").alias("TXNMY_CD")
)

df_seq_PROV_Pkey = df_jn_PKEYs_select.withColumn(
    "TERM_DT", F.rpad("TERM_DT", 10, " ")
).withColumn(
    "PAYMT_HOLD_DT", F.rpad("PAYMT_HOLD_DT", 10, " ")
).select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "PROV_SK",
    "PROV_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SRC_SYS_CD_SK",
    "CMN_PRCT",
    "REL_GRP_PROV",
    "REL_IPA_PROV",
    "PROV_CAP_PAYMT_EFT_METH_CD",
    "PROV_CLM_PAYMT_EFT_METH_CD",
    "PROV_CLM_PAYMT_METH_CD",
    "PROV_ENTY_CD",
    "PROV_FCLTY_TYP_CD",
    "PROV_PRCTC_TYP_CD",
    "PROV_SVC_CAT_CD",
    "PROV_SPEC_CD",
    "PROV_STTUS_CD",
    "PROV_TERM_RSN_CD",
    "PROV_TYP_CD",
    "TERM_DT",
    "PAYMT_HOLD_DT",
    "CLRNGHOUSE_ID",
    "EDI_DEST_ID",
    "EDI_DEST_QUAL",
    "NTNL_PROV_ID",
    "PROV_ADDR_ID",
    "PROV_NM",
    "TAX_ID",
    "TXNMY_CD"
)

write_files(
    df_seq_PROV_Pkey,
    f"{adls_path}/key/PROV.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)