# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Ravi Abburi             2017-11- 01          5781                              Original Programming                                                                             IntegrateDev2           Kalyan Neelam             2018-01-26

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

df_ds_NtnlProv_Xfrm = spark.read.parquet(f"{adls_path}/ds/NTNL_PROV.{SrcSysCd}.extr.{RunID}.parquet")

df_cpy_MultiStreams = df_ds_NtnlProv_Xfrm

df_lnk_NtnlProvPkey_All = df_cpy_MultiStreams.select(
    "NTNL_PROV_ID",
    "SRC_SYS_CD",
    "ENTITY_TYPE_CODE",
    "PROVIDER_GENDER_CODE",
    "IS_SOLE_PROPRIETOR",
    "NPI_DEACTIVATION_REASON_CODE",
    "NTNL_PROV_DCTVTN_DT",
    "NTNL_PROV_ENMRTN_DT",
    "NTNL_PROV_RCTVTN_DT",
    "NTNL_PROV_SRC_LAST_UPDT_DT",
    "NTNL_PROV_CRDTL_TX",
    "NTNL_PROV_FIRST_NM",
    "NTNL_PROV_LAST_NM",
    "NTNL_PROV_MID_NM",
    "NTNL_PROV_NM_PFX_TX",
    "NTNL_PROV_NM_SFX_TX",
    "NTNL_PROV_OTHR_CRDTL_TX",
    "NTNL_PROV_OTHR_FIRST_NM",
    "NTNL_PROV_OTHR_LAST_NM",
    "NTNL_PROV_OTHR_MID_NM",
    "NTNL_PROV_OTHR_NM_PFX_TX",
    "NTNL_PROV_OTHR_NM_SFX_TX",
    "PROV_OTHER_LAST_NAME_TYPE_CODE",
    "NTNL_PROV_ORG_LGL_BUS_NM",
    "NTNL_PROV_OTHR_ORG_NM",
    "PROV_OTHER_ORGANIZATION_NAME_TYPE_CODE",
    "IS_ORGANIZATION_SUBPART",
    "NTNL_PROV_ORG_PRNT_LGL_BUS_NM",
    "NTNL_PROV_ORG_PRNT_TAX_ID",
    "NTNL_PROV_MAIL_ADDR_LN_1",
    "NTNL_PROV_MAIL_ADDR_LN_2",
    "NTNL_PROV_MAIL_ADDR_CITY_NM",
    "PROV_BUS_MAILING_ADDR_STATE_NAME",
    "NTNL_PROV_MAIL_ADDR_POSTAL_CD",
    "PROV_BUS_MAILING_ADDR_COUNTRY_CODE",
    "NTNL_PROV_MAIL_ADDR_FAX_NO",
    "NTNL_PROV_MAIL_ADDR_TEL_NO",
    "NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_1",
    "NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_2",
    "NTNL_PROV_PRCTC_PRI_LOC_ADDR_CITY_NM",
    "PROV_BUS_PRACTICE_LOC_ADDR_STATE_NAME",
    "NTNL_PROV_PRCTC_PRI_LOC_ADDR_POSTAL_CD",
    "PROV_BUS_PRACTICE_LOC_ADDR_COUNTRY_CODE",
    "NTNL_PROV_PRCTC_PRI_LOC_ADDR_FAX_NO",
    "NTNL_PROV_PRCTC_PRI_LOC_ADDR_TEL_NO",
    "NTNL_PROV_ORG_EMPLR_ID_NO",
    "RPLMT_NTNL_PROV_ID"
)

df_lnk_IdsNtnlProvPkey_Dedup = df_cpy_MultiStreams.select(
    "NTNL_PROV_ID",
    "SRC_SYS_CD"
)

df_rdp_NaturalKeys = dedup_sort(
    df_lnk_IdsNtnlProvPkey_Dedup,
    ["NTNL_PROV_ID", "SRC_SYS_CD"],
    []
)
df_lnkRemDupDataOut = df_rdp_NaturalKeys

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT NTNL_PROV_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, NTNL_PROV_SK FROM {IDSOwner}.K_NTNL_PROV"
df_db2_K_NTNL_PROV_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_jn_Prov = df_lnkRemDupDataOut.alias("left").join(
    df_db2_K_NTNL_PROV_In.alias("right"),
    (F.col("left.NTNL_PROV_ID") == F.col("right.NTNL_PROV_ID")) & (F.col("left.SRC_SYS_CD") == F.col("right.SRC_SYS_CD")),
    "left"
).select(
    F.col("left.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("left.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("right.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("right.NTNL_PROV_SK").alias("NTNL_PROV_SK")
)

df_xfm_PKEYgen_in = df_jn_Prov

df_xfm_PKEYgen_in = df_xfm_PKEYgen_in.withColumn("orig_NTNL_PROV_SK", F.col("NTNL_PROV_SK"))
df_enriched = df_xfm_PKEYgen_in
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"NTNL_PROV_SK",<schema>,<secret_name>)

df_lnk_KNtnlProv_New = df_enriched.filter(
    F.col("orig_NTNL_PROV_SK").isNull()
).select(
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("NTNL_PROV_SK").alias("NTNL_PROV_SK")
)

df_lnkPKEYxfmOut = df_enriched.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.when(F.col("orig_NTNL_PROV_SK").isNull(), F.lit(IDSRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("NTNL_PROV_SK").alias("NTNL_PROV_SK")
)

temp_table_name_db2_K_NTNL_PROV_Load = "STAGING.IdsNtnlProvPkey_db2_K_NTNL_PROV_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name_db2_K_NTNL_PROV_Load}", jdbc_url, jdbc_props)
(
    df_lnk_KNtnlProv_New.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name_db2_K_NTNL_PROV_Load)
    .mode("overwrite")
    .save()
)
merge_sql_db2_K_NTNL_PROV_Load = f"""
MERGE INTO {IDSOwner}.K_NTNL_PROV AS T
USING {temp_table_name_db2_K_NTNL_PROV_Load} AS S
ON (T.NTNL_PROV_ID = S.NTNL_PROV_ID AND T.SRC_SYS_CD = S.SRC_SYS_CD)
WHEN MATCHED THEN
  UPDATE SET
    T.NTNL_PROV_ID = T.NTNL_PROV_ID
WHEN NOT MATCHED THEN
  INSERT
  (
    NTNL_PROV_ID,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    NTNL_PROV_SK
  )
  VALUES
  (
    S.NTNL_PROV_ID,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.NTNL_PROV_SK
  );
"""
execute_dml(merge_sql_db2_K_NTNL_PROV_Load, jdbc_url, jdbc_props)

df_jn_PKEYs = df_lnk_NtnlProvPkey_All.alias("left").join(
    df_lnkPKEYxfmOut.alias("right"),
    (F.col("left.NTNL_PROV_ID") == F.col("right.NTNL_PROV_ID")) & (F.col("left.SRC_SYS_CD") == F.col("right.SRC_SYS_CD")),
    "inner"
).select(
    F.col("right.NTNL_PROV_SK").alias("NTNL_PROV_SK"),
    F.col("left.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("left.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("right.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("right.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("left.ENTITY_TYPE_CODE").alias("ENTITY_TYPE_CODE"),
    F.col("left.PROVIDER_GENDER_CODE").alias("PROVIDER_GENDER_CODE"),
    F.col("left.IS_SOLE_PROPRIETOR").alias("IS_SOLE_PROPRIETOR"),
    F.col("left.NPI_DEACTIVATION_REASON_CODE").alias("NPI_DEACTIVATION_REASON_CODE"),
    F.col("left.NTNL_PROV_DCTVTN_DT").alias("NTNL_PROV_DCTVTN_DT"),
    F.col("left.NTNL_PROV_ENMRTN_DT").alias("NTNL_PROV_ENMRTN_DT"),
    F.col("left.NTNL_PROV_RCTVTN_DT").alias("NTNL_PROV_RCTVTN_DT"),
    F.col("left.NTNL_PROV_SRC_LAST_UPDT_DT").alias("NTNL_PROV_SRC_LAST_UPDT_DT"),
    F.col("left.NTNL_PROV_CRDTL_TX").alias("NTNL_PROV_CRDTL_TX"),
    F.col("left.NTNL_PROV_FIRST_NM").alias("NTNL_PROV_FIRST_NM"),
    F.col("left.NTNL_PROV_LAST_NM").alias("NTNL_PROV_LAST_NM"),
    F.col("left.NTNL_PROV_MID_NM").alias("NTNL_PROV_MID_NM"),
    F.col("left.NTNL_PROV_NM_PFX_TX").alias("NTNL_PROV_NM_PFX_TX"),
    F.col("left.NTNL_PROV_NM_SFX_TX").alias("NTNL_PROV_NM_SFX_TX"),
    F.col("left.NTNL_PROV_OTHR_CRDTL_TX").alias("NTNL_PROV_OTHR_CRDTL_TX"),
    F.col("left.NTNL_PROV_OTHR_FIRST_NM").alias("NTNL_PROV_OTHR_FIRST_NM"),
    F.col("left.NTNL_PROV_OTHR_LAST_NM").alias("NTNL_PROV_OTHR_LAST_NM"),
    F.col("left.NTNL_PROV_OTHR_MID_NM").alias("NTNL_PROV_OTHR_MID_NM"),
    F.col("left.NTNL_PROV_OTHR_NM_PFX_TX").alias("NTNL_PROV_OTHR_NM_PFX_TX"),
    F.col("left.NTNL_PROV_OTHR_NM_SFX_TX").alias("NTNL_PROV_OTHR_NM_SFX_TX"),
    F.col("left.PROV_OTHER_LAST_NAME_TYPE_CODE").alias("PROV_OTHER_LAST_NAME_TYPE_CODE"),
    F.col("left.NTNL_PROV_ORG_LGL_BUS_NM").alias("NTNL_PROV_ORG_LGL_BUS_NM"),
    F.col("left.NTNL_PROV_OTHR_ORG_NM").alias("NTNL_PROV_OTHR_ORG_NM"),
    F.col("left.PROV_OTHER_ORGANIZATION_NAME_TYPE_CODE").alias("PROV_OTHER_ORGANIZATION_NAME_TYPE_CODE"),
    F.col("left.IS_ORGANIZATION_SUBPART").alias("IS_ORGANIZATION_SUBPART"),
    F.col("left.NTNL_PROV_ORG_PRNT_LGL_BUS_NM").alias("NTNL_PROV_ORG_PRNT_LGL_BUS_NM"),
    F.col("left.NTNL_PROV_ORG_PRNT_TAX_ID").alias("NTNL_PROV_ORG_PRNT_TAX_ID"),
    F.col("left.NTNL_PROV_MAIL_ADDR_LN_1").alias("NTNL_PROV_MAIL_ADDR_LN_1"),
    F.col("left.NTNL_PROV_MAIL_ADDR_LN_2").alias("NTNL_PROV_MAIL_ADDR_LN_2"),
    F.col("left.NTNL_PROV_MAIL_ADDR_CITY_NM").alias("NTNL_PROV_MAIL_ADDR_CITY_NM"),
    F.col("left.PROV_BUS_MAILING_ADDR_STATE_NAME").alias("PROV_BUS_MAILING_ADDR_STATE_NAME"),
    F.col("left.NTNL_PROV_MAIL_ADDR_POSTAL_CD").alias("NTNL_PROV_MAIL_ADDR_POSTAL_CD"),
    F.col("left.PROV_BUS_MAILING_ADDR_COUNTRY_CODE").alias("PROV_BUS_MAILING_ADDR_COUNTRY_CODE"),
    F.col("left.NTNL_PROV_MAIL_ADDR_FAX_NO").alias("NTNL_PROV_MAIL_ADDR_FAX_NO"),
    F.col("left.NTNL_PROV_MAIL_ADDR_TEL_NO").alias("NTNL_PROV_MAIL_ADDR_TEL_NO"),
    F.col("left.NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_1").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_1"),
    F.col("left.NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_2").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_2"),
    F.col("left.NTNL_PROV_PRCTC_PRI_LOC_ADDR_CITY_NM").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CITY_NM"),
    F.col("left.PROV_BUS_PRACTICE_LOC_ADDR_STATE_NAME").alias("PROV_BUS_PRACTICE_LOC_ADDR_STATE_NAME"),
    F.col("left.NTNL_PROV_PRCTC_PRI_LOC_ADDR_POSTAL_CD").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_POSTAL_CD"),
    F.col("left.PROV_BUS_PRACTICE_LOC_ADDR_COUNTRY_CODE").alias("PROV_BUS_PRACTICE_LOC_ADDR_COUNTRY_CODE"),
    F.col("left.NTNL_PROV_PRCTC_PRI_LOC_ADDR_FAX_NO").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_FAX_NO"),
    F.col("left.NTNL_PROV_PRCTC_PRI_LOC_ADDR_TEL_NO").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_TEL_NO"),
    F.col("left.NTNL_PROV_ORG_EMPLR_ID_NO").alias("NTNL_PROV_ORG_EMPLR_ID_NO"),
    F.col("left.RPLMT_NTNL_PROV_ID").alias("RPLMT_NTNL_PROV_ID")
)

df_seq_NTNL_PROV_Pkey = df_jn_PKEYs.select(
    "NTNL_PROV_SK",
    "NTNL_PROV_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ENTITY_TYPE_CODE",
    "PROVIDER_GENDER_CODE",
    "IS_SOLE_PROPRIETOR",
    "NPI_DEACTIVATION_REASON_CODE",
    "NTNL_PROV_DCTVTN_DT",
    "NTNL_PROV_ENMRTN_DT",
    "NTNL_PROV_RCTVTN_DT",
    "NTNL_PROV_SRC_LAST_UPDT_DT",
    "NTNL_PROV_CRDTL_TX",
    "NTNL_PROV_FIRST_NM",
    "NTNL_PROV_LAST_NM",
    "NTNL_PROV_MID_NM",
    "NTNL_PROV_NM_PFX_TX",
    "NTNL_PROV_NM_SFX_TX",
    "NTNL_PROV_OTHR_CRDTL_TX",
    "NTNL_PROV_OTHR_FIRST_NM",
    "NTNL_PROV_OTHR_LAST_NM",
    "NTNL_PROV_OTHR_MID_NM",
    "NTNL_PROV_OTHR_NM_PFX_TX",
    "NTNL_PROV_OTHR_NM_SFX_TX",
    "PROV_OTHER_LAST_NAME_TYPE_CODE",
    "NTNL_PROV_ORG_LGL_BUS_NM",
    "NTNL_PROV_OTHR_ORG_NM",
    "PROV_OTHER_ORGANIZATION_NAME_TYPE_CODE",
    "IS_ORGANIZATION_SUBPART",
    "NTNL_PROV_ORG_PRNT_LGL_BUS_NM",
    "NTNL_PROV_ORG_PRNT_TAX_ID",
    "NTNL_PROV_MAIL_ADDR_LN_1",
    "NTNL_PROV_MAIL_ADDR_LN_2",
    "NTNL_PROV_MAIL_ADDR_CITY_NM",
    "PROV_BUS_MAILING_ADDR_STATE_NAME",
    "NTNL_PROV_MAIL_ADDR_POSTAL_CD",
    "PROV_BUS_MAILING_ADDR_COUNTRY_CODE",
    "NTNL_PROV_MAIL_ADDR_FAX_NO",
    "NTNL_PROV_MAIL_ADDR_TEL_NO",
    "NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_1",
    "NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_2",
    "NTNL_PROV_PRCTC_PRI_LOC_ADDR_CITY_NM",
    "PROV_BUS_PRACTICE_LOC_ADDR_STATE_NAME",
    "NTNL_PROV_PRCTC_PRI_LOC_ADDR_POSTAL_CD",
    "PROV_BUS_PRACTICE_LOC_ADDR_COUNTRY_CODE",
    "NTNL_PROV_PRCTC_PRI_LOC_ADDR_FAX_NO",
    "NTNL_PROV_PRCTC_PRI_LOC_ADDR_TEL_NO",
    "NTNL_PROV_ORG_EMPLR_ID_NO",
    "RPLMT_NTNL_PROV_ID"
)

write_files(
    df_seq_NTNL_PROV_Pkey,
    f"{adls_path}/key/NTNL_PROV.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)