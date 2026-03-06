# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Aditya   Raju                                        5114                              Create extract for EDW Table PROV_PREREQ_D                        EnterpriseWrhsDevl         Peter Marshall                8/29/2013

# MAGIC Write PROV_PREREQ_D Data into a Data Sets. Data Sets used in PROV_D.
# MAGIC Read all the Data from IDS PROV_PREREQ_D Table; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsEdwProvPreDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT distinct LOC.PROV_ID,
ADDR.CNTY_NM,
MPPNG.TRGT_CD
FROM {IDSOwner}.PROV_ADDR ADDR, {IDSOwner}.PROV_LOC LOC, {IDSOwner}.CLNDR_DT DT1, {IDSOwner}.CLNDR_DT DT2, {IDSOwner}.CD_MPPNG MPPNG, {IDSOwner}.cd_mppng MAP2
WHERE ADDR.PROV_ADDR_ID = LOC.PROV_ADDR_ID
  AND LOC.PROV_ADDR_EFF_DT_SK = DT1.CLNDR_DT_SK
  AND ADDR.TERM_DT_SK = DT2.CLNDR_DT_SK
  AND ADDR.SRC_SYS_CD_SK = MAP2.CD_MPPNG_SK
  AND DT1.CLNDR_DT <= '{EDWRunCycleDate}'
  AND DT2.CLNDR_DT >= '{EDWRunCycleDate}'
  AND ADDR.PROV_ADDR_ST_CD_SK = MPPNG.CD_MPPNG_SK
  AND (ADDR.PRCTC_LOC_IN = 'Y' OR MAP2.SRC_CD = 'NABP')"""
df_db2_PROV_PREREQ_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfrm_BusinessLogic_lnk_CodesLkpData_in = df_db2_PROV_PREREQ_in.select(
    F.col("PROV_ID").alias("PROV_ID"),
    F.concat(F.col("CNTY_NM"), F.col("TRGT_CD")).alias("CNTY_NM")
)

df_xfrm_BusinessLogic_lnk_IdsEdwProvPreReqDExtr_OutABC = df_db2_PROV_PREREQ_in.select(
    F.col("PROV_ID").alias("PROV_ID"),
    F.concat(F.col("CNTY_NM"), F.col("TRGT_CD")).alias("CNTY_NM")
)

df_PROV_PREREQ_DS = df_xfrm_BusinessLogic_lnk_IdsEdwProvPreReqDExtr_OutABC.select("PROV_ID", "CNTY_NM")
write_files(
    df_PROV_PREREQ_DS,
    f"{adls_path}/ds/PROV_PRE_REQ.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

extract_query = f"""select Distinct
SRC_DRVD_LKUP_VAL,
TRGT_CD
from {IDSOwner}.CD_MPPNG
where TRGT_DOMAIN_NM = 'PROVIDER ADDRESS COUNTY CLASSIFICATION'
  and TRGT_CD in ('CONTG','32CNTY')"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_lkp_Codes_output = (
    df_xfrm_BusinessLogic_lnk_CodesLkpData_in.alias("primary")
    .join(
        df_db2_CD_MPPNG_in.alias("lookup"),
        F.col("primary.CNTY_NM") == F.col("lookup.SRC_DRVD_LKUP_VAL"),
        "left"
    )
    .select(
        F.col("primary.PROV_ID").alias("PROV_ID"),
        F.col("primary.CNTY_NM").alias("CNTY_NM"),
        F.col("lookup.TRGT_CD").alias("TRGT_CD")
    )
)

df_xfrm_BusinessLogic1_PROV_CNTG_CNTY = df_lkp_Codes_output.filter(
    (F.col("CNTY_NM").isNotNull()) & (F.col("TRGT_CD") == "CONTG")
).select(
    F.col("PROV_ID").alias("PROV_ID")
)

df_xfrm_BusinessLogic1_PROV_32CNTY = df_lkp_Codes_output.filter(
    (F.col("CNTY_NM").isNotNull()) & (F.col("TRGT_CD") == "32CNTY")
).select(
    F.col("PROV_ID").alias("PROV_ID")
)

df_Rdp_CNTG_CNTY = dedup_sort(
    df_xfrm_BusinessLogic1_PROV_CNTG_CNTY,
    ["PROV_ID"],
    [("PROV_ID", "A")]
)
df_PROV_CNTG_CNTY_DS = df_Rdp_CNTG_CNTY.select("PROV_ID")
write_files(
    df_PROV_CNTG_CNTY_DS,
    f"{adls_path}/ds/PROV_CONTG_CNTY.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Rdp_PROV_32CNTY = dedup_sort(
    df_xfrm_BusinessLogic1_PROV_32CNTY,
    ["PROV_ID"],
    [("PROV_ID", "A")]
)
df_PROV_32CNTY_DS = df_Rdp_PROV_32CNTY.select("PROV_ID")
write_files(
    df_PROV_32CNTY_DS,
    f"{adls_path}/ds/PROV_32CNTY.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)