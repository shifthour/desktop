# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 06/19/09 14:44:48 Batch  15146_53151 PROMOTE bckcetl:31540 ids20 dsadm rc for steph 
# MAGIC ^1_1 06/19/09 14:29:48 Batch  15146_52211 INIT bckcett:31540 testIDS dsadm rc for steph 
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 05/25/07 11:56:51 Batch  14390_43020 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 05/25/07 11:52:00 Batch  14390_42723 INIT bckcett testIDSnew dsadm bls for on
# MAGIC ^1_1 05/17/07 12:03:56 Batch  14382_43445 PROMOTE bckcett testIDSnew u10913 Ollie move from devl to test
# MAGIC ^1_1 05/17/07 09:42:32 Batch  14382_34957 PROMOTE bckcett testIDSnew u10913 Ollie move from devl to test
# MAGIC ^1_1 05/17/07 09:38:32 Batch  14382_34718 INIT bckcett devlIDS30 u10913 Ollie Move from devl to test
# MAGIC ^1_1 04/27/07 12:54:12 Batch  14362_46467 INIT bckcett devlIDS30 u10913 O. Nielsen move from devl to 4.3 Environment
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsPrvcyExtrnlPrsnExtr
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty               2/25/2007           FHP /3028                       Originally Programmed                              devlIDS30               Steph Goddard            3/29/2007
# MAGIC              
# MAGIC Bhoomi Dasari                 2/17/2009           Prod Supp/15              Added new filter criteria                                devlIDS   
# MAGIC 
# MAGIC Bhoomi Dasari                2/25/2009         Prod Supp/15                 Added 'RunDate' paramter instead             devlIDS                      Steph Goddard            02/28/2009
# MAGIC                                                                                                         of Format.Date routine
# MAGIC 
# MAGIC 
# MAGIC Bhoomi Dasari               3/20/2009          Prod Supp/15                 Updated conditions in the extract SQL       devlIDS                      Steph Goddard            04/01/2009
# MAGIC 
# MAGIC Anoop Nair                   2022-03-08         S2S Remediation      Added FACETS DSN Connection parameters      IntegrateDev5	Ken Bradmon	2022-06-03

# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Extrnl Enty Data
# MAGIC Apply business logic
# MAGIC Using the same hash file (hf_extrnl_enty) in 
# MAGIC FctsPrvcyExtrnlEntyExtr
# MAGIC FctsPrvcyExtrnlPrsnExtr
# MAGIC FctsPrvcyExtrnlOrgExtr
# MAGIC Writing Sequential File to /pkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
RunDate = get_widget_value("RunDate","")
ids_secret_name = get_widget_value("ids_secret_name","")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_extrnl_enty = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT SRC_SYS_CD, PRVCY_EXTRNL_ENTY_UNIQ_KEY, CRT_RUN_CYC_EXCTN_SK, PRVCY_EXTRNL_ENTY_SK FROM IDS.dummy_hf_extrnl_enty"
    )
    .load()
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query = f"""
SELECT 
    {FacetsOwner}.FHD_ENPD_PERSON_D.ENEN_CKE,
    {FacetsOwner}.FHD_ENPD_PERSON_D.ENPD_LNAME,
    {FacetsOwner}.FHD_ENPD_PERSON_D.ENPD_FNAME,
    {FacetsOwner}.FHD_ENPD_PERSON_D.ENPD_MID_INIT,
    {FacetsOwner}.FHD_ENPD_PERSON_D.ENPD_DOB_DT,
    {FacetsOwner}.FHD_ENPD_PERSON_D.ENPD_GENDER_IND,
    {FacetsOwner}.FHD_ENPD_PERSON_D.ENPD_MARITAL_IND,
    {FacetsOwner}.FHD_ENPD_PERSON_D.ENAD_PZCD_TYPE,
    {FacetsOwner}.FHD_ENPD_PERSON_D.ENPH_PZCD_TYPE,
    {FacetsOwner}.FHD_ENPD_PERSON_D.ENPD_LAST_USUS_ID,
    {FacetsOwner}.FHD_ENPD_PERSON_D.ENPD_LAST_UPD_DTM
FROM
    {FacetsOwner}.FHD_ENPD_PERSON_D,
    {FacetsOwner}.FHD_ENEN_ENTITY_D
WHERE
    {FacetsOwner}.FHD_ENPD_PERSON_D.ENEN_CKE = {FacetsOwner}.FHD_ENEN_ENTITY_D.ENEN_CKE
"""
df_FacetsPrvcyExtrnlPrsn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query)
    .load()
)

df_strip = df_FacetsPrvcyExtrnlPrsn.select(
    F.col("ENEN_CKE").alias("ENEN_CKE"),
    F.regexp_replace(F.col("ENPD_LNAME"), "[\\n\\r\\t]", "").alias("ENPD_LNAME"),
    F.regexp_replace(F.col("ENPD_FNAME"), "[\\n\\r\\t]", "").alias("ENPD_FNAME"),
    F.regexp_replace(F.col("ENPD_MID_INIT"), "[\\n\\r\\t]", "").alias("ENPD_MID_INIT"),
    F.date_format(F.col("ENPD_DOB_DT"), "yyyy-MM-dd").alias("ENPD_DOB_DT"),
    F.regexp_replace(F.upper(F.col("ENPD_GENDER_IND")), "[\\n\\r\\t]", "").alias("ENPD_GENDER_IND"),
    F.regexp_replace(F.upper(F.col("ENPD_MARITAL_IND")), "[\\n\\r\\t]", "").alias("ENPD_MARITAL_IND"),
    F.regexp_replace(F.upper(F.col("ENAD_PZCD_TYPE")), "[\\n\\r\\t]", "").alias("ENAD_PZCD_TYPE"),
    F.regexp_replace(F.upper(F.col("ENPH_PZCD_TYPE")), "[\\n\\r\\t]", "").alias("ENPH_PZCD_TYPE"),
    F.regexp_replace(F.col("ENPD_LAST_USUS_ID"), "[\\n\\r\\t]", "").alias("ENPD_LAST_USUS_ID"),
    F.date_format(F.col("ENPD_LAST_UPD_DTM"), "yyyy-MM-dd").alias("ENPD_LAST_UPD_DTM")
)

df_business = (
    df_strip
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(RunDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn("PRI_KEY_STRING", F.concat_ws(";", F.lit("FACETS"), trim(F.col("ENEN_CKE"))))
    .withColumn("PRVCY_EXTRNL_ENTY_UNIQ_KEY", F.col("ENEN_CKE"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("PRVCY_EXTL_PRSN_ADDR_TYP_CD", trim(F.upper(F.col("ENAD_PZCD_TYPE"))))
    .withColumn("PRVCY_EXTRNL_PRSN_GNDR_CD", trim(F.col("ENPD_GENDER_IND")))
    .withColumn("PRVCY_EXTL_PRSN_MRTL_STS_CD", trim(F.col("ENPD_MARITAL_IND")))
    .withColumn("PRVCY_EXTL_PRSN_PHN_TYP_CD", trim(F.upper(F.col("ENPH_PZCD_TYPE"))))
    .withColumn("BRTH_DT", F.col("ENPD_DOB_DT"))
    .withColumn("FIRST_NM", trim(F.col("ENPD_FNAME")))
    .withColumn("MIDINIT", trim(F.col("ENPD_MID_INIT")))
    .withColumn("LAST_NM", trim(F.col("ENPD_LNAME")))
    .withColumn("SRC_SYS_LAST_UPDT_DT", F.col("ENPD_LAST_UPD_DTM"))
    .withColumn("SRC_SYS_LAST_UPDT_USER", trim(F.col("ENPD_LAST_USUS_ID")))
)

df_join = (
    df_business.alias("Transform")
    .join(
        df_hf_extrnl_enty.alias("lkup"),
        (
            (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD")) &
            (F.col("Transform.PRVCY_EXTRNL_ENTY_UNIQ_KEY") == F.col("lkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY"))
        ),
        how="left"
    )
)

df_join = df_join.withColumn(
    "NewCurrRunCycExtcnSk",
    F.when(F.col("lkup.PRVCY_EXTRNL_ENTY_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

df_join = df_join.withColumn(
    "PRVCY_EXTRNL_ENTY_SK",
    F.when(F.col("lkup.PRVCY_EXTRNL_ENTY_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.PRVCY_EXTRNL_ENTY_SK"))
)

df_join = df_join.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
df_join = df_join.withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("NewCurrRunCycExtcnSk")).drop("NewCurrRunCycExtcnSk")

df_enriched = df_join
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'PRVCY_EXTRNL_ENTY_SK',<schema>,<secret_name>)

df_key = df_enriched.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("PRVCY_EXTRNL_ENTY_SK").alias("PRVCY_EXTRNL_ENTY_SK"),
    F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PRVCY_EXTL_PRSN_ADDR_TYP_CD").alias("PRVCY_EXTL_PRSN_ADDR_TYP_CD"),
    F.col("PRVCY_EXTRNL_PRSN_GNDR_CD").alias("PRVCY_EXTRNL_PRSN_GNDR_CD"),
    F.col("PRVCY_EXTL_PRSN_MRTL_STS_CD").alias("PRVCY_EXTL_PRSN_MRTL_STS_CD"),
    F.col("PRVCY_EXTL_PRSN_PHN_TYP_CD").alias("PRVCY_EXTL_PRSN_PHN_TYP_CD"),
    F.col("BRTH_DT").alias("BRTH_DT"),
    F.col("FIRST_NM").alias("FIRST_NM"),
    F.col("MIDINIT").alias("MIDINIT"),
    F.col("LAST_NM").alias("LAST_NM"),
    F.col("SRC_SYS_LAST_UPDT_DT").alias("SRC_SYS_LAST_UPDT_DT"),
    F.col("SRC_SYS_LAST_UPDT_USER").alias("SRC_SYS_LAST_UPDT_USER")
)

df_key = (
    df_key
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("PRVCY_EXTL_PRSN_ADDR_TYP_CD", F.rpad(F.col("PRVCY_EXTL_PRSN_ADDR_TYP_CD"), 4, " "))
    .withColumn("PRVCY_EXTRNL_PRSN_GNDR_CD", F.rpad(F.col("PRVCY_EXTRNL_PRSN_GNDR_CD"), 1, " "))
    .withColumn("PRVCY_EXTL_PRSN_MRTL_STS_CD", F.rpad(F.col("PRVCY_EXTL_PRSN_MRTL_STS_CD"), 1, " "))
    .withColumn("PRVCY_EXTL_PRSN_PHN_TYP_CD", F.rpad(F.col("PRVCY_EXTL_PRSN_PHN_TYP_CD"), 4, " "))
    .withColumn("BRTH_DT", F.rpad(F.col("BRTH_DT"), 10, " "))
    .withColumn("MIDINIT", F.rpad(F.col("MIDINIT"), 1, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT", F.rpad(F.col("SRC_SYS_LAST_UPDT_DT"), 10, " "))
)

out_path = f"{adls_path}/key/FctsPrvcyExtrnlPrsnExtr.PrvcyExtrnlPrsn.dat.{RunID}"
write_files(
    df_key,
    out_path,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_updt = df_enriched.filter(F.col("lkup.PRVCY_EXTRNL_ENTY_SK").isNull()).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRVCY_EXTRNL_ENTY_SK").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("PRVCY_EXTRNL_ENTY_SK").alias("PRVCY_EXTRNL_ENTY_SK")
)

temp_table = "STAGING.FctsPrvcyExtrnlPrsnExtr_hf_extrnl_enty_updt_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url_ids, jdbc_props_ids)
df_updt.write.jdbc(url=jdbc_url_ids, table=temp_table, mode="overwrite", properties=jdbc_props_ids)

merge_sql = f"""
MERGE INTO IDS.dummy_hf_extrnl_enty AS T
USING {temp_table} AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.PRVCY_EXTRNL_ENTY_UNIQ_KEY = S.PRVCY_EXTRNL_ENTY_UNIQ_KEY
WHEN MATCHED THEN 
  UPDATE SET 
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.PRVCY_EXTRNL_ENTY_SK = S.PRVCY_EXTRNL_ENTY_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, PRVCY_EXTRNL_ENTY_UNIQ_KEY, CRT_RUN_CYC_EXCTN_SK, PRVCY_EXTRNL_ENTY_SK)
  VALUES (S.SRC_SYS_CD, S.PRVCY_EXTRNL_ENTY_UNIQ_KEY, S.CRT_RUN_CYC_EXCTN_SK, S.PRVCY_EXTRNL_ENTY_SK);
"""

execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)