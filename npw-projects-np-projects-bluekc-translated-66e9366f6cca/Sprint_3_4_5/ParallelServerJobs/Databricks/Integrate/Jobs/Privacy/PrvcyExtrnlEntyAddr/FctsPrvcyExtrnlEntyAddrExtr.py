# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsPrvcyExtrnlEntyAddrExtr
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty               2/25/2007         FHP/3028                        Originally Programmed                              devlIDS30                        Steph Goddard          3/29/2007
# MAGIC              
# MAGIC Bhoomi Dasari                 2/17/2009           Prod Supp/15                 Added new filter criteria                            devlIDS   
# MAGIC 
# MAGIC Bhoomi Dasari                2/25/2009         Prod Supp/15                 Added 'RunDate' paramter instead             devlIDS                       Steph Goddard          02/28/2009
# MAGIC                                                                                                         of Format.Date routine
# MAGIC 
# MAGIC Bhoomi Dasari               3/20/2009          Prod Supp/15                 Updated conditions in the extract SQL       devlIDS                        Steph Goddard         04/01/2009
# MAGIC 
# MAGIC Steph Goddard              7/14/10               TTR-689                       Replaced primary key counter IDS_SK     RebuildIntNewDevl         SAndrew                 2010-09-30
# MAGIC                                                                                                         with PRVCY_EXTRNL_ENTY_ADDR_SK; brought to current standards
# MAGIC 
# MAGIC Bhoomi Dasari               2/14/2013          TTR-1534                      Changing the length of ADDR fields          IntegrateWrhsDevl        Kalyan Neelam           2013-03-01
# MAGIC                                                                                                        from 40 to 80.
# MAGIC 
# MAGIC Anoop Nair                2022-03-07         S2S Remediation      Added FACETS DSN Connection parameters      IntegrateDev5		Ken Bradmon	2022-06-03

# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Extrnl Enty Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to /pkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


facets_secret_name = get_widget_value('facets_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query = f"""
SELECT
    ENAD.ENEN_CKE,
    ENAD.ENAD_PZCD_TYPE,
    ENAD.ENAD_SEQ_NO,
    ENAD.ENAD_ADDR1,
    ENAD.ENAD_ADDR2,
    ENAD.ENAD_ADDR3,
    ENAD.ENAD_CITY,
    ENAD.ENAD_STATE,
    ENAD.ENAD_ZIP,
    ENAD.ENAD_LAST_USUS_ID,
    ENAD.ENAD_LAST_UPD_DTM,
    ENAD.ENAD_COUNTY
FROM {FacetsOwner}.FHD_ENAD_ADDRESS_D ENAD,
     {FacetsOwner}.FHD_ENEN_ENTITY_D ENEN
WHERE
    ENAD.ENEN_CKE = ENEN.ENEN_CKE
"""
df_FacetsPrvcyExtrnEntyAddr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query)
    .load()
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_extrnl_enty_addr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD, PRVCY_EXTRNL_ENTY_UNIQ_KEY, PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK, SEQ_NO, CRT_RUN_CYC_EXCTN_SK, PRVCY_EXTRNL_ENTY_ADDR_SK FROM IDS.dummy_hf_extrnl_enty_addr")
    .load()
)

df_StripFields = df_FacetsPrvcyExtrnEntyAddr.select(
    F.col("ENEN_CKE").alias("ENEN_CKE"),
    strip_field(F.col("ENAD_PZCD_TYPE")).alias("tmp_ENAD_PZCD_TYPE"),
    F.col("ENAD_SEQ_NO").alias("ENAD_SEQ_NO"),
    strip_field(F.col("ENAD_ADDR1")).alias("tmp_ENAD_ADDR1"),
    strip_field(F.col("ENAD_ADDR2")).alias("tmp_ENAD_ADDR2"),
    strip_field(F.col("ENAD_ADDR3")).alias("tmp_ENAD_ADDR3"),
    strip_field(F.col("ENAD_CITY")).alias("tmp_ENAD_CITY"),
    strip_field(F.col("ENAD_STATE")).alias("tmp_ENAD_STATE"),
    strip_field(F.col("ENAD_ZIP")).alias("tmp_ENAD_ZIP"),
    strip_field(F.col("ENAD_LAST_USUS_ID")).alias("tmp_ENAD_LAST_USUS_ID"),
    F.col("ENAD_LAST_UPD_DTM").alias("tmp_ENAD_LAST_UPD_DTM"),
    strip_field(F.col("ENAD_COUNTY")).alias("tmp_ENAD_COUNTY")
)
df_StripFields = df_StripFields.withColumn("ENAD_PZCD_TYPE", F.upper(F.col("tmp_ENAD_PZCD_TYPE"))) \
    .withColumn("ENAD_ADDR1", F.col("tmp_ENAD_ADDR1")) \
    .withColumn("ENAD_ADDR2", F.col("tmp_ENAD_ADDR2")) \
    .withColumn("ENAD_ADDR3", F.col("tmp_ENAD_ADDR3")) \
    .withColumn("ENAD_CITY", F.col("tmp_ENAD_CITY")) \
    .withColumn("ENAD_STATE", F.col("tmp_ENAD_STATE")) \
    .withColumn("ENAD_ZIP", F.col("tmp_ENAD_ZIP")) \
    .withColumn("ENAD_LAST_USUS_ID", F.col("tmp_ENAD_LAST_USUS_ID")) \
    .withColumn("ENAD_COUNTY", F.col("tmp_ENAD_COUNTY")) \
    .drop(
        "tmp_ENAD_PZCD_TYPE",
        "tmp_ENAD_ADDR1",
        "tmp_ENAD_ADDR2",
        "tmp_ENAD_ADDR3",
        "tmp_ENAD_CITY",
        "tmp_ENAD_STATE",
        "tmp_ENAD_ZIP",
        "tmp_ENAD_LAST_USUS_ID",
        "tmp_ENAD_COUNTY"
    )
df_StripFields = df_StripFields.withColumn(
    "ENAD_LAST_UPD_DTM",
    F.date_format(F.col("tmp_ENAD_LAST_UPD_DTM"), "yyyy-MM-dd")
).drop("tmp_ENAD_LAST_UPD_DTM")

df_BusinessRules = df_StripFields.withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0)) \
    .withColumn("INSRT_UPDT_CD", F.lit("I")) \
    .withColumn("DISCARD_IN", F.lit("N")) \
    .withColumn("PASS_THRU_IN", F.lit("Y")) \
    .withColumn("FIRST_RECYC_DT", F.lit(RunDate)) \
    .withColumn("ERR_CT", F.lit(0)) \
    .withColumn("RECYCLE_CT", F.lit(0)) \
    .withColumn("SRC_SYS_CD", F.lit("FACETS")) \
    .withColumn("PRI_KEY_STRING", F.concat_ws(";", F.lit("FACETS"), F.col("ENEN_CKE"), F.col("ENAD_PZCD_TYPE"), F.col("ENAD_SEQ_NO"))) \
    .withColumn("PRVCY_EXTRNL_ENTY_ADDR_SK", F.lit(0)) \
    .withColumn("PRVCY_EXTRNL_ENTY_UNIQ_KEY", F.col("ENEN_CKE")) \
    .withColumn("PRVCY_EXTL_ENTY_ADDR_TYP_CD", F.col("ENAD_PZCD_TYPE")) \
    .withColumn("SEQ_NO", F.col("ENAD_SEQ_NO")) \
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0)) \
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0)) \
    .withColumn("PRVCY_EXTRNL_ENTY", F.col("ENEN_CKE")) \
    .withColumn("ADDR_LN_1", F.when(
        F.col("ENAD_ADDR1").isNull() | (F.length(trim(F.col("ENAD_ADDR1"))) == 0),
        "  "
    ).otherwise(trim(F.col("ENAD_ADDR1")))) \
    .withColumn("ADDR_LN_2", F.when(
        F.col("ENAD_ADDR2").isNull() | (F.length(trim(F.col("ENAD_ADDR2"))) == 0),
        "  "
    ).otherwise(trim(F.col("ENAD_ADDR2")))) \
    .withColumn("ADDR_LN_3", F.when(
        F.col("ENAD_ADDR3").isNull() | (F.length(trim(F.col("ENAD_ADDR3"))) == 0),
        "  "
    ).otherwise(trim(F.col("ENAD_ADDR3")))) \
    .withColumn("CITY_NM", F.when(
        F.col("ENAD_CITY").isNull() | (F.length(trim(F.col("ENAD_CITY"))) == 0),
        "NA"
    ).otherwise(trim(F.col("ENAD_CITY")))) \
    .withColumn("PRVCY_EXTL_ENTY_ADDR_ST_CD", F.col("ENAD_STATE")) \
    .withColumn("POSTAL_CD", F.when(
        F.col("ENAD_ZIP").isNull() | (F.length(trim(F.col("ENAD_ZIP"))) == 0),
        "  "
    ).otherwise(trim(F.col("ENAD_ZIP")))) \
    .withColumn("CNTY_NM", F.when(
        F.col("ENAD_COUNTY").isNull() | (F.length(trim(F.col("ENAD_COUNTY"))) == 0),
        " "
    ).otherwise(trim(F.col("ENAD_COUNTY")))) \
    .withColumn("SRC_SYS_LAST_UPDT_DT", F.col("ENAD_LAST_UPD_DTM")) \
    .withColumn("SRC_SYS_LAST_UPDT_USER", F.col("ENAD_LAST_USUS_ID"))

df_join = df_BusinessRules.alias("Transform").join(
    df_hf_extrnl_enty_addr.alias("lkup"),
    (
        (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD")) &
        (F.col("Transform.PRVCY_EXTRNL_ENTY_UNIQ_KEY") == F.col("lkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY")) &
        (F.col("Transform.PRVCY_EXTL_ENTY_ADDR_TYP_CD") == F.col("lkup.PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK")) &
        (F.col("Transform.SEQ_NO") == F.col("lkup.SEQ_NO"))
    ),
    "left"
)

df_primarykey = df_join.withColumn(
    "NewCurrRunCycExtcnSk",
    F.when(F.col("lkup.PRVCY_EXTRNL_ENTY_ADDR_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "Sk",
    F.when(F.col("lkup.PRVCY_EXTRNL_ENTY_ADDR_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.PRVCY_EXTRNL_ENTY_ADDR_SK"))
)

df_enriched = SurrogateKeyGen(df_primarykey, <DB sequence name>, "Sk", <schema>, <secret_name>)

df_key = df_enriched.withColumn("PRVCY_EXTRNL_ENTY_ADDR_SK", F.col("Sk")) \
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("NewCurrRunCycExtcnSk")) \
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))

df_updt = df_key.filter(F.col("lkup.PRVCY_EXTRNL_ENTY_ADDR_SK").isNull())

df_key_write = df_key.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("Transform.INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("Transform.DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("Transform.PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("PRVCY_EXTRNL_ENTY_ADDR_SK"),
    F.col("Transform.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.col("Transform.PRVCY_EXTL_ENTY_ADDR_TYP_CD").alias("PRVCY_EXTL_ENTY_ADDR_TYP_CD"),
    F.col("Transform.SEQ_NO").alias("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.PRVCY_EXTRNL_ENTY").alias("PRVCY_EXTRNL_ENTY"),
    F.col("Transform.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("Transform.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("Transform.ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("Transform.CITY_NM").alias("CITY_NM"),
    F.col("Transform.PRVCY_EXTL_ENTY_ADDR_ST_CD").alias("PRVCY_EXTL_ENTY_ADDR_ST_CD"),
    F.col("Transform.POSTAL_CD").alias("POSTAL_CD"),
    F.col("Transform.CNTY_NM").alias("CNTY_NM"),
    F.rpad(F.col("Transform.SRC_SYS_LAST_UPDT_DT"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT"),
    F.col("Transform.SRC_SYS_LAST_UPDT_USER").alias("SRC_SYS_LAST_UPDT_USER")
)
write_files(
    df_key_write,
    f"{adls_path}/key/FctsPrvcyExtrnlEntyAddrExtr.PrvcyExtrnlEntyAddr.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_updt_write = df_updt.select(
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.col("Transform.PRVCY_EXTL_ENTY_ADDR_TYP_CD").alias("PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK"),
    F.col("Transform.SEQ_NO").alias("SEQ_NO"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Sk").alias("PRVCY_EXTRNL_ENTY_ADDR_SK")
)

execute_dml("DROP TABLE IF EXISTS STAGING.FctsPrvcyExtrnlEntyAddrExtr_hf_extrnl_enty_addr_updt_temp", jdbc_url_ids, jdbc_props_ids)

df_updt_write.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.FctsPrvcyExtrnlEntyAddrExtr_hf_extrnl_enty_addr_updt_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE IDS.dummy_hf_extrnl_enty_addr AS T
USING STAGING.FctsPrvcyExtrnlEntyAddrExtr_hf_extrnl_enty_addr_updt_temp AS S
ON (
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.PRVCY_EXTRNL_ENTY_UNIQ_KEY = S.PRVCY_EXTRNL_ENTY_UNIQ_KEY
  AND T.PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK = S.PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK
  AND T.SEQ_NO = S.SEQ_NO
)
WHEN MATCHED THEN UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.PRVCY_EXTRNL_ENTY_ADDR_SK = S.PRVCY_EXTRNL_ENTY_ADDR_SK
WHEN NOT MATCHED THEN
    INSERT (
      SRC_SYS_CD,
      PRVCY_EXTRNL_ENTY_UNIQ_KEY,
      PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK,
      SEQ_NO,
      CRT_RUN_CYC_EXCTN_SK,
      PRVCY_EXTRNL_ENTY_ADDR_SK
    )
    VALUES (
      S.SRC_SYS_CD,
      S.PRVCY_EXTRNL_ENTY_UNIQ_KEY,
      S.PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK,
      S.SEQ_NO,
      S.CRT_RUN_CYC_EXCTN_SK,
      S.PRVCY_EXTRNL_ENTY_ADDR_SK
    );
"""

execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)