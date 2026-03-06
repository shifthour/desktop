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
# MAGIC JOB NAME:  FctsPrvcyExtrnlEntyExtr
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty               2/25/2007          FHP /3028                       Originally Programmed                              devlIDS30                        Steph Goddard       3/29/2007
# MAGIC              
# MAGIC Bhoomi Dasari                 2/17/2009           Prod Supp/15                 Added new filter criteria                            devlIDS   
# MAGIC 
# MAGIC 
# MAGIC Bhoomi Dasari                2/25/2009         Prod Supp/15                 Added 'RunDate' paramter instead             devlIDS                       Steph Goddard            02/28/2009
# MAGIC                                                                                                         of Format.Date routine
# MAGIC 
# MAGIC Bhoomi Dasari               3/20/2009          Prod Supp/15                 Updated conditions in the extract SQL       devlIDS                       Steph Goddard             04/01/2009
# MAGIC 
# MAGIC Steph Goddard                7/14/10              TTR-689                      Changed primary key counter from          RebuildIntNewDevl          SAndrew                    2010-09-30
# MAGIC                                                                                                         IDS_SK to PRVCY_EXTRNL_ENTY_PHN_SK; brought to current standards
# MAGIC 
# MAGIC Anoop Nair                    2022-03-07         S2S Remediation      Added FACETS DSN Connection parameters      IntegrateDev5	Ken Bradmon	2022-06-03

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
from pyspark.sql.types import IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_hf_extrnl_enty_phn, jdbc_props_hf_extrnl_enty_phn = get_db_config(ids_secret_name)
df_hf_extrnl_enty_phn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_hf_extrnl_enty_phn)
    .options(**jdbc_props_hf_extrnl_enty_phn)
    .option(
        "query",
        "SELECT SRC_SYS_CD, PRVCY_EXTRNL_ENTY_UNIQ_KEY, PRVCY_EXTRNL_ENTY_PHN_TYP_CD_S, SEQ_NO, CRT_RUN_CYC_EXCTN_SK, PRVCY_EXTRNL_ENTY_PHN_SK FROM IDS.dummy_hf_extrnl_enty_phn"
    )
    .load()
)

jdbc_url_FacetsPrvcyExtrnEntyPhn, jdbc_props_FacetsPrvcyExtrnEntyPhn = get_db_config(facets_secret_name)
extract_query = f"""
SELECT
     ENPH.ENEN_CKE,
     ENPH.ENPH_PZCD_TYPE,
     ENPH.ENPH_SEQ_NO,
     ENPH.ENPH_PHONE,
     ENPH.ENPH_PHONE_EXT,
     ENPH.ENPH_LAST_USUS_ID,
     ENPH.ENPH_LAST_UPD_DTM
FROM
     {FacetsOwner}.FHD_ENPH_PHONE_D ENPH,
     {FacetsOwner}.FHD_ENEN_ENTITY_D ENEN
WHERE ENPH.ENEN_CKE = ENEN.ENEN_CKE
"""
df_FacetsPrvcyExtrnEntyPhn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_FacetsPrvcyExtrnEntyPhn)
    .options(**jdbc_props_FacetsPrvcyExtrnEntyPhn)
    .option("query", extract_query)
    .load()
)

df_StripFields = df_FacetsPrvcyExtrnEntyPhn.select(
    F.col("ENEN_CKE"),
    F.upper(F.col("ENPH_PZCD_TYPE")).alias("ENPH_PZCD_TYPE"),
    F.col("ENPH_SEQ_NO"),
    F.regexp_replace(F.col("ENPH_PHONE"), "[\\n\\r\\t]", "").alias("ENPH_PHONE"),
    F.regexp_replace(F.col("ENPH_PHONE_EXT"), "[\\n\\r\\t]", "").alias("ENPH_PHONE_EXT"),
    F.regexp_replace(F.col("ENPH_LAST_USUS_ID"), "[\\n\\r\\t]", "").alias("ENAD_LAST_USUS_ID"),
    F.date_format(F.col("ENPH_LAST_UPD_DTM"), "yyyy-MM-dd").alias("ENAD_LAST_UPD_DTM")
)

df_BusinessRules = df_StripFields.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(RunDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (
        F.lit("FACETS") + F.lit(";") +
        F.col("ENEN_CKE") + F.lit(";") +
        F.col("ENPH_PZCD_TYPE") + F.lit(";") +
        F.col("ENPH_SEQ_NO")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("PRVCY_EXTRNL_ENTY_PHN_SK"),
    F.col("ENEN_CKE").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.col("ENPH_PZCD_TYPE").alias("PRVCY_EXTRNL_ENTY_PHN_TYP_CD"),
    F.col("ENPH_SEQ_NO").alias("SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ENEN_CKE").alias("PRVCY_EXTRNL_ENTY"),
    trim(F.col("ENPH_PHONE")).alias("PHN_NO"),
    F.when(
        F.isnull(trim(F.col("ENPH_PHONE_EXT"))) | (F.length(trim(F.col("ENPH_PHONE_EXT"))) == 0),
        F.lit("NA")
    ).otherwise(trim(F.col("ENPH_PHONE_EXT"))).alias("PHN_NO_EXT"),
    F.col("ENAD_LAST_UPD_DTM").alias("SRC_SYS_LAST_UPDT_DT"),
    F.col("ENAD_LAST_USUS_ID").alias("SRC_SYS_LAST_UPDT_USER")
)

df_primarykey_joined = df_BusinessRules.alias("Transform").join(
    df_hf_extrnl_enty_phn.alias("lkup"),
    [
        F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
        F.col("Transform.PRVCY_EXTRNL_ENTY_UNIQ_KEY") == F.col("lkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.col("Transform.PRVCY_EXTRNL_ENTY_PHN_TYP_CD") == F.col("lkup.PRVCY_EXTRNL_ENTY_PHN_TYP_CD_S"),
        F.col("Transform.SEQ_NO") == F.col("lkup.SEQ_NO")
    ],
    how="left"
).withColumn(
    "was_lkup_null",
    F.col("lkup.PRVCY_EXTRNL_ENTY_PHN_SK").isNull()
)

df_primarykey = df_primarykey_joined.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.when(
        F.col("lkup.PRVCY_EXTRNL_ENTY_PHN_SK").isNull(),
        F.lit(None)
    ).otherwise(F.col("lkup.PRVCY_EXTRNL_ENTY_PHN_SK")).alias("PRVCY_EXTRNL_ENTY_PHN_SK"),
    F.col("Transform.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.col("Transform.PRVCY_EXTRNL_ENTY_PHN_TYP_CD").alias("PRVCY_EXTRNL_ENTY_PHN_TYP_CD"),
    F.col("Transform.SEQ_NO").alias("SEQ_NO"),
    F.when(
        F.col("lkup.PRVCY_EXTRNL_ENTY_PHN_SK").isNull(),
        F.col("CurrRunCycle")
    ).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.PRVCY_EXTRNL_ENTY").alias("PRVCY_EXTRNL_ENTY"),
    F.col("Transform.PHN_NO").alias("PHN_NO"),
    F.col("Transform.PHN_NO_EXT").alias("PHN_NO_EXT"),
    F.col("Transform.SRC_SYS_LAST_UPDT_DT").alias("SRC_SYS_LAST_UPDT_DT"),
    F.col("Transform.SRC_SYS_LAST_UPDT_USER").alias("SRC_SYS_LAST_UPDT_USER"),
    F.col("was_lkup_null")
)

df_enriched = df_primarykey
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PRVCY_EXTRNL_ENTY_PHN_SK",<schema>,<secret_name>)

df_key = df_enriched.drop("was_lkup_null")
df_key_for_write = (
    df_key
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("PHN_NO_EXT", F.rpad(F.col("PHN_NO_EXT"), 5, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT", F.rpad(F.col("SRC_SYS_LAST_UPDT_DT"), 10, " "))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "PRVCY_EXTRNL_ENTY_PHN_SK",
        "PRVCY_EXTRNL_ENTY_UNIQ_KEY",
        "PRVCY_EXTRNL_ENTY_PHN_TYP_CD",
        "SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PRVCY_EXTRNL_ENTY",
        "PHN_NO",
        "PHN_NO_EXT",
        "SRC_SYS_LAST_UPDT_DT",
        "SRC_SYS_LAST_UPDT_USER"
    )
)

write_files(
    df_key_for_write,
    f"{adls_path}/key/FctsPrvcyExtrnlEntyPhnExtr.PrvcyExtrnlEntyPhn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_updt = df_enriched.filter("was_lkup_null = true").select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.col("PRVCY_EXTRNL_ENTY_PHN_TYP_CD").alias("PRVCY_EXTRNL_ENTY_PHN_TYP_CD_S"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("PRVCY_EXTRNL_ENTY_PHN_SK").alias("PRVCY_EXTRNL_ENTY_PHN_SK")
)

drop_temp_sql = "DROP TABLE IF EXISTS STAGING.FctsPrvcyExtrnlEntyPhnExtr_hf_extrnl_enty_phn_updt_temp"
execute_dml(drop_temp_sql, jdbc_url_hf_extrnl_enty_phn, jdbc_props_hf_extrnl_enty_phn)

df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_hf_extrnl_enty_phn) \
    .options(**jdbc_props_hf_extrnl_enty_phn) \
    .option("dbtable", "STAGING.FctsPrvcyExtrnlEntyPhnExtr_hf_extrnl_enty_phn_updt_temp") \
    .mode("overwrite") \
    .save()

merge_sql = """
MERGE INTO IDS.dummy_hf_extrnl_enty_phn AS T
USING STAGING.FctsPrvcyExtrnlEntyPhnExtr_hf_extrnl_enty_phn_updt_temp AS S
ON
 T.SRC_SYS_CD = S.SRC_SYS_CD
 AND T.PRVCY_EXTRNL_ENTY_UNIQ_KEY = S.PRVCY_EXTRNL_ENTY_UNIQ_KEY
 AND T.PRVCY_EXTRNL_ENTY_PHN_TYP_CD_S = S.PRVCY_EXTRNL_ENTY_PHN_TYP_CD_S
 AND T.SEQ_NO = S.SEQ_NO
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.PRVCY_EXTRNL_ENTY_PHN_SK = S.PRVCY_EXTRNL_ENTY_PHN_SK
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    PRVCY_EXTRNL_ENTY_UNIQ_KEY,
    PRVCY_EXTRNL_ENTY_PHN_TYP_CD_S,
    SEQ_NO,
    CRT_RUN_CYC_EXCTN_SK,
    PRVCY_EXTRNL_ENTY_PHN_SK
  )
  VALUES (
    S.SRC_SYS_CD,
    S.PRVCY_EXTRNL_ENTY_UNIQ_KEY,
    S.PRVCY_EXTRNL_ENTY_PHN_TYP_CD_S,
    S.SEQ_NO,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.PRVCY_EXTRNL_ENTY_PHN_SK
  );
"""
execute_dml(merge_sql, jdbc_url_hf_extrnl_enty_phn, jdbc_props_hf_extrnl_enty_phn)