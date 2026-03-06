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
# MAGIC JOB NAME:  FctsPrvcyExtrnlOrgExtr
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty               2/25/2007                     FHP/3028         Originally Programmed                              devlIDS30                    Steph Goddard            3/29/2007
# MAGIC              
# MAGIC Bhoomi Dasari                 2/17/2009           Prod Supp/15              Added new filter criteria                           devlIDS   
# MAGIC 
# MAGIC Bhoomi Dasari                2/25/2009         Prod Supp/15                 Added 'RunDate' paramter instead          devlIDS                         Steph Goddard            02/28/2009
# MAGIC                                                                                                         of Format.Date routine
# MAGIC 
# MAGIC Bhoomi Dasari               3/20/2009          Prod Supp/15                 Updated conditions in the extract SQL       devlIDS                      Steph Goddard            04/01/2009
# MAGIC 
# MAGIC Anoop Nair                2022-03-08         S2S Remediation         Added FACETS DSN Connection parameters      IntegrateDev5	Ken Bradmon	2022-06-03

# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Extrnl Org Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
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
from pyspark.sql.functions import col, lit, when, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_extrnl_org = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD, PRVCY_EXTRNL_ENTY_UNIQ_KEY, CRT_RUN_CYC_EXCTN_SK, PRVCY_EXTRNL_ENTY_SK FROM IDS.dummy_hf_extrnl_enty")
    .load()
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query = """SELECT 
     #$FacetsOwner#.FHD_ENOD_ORG_D.ENEN_CKE,      
     #$FacetsOwner#.FHD_ENOD_ORG_D.ENOD_BNAME,  
     #$FacetsOwner#.FHD_ENOD_ORG_D.ENAD_PZCD_TYPE, 
     #$FacetsOwner#.FHD_ENOD_ORG_D.ENPH_PZCD_TYPE,
     #$FacetsOwner#.FHD_ENOD_ORG_D.ENOD_LAST_USUS_ID,
     #$FacetsOwner#.FHD_ENOD_ORG_D.ENOD_LAST_UPD_DTM

FROM  
     #$FacetsOwner#.FHD_ENOD_ORG_D,
     #$FacetsOwner#.FHD_ENEN_ENTITY_D

WHERE
     #$FacetsOwner#.FHD_ENOD_ORG_D.ENEN_CKE = #$FacetsOwner#.FHD_ENEN_ENTITY_D.ENEN_CKE
"""
df_FacetsPrvcyExtrnlOrg = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query)
    .load()
)

df_StripFields = df_FacetsPrvcyExtrnlOrg.select(
    col("ENEN_CKE").alias("ENEN_CKE"),
    col("ENOD_BNAME").alias("ENOD_BNAME"),
    UpCase(col("ENAD_PZCD_TYPE")).alias("ENAD_PZCD_TYPE"),
    UpCase(col("ENPH_PZCD_TYPE")).alias("ENPH_PZCD_TYPE"),
    col("ENOD_LAST_USUS_ID").alias("ENOD_LAST_USUS_ID"),
    FORMAT_DATE(col("ENOD_LAST_UPD_DTM"), "SYBASE", "TIMESTAMP", "CCYY-MM-DD").alias("ENOD_LAST_UPD_DTM")
)

df_BusinessRules = df_StripFields.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
    rpad(lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    lit(RunDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit("FACETS").alias("SRC_SYS_CD"),
    concat(lit("FACETS"), lit(";"), trim(col("ENEN_CKE"))).alias("PRI_KEY_STRING"),
    lit(0).alias("PRVCY_EXTRNL_ENTY_SK"),
    col("ENEN_CKE").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("ENAD_PZCD_TYPE"), 4, " ").alias("PRVCY_EXTL_ORG_ADDR_TYP_CD"),
    rpad(col("ENPH_PZCD_TYPE"), 4, " ").alias("PRVCY_EXTRNL_ORG_PHN_TYP_CD"),
    trim(col("ENOD_BNAME")).alias("ORG_NM"),
    rpad(trim(col("ENOD_LAST_UPD_DTM")), 10, " ").alias("SRC_SYS_LAST_UPDT_DT"),
    rpad(col("ENOD_LAST_USUS_ID"), 10, " ").alias("SRC_SYS_LAST_UPDT_USER")
)

df_join = df_BusinessRules.alias("Transform").join(
    df_hf_extrnl_org.alias("lkup"),
    (
        (col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD")) &
        (col("Transform.PRVCY_EXTRNL_ENTY_UNIQ_KEY") == col("lkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY"))
    ),
    "left"
)

df_enriched = df_join.select(
    col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Transform.ERR_CT").alias("ERR_CT"),
    col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("Transform.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    col("lkup.CRT_RUN_CYC_EXCTN_SK").alias("lkup_CRT_RUN_CYC_EXCTN_SK"),
    col("lkup.PRVCY_EXTRNL_ENTY_SK").alias("lkup_PRVCY_EXTRNL_ENTY_SK"),
    col("Transform.CRT_RUN_CYC_EXCTN_SK").alias("temp_CRT_RUN"),
    col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("temp_LAST_UPDT"),
    col("Transform.PRVCY_EXTL_ORG_ADDR_TYP_CD").alias("PRVCY_EXTL_ORG_ADDR_TYP_CD"),
    col("Transform.PRVCY_EXTRNL_ORG_PHN_TYP_CD").alias("PRVCY_EXTRNL_ORG_PHN_TYP_CD"),
    col("Transform.ORG_NM").alias("ORG_NM"),
    col("Transform.SRC_SYS_LAST_UPDT_DT").alias("SRC_SYS_LAST_UPDT_DT"),
    col("Transform.SRC_SYS_LAST_UPDT_USER").alias("SRC_SYS_LAST_UPDT_USER")
)

df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    when(col("lkup_PRVCY_EXTRNL_ENTY_SK").isNull(), lit(CurrRunCycle)).otherwise(col("lkup_CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    lit(CurrRunCycle)
).withColumn(
    "PRVCY_EXTRNL_ENTY_SK",
    when(col("lkup_PRVCY_EXTRNL_ENTY_SK").isNull(), lit(None)).otherwise(col("lkup_PRVCY_EXTRNL_ENTY_SK"))
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PRVCY_EXTRNL_ENTY_SK",<schema>,<secret_name>)

df_Key = df_enriched.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("PRVCY_EXTRNL_ENTY_SK"),
    col("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PRVCY_EXTL_ORG_ADDR_TYP_CD"),
    col("PRVCY_EXTRNL_ORG_PHN_TYP_CD"),
    col("ORG_NM"),
    col("SRC_SYS_LAST_UPDT_DT"),
    col("SRC_SYS_LAST_UPDT_USER")
)

df_Key = df_Key.withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " ")) \
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " ")) \
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " ")) \
    .withColumn("PRVCY_EXTL_ORG_ADDR_TYP_CD", rpad(col("PRVCY_EXTL_ORG_ADDR_TYP_CD"), 4, " ")) \
    .withColumn("PRVCY_EXTRNL_ORG_PHN_TYP_CD", rpad(col("PRVCY_EXTRNL_ORG_PHN_TYP_CD"), 4, " ")) \
    .withColumn("SRC_SYS_LAST_UPDT_DT", rpad(col("SRC_SYS_LAST_UPDT_DT"), 10, " ")) \
    .withColumn("SRC_SYS_LAST_UPDT_USER", rpad(col("SRC_SYS_LAST_UPDT_USER"), 10, " "))

write_files(
    df_Key,
    f"{adls_path}/key/FctsPrvcyExtrnlOrgExtr.PrvcyExtrnlOrg.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_updt = df_enriched.filter(col("lkup_PRVCY_EXTRNL_ENTY_SK").isNull()).select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRVCY_EXTRNL_ENTY_SK").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    col("PRVCY_EXTRNL_ENTY_SK").alias("PRVCY_EXTRNL_ENTY_SK")
)

df_updt.write.format("jdbc") \
    .option("url", jdbc_url_ids) \
    .option("dbtable", "IDS.dummy_hf_extrnl_enty") \
    .options(**jdbc_props_ids) \
    .mode("append") \
    .save()