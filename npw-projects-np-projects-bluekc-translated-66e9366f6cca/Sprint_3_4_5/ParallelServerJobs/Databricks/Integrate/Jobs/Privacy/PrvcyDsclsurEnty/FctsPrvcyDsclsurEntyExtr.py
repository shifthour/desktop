# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 05/25/07 11:56:51 Batch  14390_43020 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 05/25/07 11:52:00 Batch  14390_42723 INIT bckcett testIDSnew dsadm bls for on
# MAGIC ^1_1 05/17/07 12:03:56 Batch  14382_43445 PROMOTE bckcett testIDSnew u10913 Ollie move from devl to test
# MAGIC ^1_1 05/17/07 09:42:32 Batch  14382_34957 PROMOTE bckcett testIDSnew u10913 Ollie move from devl to test
# MAGIC ^1_1 05/17/07 09:38:32 Batch  14382_34718 INIT bckcett devlIDS30 u10913 Ollie Move from devl to test
# MAGIC ^1_1 04/27/07 12:54:12 Batch  14362_46467 INIT bckcett devlIDS30 u10913 O. Nielsen move from devl to 4.3 Environment
# MAGIC 
# MAGIC 
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsPrvcyDsclsurExtr
# MAGIC DESCRIPTION:   Pulls data from FHP_PBED_BUSINES_D and FHP_PCED_COV_ENT and collects them  for loading into IDS.
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Naren Garapaty     02/20/2007                Initial program                                                                                                             devlIDS30                          Steph Goddard       3/29/2007
# MAGIC 
# MAGIC Steph Goddard      07/14/10                Changed primary key counter from IDS_SK to                                   TTR-689           RebuildIntNewDevl               SAndrew                2010-09-30
# MAGIC                                                              PRVCY_DSCLSUR_ENTY_SK; brought up to current standards
# MAGIC 
# MAGIC Anoop Nair             2022-03-08        Added FACETS DSN Connection parameters                                       S2S Remediation         IntegrateDev5	Ken Bradmon	2022-06-03

# MAGIC Extracts PBED_ID,PCED_ID from Facets
# MAGIC CollectsThe Ids And Assigns them to DISCLSUR_ID
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, regexp_replace, upper, date_format, concat, when, isnull, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

jdbc_url = None
jdbc_props = None

# Retrieve job parameters
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')

# --------------------------------------------------------------------------------
# Read from dummy table in place of hashed file "hf_prvcy_dsclsur_enty" (Scenario B)
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
hf_query = """
SELECT
  SRC_SYS_CD,
  PRVCY_DSCLSUR_ENTY_UNIQ_KEY,
  CRT_RUN_CYC_EXCTN_SK,
  PRVCY_DSCLSUR_ENTY_SK
FROM IDS.dummy_hf_prvcy_dsclsur_enty
"""
df_hf_prvcy_dsclsur_enty = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", hf_query.strip())
    .load()
)

# --------------------------------------------------------------------------------
# FctsPbedBusines (ODBC Connector)
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query_pbed = f"""
SELECT
  PBED_CKE,
  PBED_ID,
  PBED_CTYP,
  PBED_PZCD_TYPE,
  PBED_LAST_USUS_ID,
  PBED_LAST_UPD_DTM
FROM {FacetsOwner}.FHP_PBED_BUSINES_D
"""
df_FctsPbedBusines = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_pbed.strip())
    .load()
)

# --------------------------------------------------------------------------------
# StripPbed (Transformer)
# --------------------------------------------------------------------------------
df_StripPbed = df_FctsPbedBusines.select(
    col("PBED_CKE").alias("PRVCY_DSCLSUR_ENTY_UNIQ_KEY"),
    upper(regexp_replace(col("PBED_PZCD_TYPE"), "[\r\n\t]", "")).alias("PRVCY_DSCLSUR_ENTY_CAT_CD"),
    upper(regexp_replace(col("PBED_CTYP"), "[\r\n\t]", "")).alias("PRVCY_DSCLSUR_ENTY_TYP_CD"),
    regexp_replace(col("PBED_ID"), "[\r\n\t]", "").alias("PRVCY_DSCLSUR_ENTY_ID"),
    date_format(col("PBED_LAST_UPD_DTM"), "yyyy-MM-dd").alias("SRC_SYS_LAST_UPDT_DT"),
    regexp_replace(col("PBED_LAST_USUS_ID"), "[\r\n\t]", "").alias("SRC_SYS_LAST_UPDT_USER")
)

# --------------------------------------------------------------------------------
# FctsPcedCovEnt (ODBC Connector)
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query_pced = f"""
SELECT
  PCED_CKE,
  PCED_ID,
  PCED_LAST_USUS_ID,
  PCED_LAST_UPD_DTM
FROM {FacetsOwner}.FHP_PCED_COV_ENT_D
"""
df_FctsPcedCovEnt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_pced.strip())
    .load()
)

# --------------------------------------------------------------------------------
# StripPced (Transformer)
# --------------------------------------------------------------------------------
df_StripPced = df_FctsPcedCovEnt.select(
    col("PCED_CKE").alias("PRVCY_DSCLSUR_ENTY_UNIQ_KEY"),
    lit("COVE").alias("PRVCY_DSCLSUR_ENTY_CAT_CD"),
    lit("COVE").alias("PRVCY_DSCLSUR_ENTY_TYP_CD"),
    regexp_replace(col("PCED_ID"), "[\r\n\t]", "").alias("PRVCY_DSCLSUR_ENTY_ID"),
    date_format(col("PCED_LAST_UPD_DTM"), "yyyy-MM-dd").alias("SRC_SYS_LAST_UPDT_DT"),
    regexp_replace(col("PCED_LAST_USUS_ID"), "[\r\n\t]", "").alias("SRC_SYS_LAST_UPDT_USER")
)

# --------------------------------------------------------------------------------
# Collector (CCollector) - Round Robin => union
# --------------------------------------------------------------------------------
df_Collector = df_StripPced.union(df_StripPbed)

# --------------------------------------------------------------------------------
# BusinessRules (Transformer)
# --------------------------------------------------------------------------------
df_BusinessRules = (
    df_Collector
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", lit(RunDate))
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit("FACETS"))
    .withColumn("PRI_KEY_STRING", concat(lit("FACETS"), lit(";"), col("PRVCY_DSCLSUR_ENTY_UNIQ_KEY")))
    .withColumn("PRVCY_DSCLSUR_ENTY_SK", lit(0))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("PRVCY_DSCLSUR_ENTY_ID", trim("PRVCY_DSCLSUR_ENTY_ID"))
)

# --------------------------------------------------------------------------------
# PrimaryKey (Transformer) with lookup (left join) from df_hf_prvcy_dsclsur_enty
# --------------------------------------------------------------------------------
df_PrimaryKeyJoin = df_BusinessRules.alias("Transform").join(
    df_hf_prvcy_dsclsur_enty.alias("lkup"),
    [
        df_BusinessRules.SRC_SYS_CD == col("lkup.SRC_SYS_CD"),
        df_BusinessRules.PRVCY_DSCLSUR_ENTY_UNIQ_KEY == col("lkup.PRVCY_DSCLSUR_ENTY_UNIQ_KEY")
    ],
    "left"
)

# Prepare columns that need conditional logic, plus placeholders for SurrogateKeyGen
df_enriched = (
    df_PrimaryKeyJoin
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        when(isnull(col("lkup.PRVCY_DSCLSUR_ENTY_SK")), col("CurrRunCycle")).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn(
        "PRVCY_DSCLSUR_ENTY_SK",
        when(isnull(col("lkup.PRVCY_DSCLSUR_ENTY_SK")), lit(None)).otherwise(col("lkup.PRVCY_DSCLSUR_ENTY_SK"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", col("CurrRunCycle"))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", col("Transform.JOB_EXCTN_RCRD_ERR_SK"))
    .withColumn("INSRT_UPDT_CD", col("Transform.INSRT_UPDT_CD"))
    .withColumn("DISCARD_IN", col("Transform.DISCARD_IN"))
    .withColumn("PASS_THRU_IN", col("Transform.PASS_THRU_IN"))
    .withColumn("FIRST_RECYC_DT", col("Transform.FIRST_RECYC_DT"))
    .withColumn("ERR_CT", col("Transform.ERR_CT"))
    .withColumn("RECYCLE_CT", col("Transform.RECYCLE_CT"))
    .withColumn("SRC_SYS_CD", col("Transform.SRC_SYS_CD"))
    .withColumn("PRI_KEY_STRING", col("Transform.PRI_KEY_STRING"))
    .withColumn("PRVCY_DSCLSUR_ENTY_UNIQ_KEY", col("Transform.PRVCY_DSCLSUR_ENTY_UNIQ_KEY"))
    .withColumn("PRVCY_DSCLSUR_ENTY_CAT_CD", col("Transform.PRVCY_DSCLSUR_ENTY_CAT_CD"))
    .withColumn("PRVCY_DSCLSUR_ENTY_TYP_CD", col("Transform.PRVCY_DSCLSUR_ENTY_TYP_CD"))
    .withColumn("PRVCY_DSCLSUR_ENTY_ID", col("Transform.PRVCY_DSCLSUR_ENTY_ID"))
    .withColumn("SRC_SYS_LAST_UPDT_DT", col("Transform.SRC_SYS_LAST_UPDT_DT"))
    .withColumn("SRC_SYS_LAST_UPDT_USER", col("Transform.SRC_SYS_LAST_UPDT_USER"))
    .withColumn("CurrRunCycle", lit(CurrRunCycle))  # carry the parameter along
)

# Apply SurrogateKeyGen for PRVCY_DSCLSUR_ENTY_SK (KeyMgtGetNextValueConcurrent)
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PRVCY_DSCLSUR_ENTY_SK",<schema>,<secret_name>)

# --------------------------------------------------------------------------------
# Split into two outputs: "Key" (all rows) and "updt" (where lkup.PRVCY_DSCLSUR_ENTY_SK is null)
# --------------------------------------------------------------------------------
df_Key = df_enriched
df_updt = df_enriched.filter(isnull(col("lkup.PRVCY_DSCLSUR_ENTY_SK"))).select(
    col("SRC_SYS_CD"),
    col("PRVCY_DSCLSUR_ENTY_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("PRVCY_DSCLSUR_ENTY_SK")
)

# --------------------------------------------------------------------------------
# IdsPrvcyDsclsurEntyExtr (CSeqFileStage) => write delimited .dat file
# --------------------------------------------------------------------------------
# Must preserve column order from the PrimaryKey -> Key link
df_Key_final = df_Key.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("PRVCY_DSCLSUR_ENTY_SK"),
    col("PRVCY_DSCLSUR_ENTY_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("PRVCY_DSCLSUR_ENTY_CAT_CD"), 10, " ").alias("PRVCY_DSCLSUR_ENTY_CAT_CD"),
    rpad(col("PRVCY_DSCLSUR_ENTY_TYP_CD"), 10, " ").alias("PRVCY_DSCLSUR_ENTY_TYP_CD"),
    col("PRVCY_DSCLSUR_ENTY_ID"),
    rpad(col("SRC_SYS_LAST_UPDT_DT"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT"),
    col("SRC_SYS_LAST_UPDT_USER")
)

write_files(
    df_Key_final,
    f"{adls_path}/key/FctsPrvcyDsclsurEntyExtr.PrvcyDsclsurEnty.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# hf_prvcy_dsclsur_enty_updt (CHashedFileStage) => write back to dummy table (Scenario B)
# Insert only new records (those missing in lookup)
# --------------------------------------------------------------------------------
# Write (append) to the same dummy table in place of hashed file
df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "IDS.dummy_hf_prvcy_dsclsur_enty") \
    .mode("append") \
    .save()