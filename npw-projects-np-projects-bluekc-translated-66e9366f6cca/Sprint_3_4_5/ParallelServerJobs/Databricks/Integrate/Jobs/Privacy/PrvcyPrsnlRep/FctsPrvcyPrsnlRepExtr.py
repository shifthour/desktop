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
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsPrvcyPrsnlRepExtr
# MAGIC PROCESSING: Pulling data from FHP_PMPR_REP_D
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari               3/10/2007            CDS Sunset/3279          Originally Programmed                              devlIDS30                   Steph Goddard          3/29/2007        
# MAGIC 
# MAGIC Steph Goddard             7/14/10              TTR-689                       changed primary key counter from            RebuildIntNewDevl          SANdrew                 2010-09-30
# MAGIC                                                                                                       IDS_SK to PRVCY_PRSNL_REP_SK; updated to current standards
# MAGIC 
# MAGIC Anoop Nair                2022-03-14         S2S Remediation      Added FACETS DSN Connection parameters      IntegrateDev5		Ken Bradmon	2022-06-03

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
from pyspark.sql.functions import col, lit, regexp_replace, date_format, when, concat_ws, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# ----------------------------------------------------------------------------
# Replace hashed file "hf_prvcy_prsnl_rep" with dummy table in IDS:
# Read from dummy table "IDS.dummy_hf_prvcy_prsnl_rep"
# ----------------------------------------------------------------------------
df_hf_prvcy_prsnl_rep = (
  spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD_SK, PRVCY_PRSNL_REP_UNIQ_KEY, CRT_RUN_CYC_EXCTN_SK, PRVCY_PRSNL_REP_SK FROM IDS.dummy_hf_prvcy_prsnl_rep")
    .load()
)

# ----------------------------------------------------------------------------
# FacetsPrvcyPrsnlRep (ODBCConnector)
# ----------------------------------------------------------------------------
extract_query = f"""
SELECT
  PMPR_CKE,
  PMPR_ID,
  PMPR_PZCD_TYPE,
  PMPR_LAST_USUS_ID,
  PMPR_LAST_UPD_DTM
FROM {FacetsOwner}.FHP_PMPR_REP_D
"""
df_FacetsPrvcyPrsnlRep = (
  spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query)
    .load()
)

# ----------------------------------------------------------------------------
# StripFields (CTransformerStage)
# ----------------------------------------------------------------------------
df_Strip = (
  df_FacetsPrvcyPrsnlRep
    .withColumn("PMPR_ID", regexp_replace(col("PMPR_ID"), "[\n\r\t]", ""))
    .withColumn("PMPR_PZCD_TYPE", regexp_replace(col("PMPR_PZCD_TYPE"), "[\n\r\t]", ""))
    .withColumn("PMPR_LAST_USUS_ID", regexp_replace(col("PMPR_LAST_USUS_ID"), "[\n\r\t]", ""))
    .withColumn("PMPR_LAST_UPD_DTM", date_format(col("PMPR_LAST_UPD_DTM"), "yyyy-MM-dd"))
    .select(
       "PMPR_CKE",
       "PMPR_ID",
       "PMPR_PZCD_TYPE",
       "PMPR_LAST_USUS_ID",
       "PMPR_LAST_UPD_DTM"
    )
)

# ----------------------------------------------------------------------------
# BusinessRules (CTransformerStage)
# ----------------------------------------------------------------------------
df_BusinessRules = (
  df_Strip
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", lit(RunDate))
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit("FACETS"))
    .withColumn("PRI_KEY_STRING", concat_ws(";", lit("FACETS"), col("PMPR_CKE")))
    .withColumn("PRVCY_PRSNL_REP_SK", lit(0))
    .withColumn("PRVCY_PRSNL_REP_UNIQ_KEY", col("PMPR_CKE"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("PRVCY_PRSNL_REP_TYP_CD", col("PMPR_PZCD_TYPE"))
    .withColumn("PRVCY_PRSNL_REP_ID", trim(col("PMPR_ID")))
    .withColumn("SRC_SYS_LAST_UPDT_DT", col("PMPR_LAST_UPD_DTM"))
    .withColumn("SRC_SYS_LAST_UPDT_USER", col("PMPR_LAST_USUS_ID"))
    .select(
       "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN",
       "FIRST_RECYC_DT","ERR_CT","RECYCLE_CT","SRC_SYS_CD","PRI_KEY_STRING",
       "PRVCY_PRSNL_REP_SK","PRVCY_PRSNL_REP_UNIQ_KEY","CRT_RUN_CYC_EXCTN_SK",
       "LAST_UPDT_RUN_CYC_EXCTN_SK","PRVCY_PRSNL_REP_TYP_CD","PRVCY_PRSNL_REP_ID",
       "SRC_SYS_LAST_UPDT_DT","SRC_SYS_LAST_UPDT_USER"
    )
)

# ----------------------------------------------------------------------------
# PrimaryKey (CTransformerStage) - left join to dummy table + SurrogateKeyGen
# ----------------------------------------------------------------------------
df_primarykey = (
  df_BusinessRules.alias("Transform")
    .join(
      df_hf_prvcy_prsnl_rep.alias("lkup"),
      (
        (col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD_SK")) &
        (col("Transform.PRVCY_PRSNL_REP_UNIQ_KEY") == col("lkup.PRVCY_PRSNL_REP_UNIQ_KEY"))
      ),
      "left"
    )
)

df_enriched = (
  df_primarykey
    .withColumn(
      "PRVCY_PRSNL_REP_SK",
      when(col("lkup.PRVCY_PRSNL_REP_SK").isNull(), lit(None)).otherwise(col("lkup.PRVCY_PRSNL_REP_SK"))
    )
    .withColumn(
      "NewCurrRunCycExtcnSk",
      when(col("lkup.PRVCY_PRSNL_REP_SK").isNull(), lit(CurrRunCycle)).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PRVCY_PRSNL_REP_SK",<schema>,<secret_name>)

df_final = df_enriched.select(
  col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
  col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
  col("Transform.DISCARD_IN").alias("DISCARD_IN"),
  col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
  col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
  col("Transform.ERR_CT").alias("ERR_CT"),
  col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
  col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
  col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
  col("PRVCY_PRSNL_REP_SK").alias("PRVCY_PRSNL_REP_SK"),
  col("Transform.PRVCY_PRSNL_REP_UNIQ_KEY").alias("PRVCY_PRSNL_REP_UNIQ_KEY"),
  col("NewCurrRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
  lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
  col("Transform.PRVCY_PRSNL_REP_TYP_CD").alias("PRVCY_PRSNL_REP_TYP_CD"),
  col("Transform.PRVCY_PRSNL_REP_ID").alias("PRVCY_PRSNL_REP_ID"),
  col("Transform.SRC_SYS_LAST_UPDT_DT").alias("SRC_SYS_LAST_UPDT_DT"),
  col("Transform.SRC_SYS_LAST_UPDT_USER").alias("SRC_SYS_LAST_UPDT_USER"),
  col("lkup.PRVCY_PRSNL_REP_SK").alias("lkup_PRVCY_PRSNL_REP_SK")  # needed temporarily for filter
)

# Apply rpad for any char/varchar columns in final output:
df_final_padded = (
  df_final
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("PRVCY_PRSNL_REP_TYP_CD", rpad(col("PRVCY_PRSNL_REP_TYP_CD"), 4, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT", rpad(col("SRC_SYS_LAST_UPDT_DT"), 10, " "))
    .withColumn("SRC_SYS_LAST_UPDT_USER", rpad(col("SRC_SYS_LAST_UPDT_USER"), 10, " "))
    .select(
       "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN",
       "FIRST_RECYC_DT","ERR_CT","RECYCLE_CT","SRC_SYS_CD","PRI_KEY_STRING",
       "PRVCY_PRSNL_REP_SK","PRVCY_PRSNL_REP_UNIQ_KEY","CRT_RUN_CYC_EXCTN_SK",
       "LAST_UPDT_RUN_CYC_EXCTN_SK","PRVCY_PRSNL_REP_TYP_CD","PRVCY_PRSNL_REP_ID",
       "SRC_SYS_LAST_UPDT_DT","SRC_SYS_LAST_UPDT_USER","lkup_PRVCY_PRSNL_REP_SK"
    )
)

# ----------------------------------------------------------------------------
# IdsPrvcyPrsnlRepExtr (CSeqFileStage)
# Write final link "Key" to .dat file
# ----------------------------------------------------------------------------
df_key = df_final_padded.drop("lkup_PRVCY_PRSNL_REP_SK")  # remove temporary column

write_files(
  df_key,
  f"{adls_path}/key/FctsPrvcyPrsnlRepExtr.PrvcyPrsnlRep.dat.{RunID}",
  delimiter=",",
  mode="overwrite",
  is_pqruet=False,
  header=False,
  quote="\"",
  nullValue=None
)

# ----------------------------------------------------------------------------
# hf_prvcy_prsnl_rep_updt (CHashedFileStage) - scenario B insert into dummy table
# Constraint: IsNull(lkup.PRVCY_PRSNL_REP_SK) = @TRUE
# ----------------------------------------------------------------------------
df_updt = (
  df_final_padded
    .filter(col("lkup_PRVCY_PRSNL_REP_SK").isNull())
    .select(
       col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
       col("PRVCY_PRSNL_REP_UNIQ_KEY"),
       lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
       col("PRVCY_PRSNL_REP_SK")
    )
)

# Write df_updt to a staging table and merge into IDS.dummy_hf_prvcy_prsnl_rep
execute_dml(
  "DROP TABLE IF EXISTS STAGING.FctsPrvcyPrsnlRepExtr_hf_prvcy_prsnl_rep_updt_temp",
  jdbc_url_ids,
  jdbc_props_ids
)

(
  df_updt
    .write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", "STAGING.FctsPrvcyPrsnlRepExtr_hf_prvcy_prsnl_rep_updt_temp")
    .mode("overwrite")
    .save()
)

merge_sql = """
MERGE IDS.dummy_hf_prvcy_prsnl_rep AS T
USING STAGING.FctsPrvcyPrsnlRepExtr_hf_prvcy_prsnl_rep_updt_temp AS S
ON T.SRC_SYS_CD_SK=S.SRC_SYS_CD_SK
   AND T.PRVCY_PRSNL_REP_UNIQ_KEY=S.PRVCY_PRSNL_REP_UNIQ_KEY
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD_SK, PRVCY_PRSNL_REP_UNIQ_KEY, CRT_RUN_CYC_EXCTN_SK, PRVCY_PRSNL_REP_SK)
  VALUES (S.SRC_SYS_CD_SK, S.PRVCY_PRSNL_REP_UNIQ_KEY, S.CRT_RUN_CYC_EXCTN_SK, S.PRVCY_PRSNL_REP_SK);
"""

execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)