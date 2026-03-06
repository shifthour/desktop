# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   
# MAGIC 
# MAGIC PROCESSING:   .
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Naren Garapaty     09/20/2007                Initial program                                                                                        3259                    devlIDS30                Steph Goddard           09/27/2007          
# MAGIC 
# MAGIC 
# MAGIC Parikshith Chada    10/05/2007            Added Balancing extract process to the overall job                              3259                     devlIDS30  
# MAGIC Prabhu ES              2022-02-24              S2S Remediation - MSSQL connection parameters added                                               IntegrateDev5             Kalyan Neelam           2022-06-13

# MAGIC Pulling FACETS Data
# MAGIC Assign primary surrogate key
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Apply business logic
# MAGIC Writing Sequential File to ../key
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
FirstRecycleDt = get_widget_value('FirstRecycleDt','')

jdbc_url_dummy, jdbc_props_dummy = get_db_config(<...>)
df_hf_alt_fund_pln = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_dummy)
    .options(**jdbc_props_dummy)
    .option("query", "SELECT SRC_SYS_CD, ALT_FUND_PLN_ROW_ID, CRT_RUN_CYC_EXCTN_SK, ALT_FUND_PLN_SK FROM IDS.dummy_hf_alt_fund_pln")
    .load()
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", f"SELECT A.AFEP_ROW_ID, A.AFAI_CK, A.AFCP_PER_NO, A.GRGR_CK, A.SGSG_CK, A.CSPD_CAT, A.CSPI_ID, B.SGSG_ID, C.GRGR_ID FROM {FacetsOwner}.CMC_AFEP_ENTY_PLAN A, {FacetsOwner}.CMC_SGSG_SUB_GROUP B, {FacetsOwner}.CMC_GRGR_GROUP C WHERE A.SGSG_CK=B.SGSG_CK AND B.GRGR_CK=C.GRGR_CK")
    .load()
)

df_Strip = df_FACETS.select(
    F.col("AFEP_ROW_ID").alias("AFEP_ROW_ID"),
    F.col("AFAI_CK").alias("AFAI_CK"),
    F.col("AFCP_PER_NO").alias("AFCP_PER_NO"),
    F.col("GRGR_CK").alias("GRGR_CK"),
    F.col("SGSG_CK").alias("SGSG_CK"),
    F.regexp_replace(F.col("CSPD_CAT"), "[\n\r\t]", "").alias("CSPD_CAT"),
    F.regexp_replace(F.col("CSPI_ID"), "[\n\r\t]", "").alias("CSPI_ID"),
    F.regexp_replace(F.col("SGSG_ID"), "[\n\r\t]", "").alias("SGSG_ID"),
    F.regexp_replace(F.col("GRGR_ID"), "[\n\r\t]", "").alias("GRGR_ID")
)

df_BusinessRules = df_Strip.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(FirstRecycleDt).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"), F.lit(" ;"), trim(F.col("AFEP_ROW_ID"))).alias("PRI_KEY_STRING"),
    F.lit(0).alias("ALT_FUND_PLN_SK"),
    trim(F.col("AFEP_ROW_ID")).alias("ALT_FUND_PLN_ROW_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AFAI_CK").alias("ALT_FUND"),
    trim(F.col("CSPI_ID")).alias("CLS_PLN"),
    F.col("GRGR_CK").alias("GRP"),
    F.lit(0).alias("SUBGRP"),
    trim(F.col("CSPD_CAT")).alias("ALT_FUND_CLS_PROD_CAT_CD"),
    F.col("AFCP_PER_NO").alias("ALT_FUND_CNTR_PERD_NO"),
    F.col("AFAI_CK").alias("ALT_FUND_UNIQ_KEY"),
    trim(F.col("SGSG_ID")).alias("SGSG_ID"),
    trim(F.col("GRGR_ID")).alias("GRGR_ID")
)

df_enriched = df_BusinessRules.alias("Transform").join(
    df_hf_alt_fund_pln.alias("lkup"),
    (
        (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD")) &
        (F.col("Transform.ALT_FUND_PLN_ROW_ID") == F.col("lkup.ALT_FUND_PLN_ROW_ID"))
    ),
    "left"
)

df_enriched = df_enriched.withColumn(
    "SK",
    F.when(F.col("lkup.ALT_FUND_PLN_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.ALT_FUND_PLN_SK"))
).withColumn(
    "NewCrtRunCycExtcnSk",
    F.when(F.col("lkup.ALT_FUND_PLN_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'SK',<schema>,<secret_name>)

df_pkeyTrnKey = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SK").alias("ALT_FUND_PLN_SK"),
    F.col("Transform.ALT_FUND_PLN_ROW_ID").alias("ALT_FUND_PLN_ROW_ID"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.ALT_FUND").alias("ALT_FUND"),
    F.col("Transform.CLS_PLN").alias("CLS_PLN"),
    F.col("Transform.GRP").alias("GRP"),
    F.col("Transform.SUBGRP").alias("SUBGRP"),
    F.col("Transform.ALT_FUND_CLS_PROD_CAT_CD").alias("ALT_FUND_CLS_PROD_CAT_CD"),
    F.col("Transform.ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
    F.col("Transform.ALT_FUND_UNIQ_KEY").alias("ALT_FUND_UNIQ_KEY"),
    F.col("Transform.SGSG_ID").alias("SGSG_ID"),
    F.col("Transform.GRGR_ID").alias("GRGR_ID")
)

df_pkeyTrnUpdt = df_enriched.filter(F.col("lkup.ALT_FUND_PLN_SK").isNull()).select(
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.ALT_FUND_PLN_ROW_ID").alias("ALT_FUND_PLN_ROW_ID"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("SK").alias("ALT_FUND_PLN_SK")
)

execute_dml("DROP TABLE IF EXISTS STAGING.FctsAltFundPlnExtr_hf_alt_fund_pln_updt_temp", jdbc_url_dummy, jdbc_props_dummy)
(
    df_pkeyTrnUpdt.write
    .format("jdbc")
    .option("url", jdbc_url_dummy)
    .options(**jdbc_props_dummy)
    .option("dbtable", "STAGING.FctsAltFundPlnExtr_hf_alt_fund_pln_updt_temp")
    .mode("overwrite")
    .save()
)
merge_sql = """
MERGE INTO IDS.dummy_hf_alt_fund_pln AS T
USING STAGING.FctsAltFundPlnExtr_hf_alt_fund_pln_updt_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.ALT_FUND_PLN_ROW_ID = S.ALT_FUND_PLN_ROW_ID
WHEN MATCHED THEN UPDATE SET
  T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
  T.ALT_FUND_PLN_SK = S.ALT_FUND_PLN_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, ALT_FUND_PLN_ROW_ID, CRT_RUN_CYC_EXCTN_SK, ALT_FUND_PLN_SK)
  VALUES (S.SRC_SYS_CD, S.ALT_FUND_PLN_ROW_ID, S.CRT_RUN_CYC_EXCTN_SK, S.ALT_FUND_PLN_SK)
;
"""
execute_dml(merge_sql, jdbc_url_dummy, jdbc_props_dummy)

df_final_IdsAltFundPln = df_pkeyTrnKey.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("ALT_FUND_PLN_SK"),
    F.col("ALT_FUND_PLN_ROW_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ALT_FUND"),
    F.col("CLS_PLN"),
    F.col("GRP"),
    F.col("SUBGRP"),
    F.col("ALT_FUND_CLS_PROD_CAT_CD"),
    F.col("ALT_FUND_CNTR_PERD_NO"),
    F.col("ALT_FUND_UNIQ_KEY"),
    F.rpad(F.col("SGSG_ID"), 4, " ").alias("SGSG_ID"),
    F.rpad(F.col("GRGR_ID"), 8, " ").alias("GRGR_ID")
)

write_files(
    df_final_IdsAltFundPln,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", f"SELECT A.AFEP_ROW_ID, A.AFAI_CK FROM {FacetsOwner}.CMC_AFEP_ENTY_PLAN A, {FacetsOwner}.CMC_SGSG_SUB_GROUP B, {FacetsOwner}.CMC_GRGR_GROUP C WHERE A.SGSG_CK=B.SGSG_CK AND B.GRGR_CK=C.GRGR_CK")
    .load()
)

df_Transform = df_Facets_Source.select(
    GetFkeyCodes("IDS", 1, "SOURCE SYSTEM", "FACETS", "X").alias("SRC_SYS_CD_SK"),
    trim(F.col("AFEP_ROW_ID")).alias("ALT_FUND_PLN_ROW_ID"),
    F.col("AFAI_CK").alias("ALT_FUND_UNIQ_KEY")
)

write_files(
    df_Transform,
    f"{adls_path}/load/B_ALT_FUND_PLN.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)