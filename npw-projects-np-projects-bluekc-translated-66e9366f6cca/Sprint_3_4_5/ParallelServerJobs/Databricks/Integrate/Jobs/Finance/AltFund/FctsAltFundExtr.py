# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   .
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                             Date                           Change Description                                                                 Project #               Development Project           Code Reviewer            Date Reviewed  
# MAGIC ------------------                         ----------------------------        -----------------------------------------------------------------------------                   ----------------             ------------------------------------         ----------------------------         -----------------------
# MAGIC Naren Garapaty                     09/20/2007                Initial program                                                                          3259                      devlIDS30                          Steph Goddard             09/27/2007     
# MAGIC 
# MAGIC 
# MAGIC Parikshith Chada                   10/04/2007               Added Balancing extract process to the overall job                  3259                     devlIDS30                    
# MAGIC 
# MAGIC Prabhu ES                       2022-04-11                S2S MSSQL ODBC conn params added                                                                    IntegrateDev5                      Kalyan Neelam              2022-06-13

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import col, lit, when, regexp_replace, upper, length, rpad, date_format, expr
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
FirstRecycleDt = get_widget_value('FirstRecycleDt','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_hf_alt_fund, jdbc_props_hf_alt_fund = get_db_config(ids_secret_name)
df_hf_alt_fund = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_hf_alt_fund)
    .options(**jdbc_props_hf_alt_fund)
    .option("query", "SELECT SRC_SYS_CD, ALT_FUND_UNIQ_KEY, CRT_RUN_CYC_EXCTN_SK, ALT_FUND_SK FROM dummy_hf_alt_fund")
    .load()
)

jdbc_url_FACETS, jdbc_props_FACETS = get_db_config(facets_secret_name)
extract_query_FACETS = f"SELECT A.AFAI_CK, A.AFAI_ID, A.AFAI_NAME, A.AFAI_EFF_DT, A.AFAI_TERM_DT, B.AFAD_ADDR1, B.AFAD_ADDR2, B.AFAD_ADDR3, B.AFAD_CITY, B.AFAD_STATE, B.AFAD_ZIP, B.AFAD_EMAIL FROM {FacetsOwner}.CMC_AFAI_INDIC A, {FacetsOwner}.CMC_AFAD_ADDR B WHERE A.AFAI_CK=B.AFAI_CK"
df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_FACETS)
    .options(**jdbc_props_FACETS)
    .option("query", extract_query_FACETS)
    .load()
)

df_StripField = (
    df_FACETS
    .withColumn("AFAI_CK", col("AFAI_CK"))
    .withColumn("AFAI_ID", regexp_replace(col("AFAI_ID"), "[\r\n\t]", ""))
    .withColumn("AFAI_NAME", upper(regexp_replace(col("AFAI_NAME"), "[\r\n\t]", "")))
    .withColumn("AFAI_EFF_DT", date_format(col("AFAI_EFF_DT"), "yyyy-MM-dd"))
    .withColumn("AFAI_TERM_DT", date_format(col("AFAI_TERM_DT"), "yyyy-MM-dd"))
    .withColumn("AFAD_ADDR1", upper(regexp_replace(col("AFAD_ADDR1"), "[\r\n\t]", "")))
    .withColumn("AFAD_ADDR2", upper(regexp_replace(col("AFAD_ADDR2"), "[\r\n\t]", "")))
    .withColumn("AFAD_ADDR3", upper(regexp_replace(col("AFAD_ADDR3"), "[\r\n\t]", "")))
    .withColumn("AFAD_CITY", upper(regexp_replace(col("AFAD_CITY"), "[\r\n\t]", "")))
    .withColumn("AFAD_STATE", regexp_replace(col("AFAD_STATE"), "[\r\n\t]", ""))
    .withColumn("AFAD_ZIP", regexp_replace(col("AFAD_ZIP"), "[\r\n\t]", ""))
    .withColumn("AFAD_EMAIL", regexp_replace(col("AFAD_EMAIL"), "[\r\n\t]", ""))
    .select(
        "AFAI_CK",
        "AFAI_ID",
        "AFAI_NAME",
        "AFAI_EFF_DT",
        "AFAI_TERM_DT",
        "AFAD_ADDR1",
        "AFAD_ADDR2",
        "AFAD_ADDR3",
        "AFAD_CITY",
        "AFAD_STATE",
        "AFAD_ZIP",
        "AFAD_EMAIL"
    )
)

df_BusinessRules = (
    df_StripField
    .withColumn("RowPassThru", lit("Y"))
    .withColumn("svAfaiCk", trim(col("AFAI_CK")))
    .withColumn("svSrcSysCd", lit("FACETS"))
    .withColumn("svAddrLn1", trim(col("AFAD_ADDR1")))
    .withColumn("svAddrLn2", trim(col("AFAD_ADDR2")))
    .withColumn("svAddrLn3", trim(col("AFAD_ADDR3")))
    .withColumn("svCityNm", trim(col("AFAD_CITY")))
)

df_BusinessRules = df_BusinessRules.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
    rpad(col("RowPassThru"), 1, " ").alias("PASS_THRU_IN"),
    lit(FirstRecycleDt).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("svSrcSysCd").alias("SRC_SYS_CD"),
    expr("svSrcSysCd || ' ;' || svAfaiCk").alias("PRI_KEY_STRING"),
    lit(0).alias("ALT_FUND_SK"),
    col("AFAI_CK").alias("ALT_FUND_UNIQ_KEY"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    trim(col("AFAI_ID")).alias("ALT_FUND_ID"),
    trim(col("AFAI_NAME")).alias("ALT_FUND_NM"),
    rpad(col("AFAI_EFF_DT"), 10, " ").alias("EFF_DT"),
    rpad(col("AFAI_TERM_DT"), 10, " ").alias("TERM_DT"),
    when(col("svAddrLn1").isNull() | (length(col("svAddrLn1")) == 0), lit(None)).otherwise(col("svAddrLn1")).alias("ADDR_LN_1"),
    when(col("svAddrLn2").isNull() | (length(col("svAddrLn2")) == 0), lit(None)).otherwise(col("svAddrLn2")).alias("ADDR_LN_2"),
    when(col("svAddrLn3").isNull() | (length(col("svAddrLn3")) == 0), lit(None)).otherwise(col("svAddrLn3")).alias("ADDR_LN_3"),
    when(col("svCityNm").isNull() | (length(col("svCityNm")) == 0), lit(None)).otherwise(col("svCityNm")).alias("CITY_NM"),
    trim(col("AFAD_STATE")).alias("ALT_FUND_ST_CD"),
    when(length(trim(col("AFAD_ZIP"))) == 0, lit(None)).otherwise(trim(col("AFAD_ZIP"))).alias("POSTAL_CD"),
    when(length(trim(col("AFAD_EMAIL"))) == 0, lit(None)).otherwise(trim(col("AFAD_EMAIL"))).alias("EMAIL_ADDR_TX")
)

df_joined = df_BusinessRules.alias("Transform").join(
    df_hf_alt_fund.alias("lkup"),
    [
        col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD"),
        col("Transform.ALT_FUND_UNIQ_KEY") == col("lkup.ALT_FUND_UNIQ_KEY")
    ],
    "left"
)

df_enriched = df_joined.withColumn(
    "SK",
    when(col("lkup.ALT_FUND_SK").isNull(), lit(None)).otherwise(col("lkup.ALT_FUND_SK"))
).withColumn(
    "NewCrtRunCycExtcnSk",
    when(col("lkup.ALT_FUND_SK").isNull(), lit(CurrRunCycle).cast("int")).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'SK',<schema>,<secret_name>)

df_key = df_enriched.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("SK").alias("ALT_FUND_SK"),
    col("ALT_FUND_UNIQ_KEY"),
    col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).cast("int").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ALT_FUND_ID"),
    col("ALT_FUND_NM"),
    col("EFF_DT"),
    col("TERM_DT"),
    col("ADDR_LN_1"),
    col("ADDR_LN_2"),
    col("ADDR_LN_3"),
    col("CITY_NM"),
    col("ALT_FUND_ST_CD"),
    col("POSTAL_CD"),
    col("EMAIL_ADDR_TX")
)

df_updt = df_enriched.filter(col("lkup.ALT_FUND_SK").isNull()).select(
    col("SRC_SYS_CD"),
    col("ALT_FUND_UNIQ_KEY"),
    lit(CurrRunCycle).cast("int").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("SK").alias("ALT_FUND_SK")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
execute_dml("DROP TABLE IF EXISTS STAGING.FctsAltFundExtr_hf_alt_fund_updt_temp", jdbc_url_ids, jdbc_props_ids)
df_updt.write.format("jdbc").option("url", jdbc_url_ids).options(**jdbc_props_ids).option("dbtable", "STAGING.FctsAltFundExtr_hf_alt_fund_updt_temp").mode("overwrite").save()

merge_sql = """
MERGE dummy_hf_alt_fund AS T
USING STAGING.FctsAltFundExtr_hf_alt_fund_updt_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.ALT_FUND_UNIQ_KEY = S.ALT_FUND_UNIQ_KEY
WHEN MATCHED THEN
  UPDATE SET 
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.ALT_FUND_SK = S.ALT_FUND_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, ALT_FUND_UNIQ_KEY, CRT_RUN_CYC_EXCTN_SK, ALT_FUND_SK)
  VALUES (S.SRC_SYS_CD, S.ALT_FUND_UNIQ_KEY, S.CRT_RUN_CYC_EXCTN_SK, S.ALT_FUND_SK);
"""
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)

output_path_IdsAltFund = f"{adls_path}/key/{TmpOutFile}"
write_files(df_key, output_path_IdsAltFund, ',', 'overwrite', False, False, '"', None)

extract_query_Facets_Source = f"SELECT CMC_AFAI_INDIC.AFAI_CK,CMC_AFAI_INDIC.AFAI_ID FROM {FacetsOwner}.CMC_AFAI_INDIC CMC_AFAI_INDIC,{FacetsOwner}.CMC_AFAD_ADDR CMC_AFAD_ADDR WHERE CMC_AFAI_INDIC.AFAI_CK=CMC_AFAD_ADDR.AFAI_CK"
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_FACETS)
    .options(**jdbc_props_FACETS)
    .option("query", extract_query_Facets_Source)
    .load()
)

df_Transform = (
    df_Facets_Source
    .withColumn("svSrcSysCdSk", GetFkeyCodes("IDS", 1, "SOURCE SYSTEM", "FACETS", "X"))
    .withColumn("AFAI_ID_CLEAN", trim(regexp_replace(col("AFAI_ID"), "[\r\n\t]", "")))
)

df_Transform = df_Transform.select(
    col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
    col("AFAI_CK").alias("ALT_FUND_UNIQ_KEY"),
    col("AFAI_ID_CLEAN").alias("ALT_FUND_ID")
)

df_Snapshot_File = df_Transform.select(
    col("SRC_SYS_CD_SK"),
    col("ALT_FUND_UNIQ_KEY"),
    rpad(col("ALT_FUND_ID"), 12, " ").alias("ALT_FUND_ID")
)

output_path_Snapshot_File = f"{adls_path}/load/B_ALT_FUND.dat"
write_files(df_Snapshot_File, output_path_Snapshot_File, ',', 'overwrite', False, False, '"', None)