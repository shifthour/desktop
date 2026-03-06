# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBhiAlphaPfxExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Extract Alpha Pfx Ref to be sent to BCBSA for BHI (Blue Health Intelligence)
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  None
# MAGIC 
# MAGIC HASH FILES:  None
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                                    Date                    Project/Ticket #\(9)           Change Description\(9)\(9)\(9)              Development Project\(9)      Code Reviewer\(9)        Date Reviewed       
# MAGIC ------------------                                                 -------------------         ---------------------------\(9)           -----------------------------------------------------------------------            --------------------------------\(9)      -------------------------         -------------------------    
# MAGIC Praveen Annam                                          2013-08-22           5115 BHI                               Original programming                                                  EnterpriseNewDevl        Kalyan Neelam          2013-10-31
# MAGIC Praveen Annam                                          2014-07-14               5115 BHI                           FTP stage added to transfer in binary mode               EnterpriseNewDevl        Bhoomi Dasari           7/15/2014 
# MAGIC Bhoomi Dasari                                             2014-08-13          5115-BHI                           Changed file name from alpha_pfx_ref to                        EnterpriseNewDevl        Kalyan Neelam          2014-08-14
# MAGIC                                                                                                                                           alpha_prefix_ref
# MAGIC Praneeth Kakarla                                        2019-07-31           US-136670                       Coding from MA Datamart to BCA                                   EnterpriseDev2                  Kalyan Neelam          2019-08-09
# MAGIC                                                                                                                                           (Blue Cross Association) for Alpha Prefix file
# MAGIC 
# MAGIC Praneeth Kakarla\(9)                              2020-01-08              US - 187221\(9) Code change as per BCBSA Correction                          EnterpriseDev2                          Kalyan Neelam          2020-01-09

# MAGIC Extract Alpha Pfx data,
# MAGIC Create fixed length data and provide monthly file to BHI
# MAGIC Control File is created with file name and respective counts, write mode is set as append to capture information for all BHI files
# MAGIC Remove Duplicate stage used to satisfify looping condition.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, rpad, lpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------

EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
MADataMartAsstOwner = get_widget_value('MADataMartAsstOwner','')
madatamartasst_secret_name = get_widget_value('madatamartasst_secret_name','')
StartDate = get_widget_value('StartDate','')
EndDate = get_widget_value('EndDate','')
CurrDate = get_widget_value('CurrDate','')
ProdIn = get_widget_value('ProdIn','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = """SELECT DISTINCT ALPHA_PFX_CD, ALPHA_PFX_NM
FROM {}.ALPHA_PFX_D
WHERE ALPHA_PFX_PROF_CLM_PLN_CD = '240' OR ALPHA_PFX_INSTUT_CLM_PLN_CD = '240'""".format(EDWOwner)
df_Alpha_Pfx_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Trns_data = df_Alpha_Pfx_D.select(
    lit("240").alias("BHI_HOME_PLN_ID"),
    rpad(col("ALPHA_PFX_CD"), 3, " ").alias("ALPHA_PFX"),
    when(col("ALPHA_PFX_NM").isNull(), lit("                              "))
    .otherwise(rpad(col("ALPHA_PFX_NM"), 30, " "))
    .alias("ALPHA_PFX_DESC")
)

df_Trns_Default_dul = df_Alpha_Pfx_D.limit(1).select(
    lit("240").alias("BHI_HOME_PLN_ID"),
    lit("999").alias("ALPHA_PFX"),
    lit("DEFAULT                       ").alias("ALPHA_PFX_DESC")
)

df_Rm_Dup_in = df_Trns_Default_dul
df_Rm_Dup_dedup = dedup_sort(df_Rm_Dup_in, ["ALPHA_PFX"], [("ALPHA_PFX","A")])
df_Rm_Dup = df_Rm_Dup_dedup.select(
    col("BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID"),
    col("ALPHA_PFX").alias("ALPHA_PFX"),
    col("ALPHA_PFX_DESC").alias("ALPHA_PFX_DESC")
)

jdbc_url_mad, jdbc_props_mad = get_db_config(madatamartasst_secret_name)
extract_query_mad = """SELECT  ALPHA_PFX,ALPHA_PFX_DESC FROM(
SELECT DISTINCT
 CASE 
               WHEN MCARE_MBR_ENR_V.PROD_DESC='MEDICARE HMO' THEN 'RKC'         
              ELSE      
                CASE WHEN MCARE_MBR_ENR_V.PROD_DESC='MEDICARE PPO' THEN 'RKQ'                   
                END
           END  as ALPHA_PFX,
  CASE 
        WHEN MCARE_MBR_ENR_V.PROD_DESC='MEDICARE HMO' THEN 'BLUE KC MEDICARE ADVANTAGE'         
        ELSE 
                         CASE WHEN MCARE_MBR_ENR_V.PROD_DESC='MEDICARE PPO' THEN 'BLUE KC MA PPO'                   
                         END
  END  as ALPHA_PFX_DESC
  FROM   {}.[MCARE_MBR_ENR_V] MCARE_MBR_ENR_V) a
  WHERE  ALPHA_PFX_DESC IS NOT NULL""".format(MADataMartAsstOwner)
df_MBR_ENR_V = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_mad)
    .options(**jdbc_props_mad)
    .option("query", extract_query_mad)
    .load()
)

df_Alpha_Trns_data = df_MBR_ENR_V.select(
    lit("240").alias("BHI_HOME_PLN_ID"),
    col("ALPHA_PFX").alias("ALPHA_PFX"),
    col("ALPHA_PFX_DESC").alias("ALPHA_PFX_DESC")
)

df_Funnel = df_Trns_data.select("BHI_HOME_PLN_ID","ALPHA_PFX","ALPHA_PFX_DESC") \
    .union(df_Rm_Dup.select("BHI_HOME_PLN_ID","ALPHA_PFX","ALPHA_PFX_DESC")) \
    .union(df_Alpha_Trns_data.select("BHI_HOME_PLN_ID","ALPHA_PFX","ALPHA_PFX_DESC"))

df_Alpha_Rm_Dup_dedup = dedup_sort(
    df_Funnel, 
    ["ALPHA_PFX","BHI_HOME_PLN_ID","ALPHA_PFX_DESC"], 
    [("ALPHA_PFX","A"),("BHI_HOME_PLN_ID","A"),("ALPHA_PFX_DESC","A")]
)
df_Alpha_Rm_Dup = df_Alpha_Rm_Dup_dedup.select(
    col("BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID"),
    col("ALPHA_PFX").alias("ALPHA_PFX"),
    col("ALPHA_PFX_DESC").alias("ALPHA_PFX_DESC")
)

df_Copy_Extr = df_Alpha_Rm_Dup.select(
    col("BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID"),
    col("ALPHA_PFX").alias("ALPHA_PFX"),
    col("ALPHA_PFX_DESC").alias("ALPHA_PFX_DESC")
)
df_Copy_Count = df_Alpha_Rm_Dup.select(
    col("BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID")
)

df_Copy_Extr_write = df_Copy_Extr.select(
    rpad(col("BHI_HOME_PLN_ID"),3," ").alias("BHI_HOME_PLN_ID"),
    rpad(col("ALPHA_PFX"),3," ").alias("ALPHA_PFX"),
    rpad(col("ALPHA_PFX_DESC"),30," ").alias("ALPHA_PFX_DESC")
)
write_files(
    df_Copy_Extr_write,
    f"{adls_path_publish}/external/alpha_prefix_ref",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='',
    nullValue=None
)

df_Aggregator = df_Copy_Count.groupBy("BHI_HOME_PLN_ID").count()
df_Aggregator_out = df_Aggregator.select(
    col("BHI_HOME_PLN_ID"),
    col("count").alias("COUNT")
)

df_Trns_cntrl = df_Aggregator_out.select(
    col("BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID"),
    rpad(lit("STD_ALPHA_PREFIX_REF"), 30, " ").alias("EXTR_NM"),
    rpad(lit(StartDate.replace("-","")), 8, " ").alias("MIN_CLM_PROCESSED_DT"),
    rpad(lit(EndDate.replace("-","")), 8, " ").alias("MAX_CLM_PRCS_DT"),
    rpad(lit(CurrDate.replace("-","")), 8, " ").alias("SUBMSN_DT"),
    rpad(lpad(col("COUNT").cast("string"), 10, "0"), 10, " ").alias("RCRD_CT"),
    rpad(lit("+00000000000000"),15," ").alias("TOT_SUBMT_AMT"),
    rpad(lit("+00000000000000"),15," ").alias("TOT_NONCOV_AMT"),
    rpad(lit("+00000000000000"),15," ").alias("TOT_ALW_AMT"),
    rpad(lit("+00000000000000"),15," ").alias("TOT_PD_AMT"),
    rpad(lit("+00000000000000"),15," ").alias("TOT_COB_TPL_AMT"),
    rpad(lit("+00000000000000"),15," ").alias("TOT_COINS_AMT"),
    rpad(lit("+00000000000000"),15," ").alias("TOT_COPAY_AMT"),
    rpad(lit("+00000000000000"),15," ").alias("TOT_DEDCT_AMT"),
    rpad(lit("+00000000000000"),15," ").alias("TOT_FFS_EQVLNT_AMT")
)

df_Trns_cntrl_write = df_Trns_cntrl.select(
    "BHI_HOME_PLN_ID",
    "EXTR_NM",
    "MIN_CLM_PROCESSED_DT",
    "MAX_CLM_PRCS_DT",
    "SUBMSN_DT",
    "RCRD_CT",
    "TOT_SUBMT_AMT",
    "TOT_NONCOV_AMT",
    "TOT_ALW_AMT",
    "TOT_PD_AMT",
    "TOT_COB_TPL_AMT",
    "TOT_COINS_AMT",
    "TOT_COPAY_AMT",
    "TOT_DEDCT_AMT",
    "TOT_FFS_EQVLNT_AMT"
)
write_files(
    df_Trns_cntrl_write,
    f"{adls_path_publish}/external/submission_control",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='',
    nullValue=None
)