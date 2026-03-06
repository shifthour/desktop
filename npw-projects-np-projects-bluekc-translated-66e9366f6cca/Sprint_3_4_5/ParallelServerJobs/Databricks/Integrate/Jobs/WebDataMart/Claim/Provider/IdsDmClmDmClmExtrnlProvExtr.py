# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                    IDS Product Data Mart Deduct Component extract from IDS to Data Mart.  
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                     Extracts from IDS and pulls to the Data Marts.  Perform no changes with the data.
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project               Code Reviewer                      Date Reviewed
# MAGIC ---------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------              ---------------------------------               -------------------------
# MAGIC Ralph Tucker                  2008-10-27       Labor Accts - 3648                New Etl                                                                              devlIDSnew                             Steph Goddard                      11/07/2008                
# MAGIC   
# MAGIC Archana Palivela           09/20/2013          5114                                 Original Programming(Server to Parallel)                             IntegrateWrhsDevl                      Jag Yelavarthi                        2013-11-30

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CLM_EXTRNL_PROV_ST_CD_SK
# MAGIC CLM_EXTRNL_PROV_CTRY_CD_SK
# MAGIC Write CLM_DM_CLM_EXTRNL_PROVData into a Sequential file for Load Job IdsDmClmDmClmExtrnlProvLoad
# MAGIC Read all the Data from IDS CLM_EXTRNL_PROV Table; .
# MAGIC Add Defaults and Null Handling......
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsDmClmDmClmExtrnlProvExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
DataMartRunCycle = get_widget_value('DataMartRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_CLM_EXTRNL_PROV_in = f"""SELECT 
COALESCE(MAP.TRGT_CD,'UNK') SRC_SYS_CD,
PROV.CLM_ID,
PROV.PROV_NM,
PROV.ADDR_LN_1,
PROV.ADDR_LN_2,
PROV.ADDR_LN_3,
PROV.CITY_NM,
PROV.CLM_EXTRNL_PROV_ST_CD_SK,
PROV.POSTAL_CD,
PROV.CNTY_NM,
PROV.CLM_EXTRNL_PROV_CTRY_CD_SK,
PROV.PHN_NO,
PROV.PROV_ID,
PROV.PROV_NPI,
PROV.SVC_PROV_ID,
PROV.SVC_PROV_NPI,
PROV.TAX_ID
FROM {IDSOwner}.CLM_EXTRNL_PROV        PROV,
     {IDSOwner}.W_WEBDM_ETL_DRVR     ex,
     {IDSOwner}.CD_MPPNG MAP
WHERE ex.SRC_SYS_CD_SK = PROV.SRC_SYS_CD_SK
  AND ex.CLM_ID = PROV.CLM_ID
  AND PROV.SRC_SYS_CD_SK = MAP.CD_MPPNG_SK
"""

df_db2_CLM_EXTRNL_PROV_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_EXTRNL_PROV_in)
    .load()
)

extract_query_db2_CD_MPPNG_in = f"""SELECT
CD_MPPNG_SK,
COALESCE(SRC_CD,'UNK') SRC_CD,
COALESCE(SRC_CD_NM,'UNK') SRC_CD_NM,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
from {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

df_cpy_cd_mppng_Ref_ProvStCdLkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_Ref_ProvCtryCdLkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Codes = (
    df_db2_CLM_EXTRNL_PROV_in.alias("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC")
    .join(
        df_cpy_cd_mppng_Ref_ProvStCdLkup.alias("Ref_ProvStCdLkup"),
        F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.CLM_EXTRNL_PROV_ST_CD_SK") == F.col("Ref_ProvStCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng_Ref_ProvCtryCdLkup.alias("Ref_ProvCtryCdLkup"),
        F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.CLM_EXTRNL_PROV_CTRY_CD_SK") == F.col("Ref_ProvCtryCdLkup.CD_MPPNG_SK"),
        "left"
    )
)

df_lkp_Codes = df_lkp_Codes.select(
    F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.CLM_ID").alias("CLM_ID"),
    F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.PROV_NM").alias("CLM_EXTRNL_PROV_NM"),
    F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.ADDR_LN_1").alias("CLM_EXTRNL_PROV_ADDR_LN_1"),
    F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.ADDR_LN_2").alias("CLM_EXTRNL_PROV_ADDR_LN_2"),
    F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.ADDR_LN_3").alias("CLM_EXTRNL_PROV_ADDR_LN_3"),
    F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.CITY_NM").alias("CLM_EXTRNL_PROV_CITY_NM"),
    F.col("Ref_ProvStCdLkup.TRGT_CD").alias("CLM_EXTRNL_PROV_ST_CD"),
    F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.POSTAL_CD").alias("CLM_EXTRNL_PROV_POSTAL_CD"),
    F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.CNTY_NM").alias("CLM_EXTRNL_PROV_CNTY_NM"),
    F.col("Ref_ProvCtryCdLkup.TRGT_CD").alias("CLM_EXTRNL_PROV_CTRY_CD"),
    F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.PHN_NO").alias("CLM_EXTRNL_PROV_PHN_NO"),
    F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.PROV_ID").alias("CLM_EXTRNL_PROV_ID"),
    F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.PROV_NPI").alias("CLM_EXTRNL_PROV_NPI"),
    F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.SVC_PROV_ID").alias("CLM_EXTRNL_PROV_SVC_PROV_ID"),
    F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.SVC_PROV_NPI").alias("CLM_EXTRNL_PROV_SVC_PROV_NPI"),
    F.col("lnk_IdsDmClmDmClmExtrnlProvExtr_InABC.TAX_ID").alias("CLM_EXTRNL_PROV_TAX_ID")
)

df_xfrm_BusinessLogic = df_lkp_Codes.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_EXTRNL_PROV_NM").alias("CLM_EXTRNL_PROV_NM"),
    F.col("CLM_EXTRNL_PROV_ADDR_LN_1").alias("CLM_EXTRNL_PROV_ADDR_LN_1"),
    F.col("CLM_EXTRNL_PROV_ADDR_LN_2").alias("CLM_EXTRNL_PROV_ADDR_LN_2"),
    F.col("CLM_EXTRNL_PROV_ADDR_LN_3").alias("CLM_EXTRNL_PROV_ADDR_LN_3"),
    F.col("CLM_EXTRNL_PROV_CITY_NM").alias("CLM_EXTRNL_PROV_CITY_NM"),
    F.when(trim(F.col("CLM_EXTRNL_PROV_ST_CD")) == "", F.lit("UNK"))
     .otherwise(F.col("CLM_EXTRNL_PROV_ST_CD")).alias("CLM_EXTRNL_PROV_ST_CD"),
    F.col("CLM_EXTRNL_PROV_POSTAL_CD").alias("CLM_EXTRNL_PROV_POSTAL_CD"),
    F.col("CLM_EXTRNL_PROV_CNTY_NM").alias("CLM_EXTRNL_PROV_CNTY_NM"),
    F.when(trim(F.col("CLM_EXTRNL_PROV_CTRY_CD")) == "", F.lit("UNK"))
     .otherwise(F.col("CLM_EXTRNL_PROV_CTRY_CD")).alias("CLM_EXTRNL_PROV_CTRY_CD"),
    F.col("CLM_EXTRNL_PROV_PHN_NO").alias("CLM_EXTRNL_PROV_PHN_NO"),
    F.col("CLM_EXTRNL_PROV_ID").alias("CLM_EXTRNL_PROV_ID"),
    F.col("CLM_EXTRNL_PROV_NPI").alias("CLM_EXTRNL_PROV_NPI"),
    F.col("CLM_EXTRNL_PROV_SVC_PROV_ID").alias("CLM_EXTRNL_PROV_SVC_PROV_ID"),
    F.col("CLM_EXTRNL_PROV_SVC_PROV_NPI").alias("CLM_EXTRNL_PROV_SVC_PROV_NPI"),
    F.col("CLM_EXTRNL_PROV_TAX_ID").alias("CLM_EXTRNL_PROV_TAX_ID"),
    F.lit(DataMartRunCycle).alias("LAST_UPDT_RUN_CYC_NO")
)

df_final = (
    df_xfrm_BusinessLogic
    .withColumn("CLM_EXTRNL_PROV_POSTAL_CD", F.rpad(F.col("CLM_EXTRNL_PROV_POSTAL_CD"), 11, " "))
    .withColumn("CLM_EXTRNL_PROV_PHN_NO", F.rpad(F.col("CLM_EXTRNL_PROV_PHN_NO"), 20, " "))
    .withColumn("CLM_EXTRNL_PROV_SVC_PROV_ID", F.rpad(F.col("CLM_EXTRNL_PROV_SVC_PROV_ID"), 13, " "))
    .withColumn("CLM_EXTRNL_PROV_TAX_ID", F.rpad(F.col("CLM_EXTRNL_PROV_TAX_ID"), 9, " "))
)

df_output = df_final.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_EXTRNL_PROV_NM",
    "CLM_EXTRNL_PROV_ADDR_LN_1",
    "CLM_EXTRNL_PROV_ADDR_LN_2",
    "CLM_EXTRNL_PROV_ADDR_LN_3",
    "CLM_EXTRNL_PROV_CITY_NM",
    "CLM_EXTRNL_PROV_ST_CD",
    "CLM_EXTRNL_PROV_POSTAL_CD",
    "CLM_EXTRNL_PROV_CNTY_NM",
    "CLM_EXTRNL_PROV_CTRY_CD",
    "CLM_EXTRNL_PROV_PHN_NO",
    "CLM_EXTRNL_PROV_ID",
    "CLM_EXTRNL_PROV_NPI",
    "CLM_EXTRNL_PROV_SVC_PROV_ID",
    "CLM_EXTRNL_PROV_SVC_PROV_NPI",
    "CLM_EXTRNL_PROV_TAX_ID",
    "LAST_UPDT_RUN_CYC_NO"
)

write_files(
    df_output,
    f"{adls_path}/load/CLM_DM_CLM_EXTRNL_PROV.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)