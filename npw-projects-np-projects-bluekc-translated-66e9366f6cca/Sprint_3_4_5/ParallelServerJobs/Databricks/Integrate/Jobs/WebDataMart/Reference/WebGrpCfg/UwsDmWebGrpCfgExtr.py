# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    UwsWebGrpCfgExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                    Web Group Cfg extract from UWS to Data Mart.   
# MAGIC 
# MAGIC              
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                     Extracts from UWS and pulls to the Data Marts.  Perform no changes with the data.
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Data Mart:  WEB_GRP_CFG
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                    
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                   Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                            ----------------------------------              ---------------------------------               -------------------------
# MAGIC Bhoomi Dasari                 2009-09-23             JAA - 3500                            Original Programming                                                  devlIDSnew                          Steph Goddard                        09/25/2009
# MAGIC   
# MAGIC Archana Palivela             08/08/2013          5114                                    Original Programming(Server to Parallel)                         IntegrateWrhsDevl               Bhoomi Dasari                         4/8/2014

# MAGIC UWS Web Group CFG extract from UWS to Data Mart.
# MAGIC Write WEB_GRP_CFG Data into a Sequential file for Load Job IdsDmProdDmDedctCmpntLoad.
# MAGIC Read all the Data from IDS WEB_GRP_CFGTable; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: UwsDmWebGrpCfgExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, coalesce, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
DataMartRunCycle = get_widget_value('DataMartRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_DB2_GRP_Uniq = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT GRP_ID, GRP_UNIQ_KEY FROM " + IDSOwner + ".GRP")
    .load()
)

jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
df_UWS_WEB_GRP_CFG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", "SELECT GRP_ID, LOGO_URL_ADDR, USER_ID, LAST_UPDT_DT_SK FROM " + UWSOwner + ".WEB_GRP_CFG")
    .load()
)

df_lkp_Codes = (
    df_UWS_WEB_GRP_CFG.alias("lnk_UwsDmWebGrpCfgExtr_InABC")
    .join(
        df_DB2_GRP_Uniq.alias("Lnk_GrpId"),
        col("lnk_UwsDmWebGrpCfgExtr_InABC.GRP_ID") == col("Lnk_GrpId.GRP_ID"),
        "left"
    )
    .select(
        col("lnk_UwsDmWebGrpCfgExtr_InABC.GRP_ID").alias("GRP_ID"),
        col("lnk_UwsDmWebGrpCfgExtr_InABC.LOGO_URL_ADDR").alias("LOGO_URL_ADDR"),
        col("lnk_UwsDmWebGrpCfgExtr_InABC.USER_ID").alias("USER_ID"),
        col("lnk_UwsDmWebGrpCfgExtr_InABC.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
        col("Lnk_GrpId.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY")
    )
)

df_xfrm_BusinessLogic = df_lkp_Codes.select(
    col("GRP_ID"),
    coalesce(col("GRP_UNIQ_KEY"), lit(" ")).alias("GRP_UNIQ_KEY"),
    col("LOGO_URL_ADDR"),
    col("USER_ID"),
    col("LAST_UPDT_DT_SK")
)

df_final = df_xfrm_BusinessLogic.select(
    col("GRP_ID"),
    col("GRP_UNIQ_KEY"),
    col("LOGO_URL_ADDR"),
    col("USER_ID"),
    rpad(col("LAST_UPDT_DT_SK"), 10, " ").alias("LAST_UPDT_DT_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/WEB_GRP_CFG.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)